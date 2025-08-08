// Copyright 2025 RISC Zero, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::collections::HashSet;

use alloy::{
    consensus::Transaction as TxTrait,
    network::Ethereum,
    primitives::Address,
    providers::Provider,
    rpc::types::Transaction,
    sol_types::SolCall,
};
use anyhow::Result;
use boundless_market::{
    contracts::{boundless_market::BoundlessMarketService, RequestId, IBoundlessMarket},
    order_stream_client::OrderStreamClient,
};
use serde_json;
use hex;
use tokio::{
    sync::{mpsc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use thiserror::Error;

use crate::{
    errors::{impl_coded_debug, CodedError},
    OrderRequest,
};

#[derive(Error)]
pub enum MempoolMonitorErr {
    #[error("{code} Mempool polling failed: {0:?}", code = self.code())]
    MempoolPollingErr(anyhow::Error),

    #[error("{code} Transaction processing failed: {0:?}", code = self.code())]
    TransactionProcessingErr(anyhow::Error),

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedErr(#[from] anyhow::Error),
}

impl CodedError for MempoolMonitorErr {
    fn code(&self) -> &str {
        match self {
            MempoolMonitorErr::MempoolPollingErr(_) => "[B-MP-001]",
            MempoolMonitorErr::TransactionProcessingErr(_) => "[B-MP-002]",
            MempoolMonitorErr::UnexpectedErr(_) => "[B-MP-000]",
        }
    }
}

impl_coded_debug!(MempoolMonitorErr);

#[derive(Clone)]
pub struct MempoolMonitor<P> {
    provider: Arc<P>,
    market_addr: Address,
    new_order_tx: mpsc::Sender<Box<OrderRequest>>,
    market_service: BoundlessMarketService<Arc<P>>,
    order_stream: Option<OrderStreamClient>,
    // 📝 트랜잭션 해시 추적으로 중복 처리 방지
    processed_tx_hashes: Arc<Mutex<HashSet<String>>>,
}

impl<P> MempoolMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    pub fn new(
        provider: Arc<P>,
        market_addr: Address,
        new_order_tx: mpsc::Sender<Box<OrderRequest>>,
        order_stream: Option<OrderStreamClient>,
    ) -> Self {
        let market_service = BoundlessMarketService::new(
            market_addr, 
            provider.clone(), 
            market_addr  // Use market address as caller for now
        );

        Self {
            provider,
            market_addr,
            new_order_tx,
            market_service,
            order_stream,
            processed_tx_hashes: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn start_monitor(
        &self,
        cancel_token: CancellationToken,
    ) -> Result<(), MempoolMonitorErr> {
        tracing::info!("⚡ MEMPOOL MONITOR: Started (5ms ULTRA polling) - MULTI-PROCESSING MODE ACTIVATED");
        tracing::info!("🎯 WATCHING CONTRACT: {:?}", self.market_addr);
        
        // Test first scan immediately
        tracing::info!("🔍 Testing first mempool scan...");
        if let Err(e) = self.scan_mempool_for_orders().await {
            tracing::error!("❌ FIRST SCAN FAILED: {:?}", e);
        } else {
            tracing::info!("✅ First scan completed successfully");
        }
        
        // ⚡ 극한 속도: 5ms 폴링으로 단축 (더 빠른 감지)
        let mut interval = tokio::time::interval(Duration::from_millis(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("Mempool monitor received cancellation signal");
                    break;
                }
                _ = interval.tick() => {
                    if let Err(err) = self.scan_mempool_for_orders().await {
                        tracing::debug!("Mempool scan failed (this is normal): {:?}", err);
                        // Continue polling even on errors - mempool access can be unreliable
                    }
                }
            }
        }

        tracing::info!("MEMPOOL MONITOR: Stopped");
        Ok(())
    }

    async fn scan_mempool_for_orders(&self) -> Result<(), MempoolMonitorErr> {
        static mut SCAN_COUNT: u64 = 0;
        
        // Increment scan count at the beginning
        unsafe {
            SCAN_COUNT += 1;
            // Log status every 5 seconds to show it's working
            if SCAN_COUNT % 1000 == 0 {  // 1000 * 5ms = 5 seconds
                tracing::info!("⚡ MEMPOOL MONITOR: Active (Scan #{}, 5ms ULTRA mode, monitoring 0x380f9c38)", SCAN_COUNT);
            }
        }
        
        // 🔧 Use raw JSON parsing to avoid deserialization issues with Base network
        let pending_block: Option<serde_json::Value> = self
            .provider
            .client()
            .request("eth_getBlockByNumber", ("pending", true))
            .await
            .map_err(|e| {
                // Log parsing errors occasionally to diagnose issues
                static mut LAST_ERROR_LOG: u64 = 0;
                unsafe {
                    if SCAN_COUNT > LAST_ERROR_LOG + 1500 { // Log error every 30 seconds
                        tracing::error!("⚠️ RPC request error (will retry): {}", e);
                        LAST_ERROR_LOG = SCAN_COUNT;
                    }
                }
                MempoolMonitorErr::MempoolPollingErr(e.into())
            })?;

        // Log first successful block fetch
        unsafe {
            if SCAN_COUNT == 1 {
                tracing::info!("✅ MEMPOOL MONITOR: Successfully connected and fetching pending blocks");
            }
        }

        if let Some(block_json) = pending_block {
            if let Some(transactions) = block_json.get("transactions").and_then(|t| t.as_array()) {
                // Remove noisy debug log
                
                let submit_request_txs = self.filter_submit_request_transactions_json(transactions);
                
                if !submit_request_txs.is_empty() {
                    // 🔄 중복 트랜잭션 필터링
                    let mut new_transactions = Vec::new();
                    let mut processed_hashes = self.processed_tx_hashes.lock().await;
                    
                    for tx_json in submit_request_txs {
                        if let Some(tx_hash) = tx_json.get("hash").and_then(|h| h.as_str()) {
                            if !processed_hashes.contains(tx_hash) {
                                processed_hashes.insert(tx_hash.to_string());
                                new_transactions.push(tx_json);
                            } else {
                                tracing::debug!("🔄 DUPLICATE TX SKIPPED: {}", tx_hash);
                            }
                        }
                    }
                    
                    // 💾 메모리 관리: 100개 이상의 해시가 쌓이면 절반 제거
                    if processed_hashes.len() > 100 {
                        let hashes_to_remove: Vec<String> = processed_hashes.iter().take(50).cloned().collect();
                        for hash in hashes_to_remove {
                            processed_hashes.remove(&hash);
                        }
                        tracing::debug!("🧹 MEMORY CLEANUP: Removed 50 old transaction hashes");
                    }
                    drop(processed_hashes);
                    
                    if !new_transactions.is_empty() {
                        // 🚀 MULTI-PROCESSING: 감지된 모든 트랜잭션을 병렬 처리
                        tracing::info!("\\n🚀 MULTI-PROCESSING: Processing ALL {} detected transactions simultaneously", new_transactions.len());
                        
                        // 모든 트랜잭션을 병렬로 처리
                        let mut processing_tasks = Vec::new();
                        for (idx, tx_json) in new_transactions.iter().enumerate() {
                            let tx_json_clone = (*tx_json).clone();
                            let new_order_tx_clone = self.new_order_tx.clone();
                            let provider_clone = self.provider.clone();
                            let market_addr = self.market_addr;
                            
                            let task = tokio::spawn(async move {
                                // 각 트랜잭션을 독립적으로 처리
                                let result = Self::process_single_transaction(
                                    tx_json_clone,
                                    idx,
                                    new_order_tx_clone,
                                    provider_clone,
                                    market_addr
                                ).await;
                                
                                match result {
                                    Ok(order_id) => {
                                        tracing::info!("🚀 MULTI-PROCESSING SUCCESS #{}: {} - HANDOFF COMPLETED!", idx + 1, order_id);
                                        Some(order_id)
                                    }
                                    Err(e) => {
                                        tracing::warn!("⚠️ MULTI-PROCESSING FAILED #{}: {}", idx + 1, e);
                                        None
                                    }
                                }
                            });
                            
                            processing_tasks.push(task);
                        }
                        
                        // 모든 태스크의 완료를 기다림 (병렬 실행)
                        let mut successful_orders = 0;
                        for (_idx, task) in processing_tasks.into_iter().enumerate() {
                            if let Ok(Some(_order_id)) = task.await {
                                successful_orders += 1;
                            }
                        }
                        
                        tracing::info!("🏁 MULTI-PROCESSING BATCH COMPLETED: {}/{} orders successfully processed", 
                                     successful_orders, new_transactions.len());
                        tracing::info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\\n");
                    }
                } else {
                    // Log all market contract transactions for debugging
                    let _market_txs: Vec<_> = transactions
                        .iter()
                        .filter(|tx| {
                            if let (Some(to), Some(input)) = (
                                tx.get("to").and_then(|t| t.as_str()),
                                tx.get("input").and_then(|i| i.as_str())
                            ) {
                                let to_addr = to.parse::<Address>().unwrap_or(Address::ZERO);
                                to_addr == self.market_addr && input.len() >= 10
                            } else {
                                false
                            }
                        })
                        .collect();
                    
                    // Remove noisy debug logging for non-matching market transactions
                }
            } else {
                // Remove noisy debug log
            }
        } else {
            // Remove noisy debug log
        }

        Ok(())
    }

    fn filter_submit_request_transactions_json<'a>(&self, transactions: &'a [serde_json::Value]) -> Vec<&'a serde_json::Value> {
        transactions
            .iter()
            .filter(|tx| {
                // 🚀 TO 주소 제약 제거: 모든 트랜잭션에서 submitRequest 함수 탐지
                if let Some(input) = tx.get("input").and_then(|i| i.as_str()) {
                    self.is_submit_request_calldata_hex(input)
                } else {
                    false
                }
            })
            .collect()
    }

    fn filter_submit_request_transactions<'a>(&self, transactions: &'a [Transaction]) -> Vec<&'a Transaction> {
        transactions
            .iter()
            .filter(|tx| {
                // Filter for transactions to our market contract with submitRequest function
                let to_addr = tx.inner.to();
                let input_data = tx.inner.input();
                
                // 두 가지 조건만 확인:
                // 1. 마켓 컨트랙트로 보내는 트랜잭션
                // 2. submitRequest 함수 호출 (from 주소는 확인하지 않음 - 속도 우선)
                to_addr == Some(self.market_addr) &&
                self.is_submit_request_calldata(input_data)
            })
            .collect()
    }

    fn is_submit_request_calldata_hex(&self, hex_input: &str) -> bool {
        if hex_input.len() < 10 { // 0x + 8 hex chars = 10 characters minimum
            return false;
        }

        // submitRequest function selector: 0x380f9c38
        let selector = &hex_input[2..10]; // Remove 0x prefix, take selector
        
        if selector == "380f9c38" {
            tracing::info!(
                "🎯 MEMPOOL SUBMITREQUEST DETECTED! Selector: 0x{} - calldata len: {}",
                selector, hex_input.len()
            );
            true
        } else {
            // Remove noisy debug log
            false
        }
    }

    fn is_submit_request_calldata(&self, calldata: &[u8]) -> bool {
        if calldata.len() < 4 {
            return false;
        }

        // submitRequest function selector: 0x380f9c38
        // ✅ VERIFIED FROM ACTUAL BASE TRANSACTION: 
        // https://basescan.org/tx/0x444b1de85937bb2e4081f9d9c1b0cbca9e57571901f97d0a7f9f0e995790c9e1
        // Function: submitRequest(ProofRequest calldata request, bytes calldata clientSignature)
        let function_selector = &calldata[0..4];
        let submit_request_selector = [0x38, 0x0f, 0x9c, 0x38];
        
        if function_selector == submit_request_selector {
            tracing::info!(
                "🎯 MEMPOOL SUBMITREQUEST DETECTED! Selector: [{:#04x}, {:#04x}, {:#04x}, {:#04x}] - calldata len: {}",
                function_selector[0], function_selector[1], function_selector[2], function_selector[3],
                calldata.len()
            );
            true
        } else {
            // Remove noisy debug log
            false
        }
    }

    async fn extract_request_id_from_tx_json(&self, tx_json: &serde_json::Value) -> Result<(RequestId, IBoundlessMarket::submitRequestCall)> {
        // Extract and decode submitRequest transaction calldata from JSON
        let input_hex = tx_json.get("input")
            .and_then(|i| i.as_str())
            .ok_or_else(|| anyhow::anyhow!("No input field in transaction"))?;
        
        // Convert hex string to bytes
        let input = hex::decode(&input_hex[2..]) // Remove 0x prefix
            .map_err(|e| anyhow::anyhow!("Failed to decode hex input: {}", e))?;
        
        tracing::debug!(
            "🔍 Extracting request from tx with {} bytes of calldata",
            input.len()
        );

        // Decode the submitRequest call data
        let decoded_call = IBoundlessMarket::submitRequestCall::abi_decode(&input)
            .map_err(|e| anyhow::anyhow!("Failed to decode submitRequest calldata: {}", e))?;
        
        let request_id_u256 = decoded_call.request.id;
        let request_id = RequestId::from_lossy(request_id_u256);
        
        tracing::info!(
            "🚀 DECODED MEMPOOL SUBMITREQUEST: request_id={:x}, from tx hash {}",
            request_id_u256,
            tx_json.get("hash").and_then(|h| h.as_str()).unwrap_or("N/A")
        );
        
        Ok((request_id, decoded_call))
    }

    async fn extract_request_id_from_tx(&self, tx: &Transaction) -> Result<(RequestId, IBoundlessMarket::submitRequestCall)> {
        // Extract and decode submitRequest transaction calldata
        let input = tx.inner.input();
        
        tracing::debug!(
            "🔍 Extracting request from tx with {} bytes of calldata",
            input.len()
        );

        // Decode the submitRequest call data
        let decoded_call = IBoundlessMarket::submitRequestCall::abi_decode(input)
            .map_err(|e| anyhow::anyhow!("Failed to decode submitRequest calldata: {}", e))?;
        
        let request_id_u256 = decoded_call.request.id;
        let request_id = RequestId::from_lossy(request_id_u256);
        
        tracing::info!(
            "🚀 DECODED MEMPOOL SUBMITREQUEST: request_id={:x}, from block {:?}",
            request_id_u256,
            tx.block_number
        );
        
        Ok((request_id, decoded_call))
    }

    // 🚀 병렬 처리를 위한 단일 트랜잭션 처리 헬퍼
    async fn process_single_transaction(
        tx_json: serde_json::Value,
        idx: usize,
        new_order_tx: mpsc::Sender<Box<OrderRequest>>,
        provider: Arc<P>,
        market_addr: Address,
    ) -> Result<String> 
    where 
        P: Provider<Ethereum> + 'static + Clone,
    {
        // 트랜잭션 디코딩
        let (request_id, decoded_call) = Self::extract_request_id_from_tx_json_static(&tx_json, &provider).await?;
        
        let client = request_id.addr;
        let max_price = decoded_call.request.offer.maxPrice;
        let image_id = &decoded_call.request.requirements.imageId;
        let tx_hash = tx_json.get("hash").and_then(|h| h.as_str()).unwrap_or("N/A");
        
        // ⚡ 성능 우선: 상세 로깅 최소화
        if idx == 0 || tracing::enabled!(tracing::Level::DEBUG) {
            tracing::info!(
                "  📋 PARALLEL TX #{}: Request ID: {:?} | Client: {:?} | MaxPrice: {} | ImageID: 0x{}...{} | TxHash: {}",
                idx + 1,
                request_id,
                client,
                max_price,
                hex::encode(&image_id.0[..4]),
                hex::encode(&image_id.0[28..]),
                tx_hash
            );
        }
        
        // Order 생성
        let order = Self::create_mempool_order_request_static(decoded_call, provider, market_addr).await?;
        let order_id = order.id();
        
        // Order 전송
        new_order_tx.send(Box::new(order)).await
            .map_err(|e| anyhow::anyhow!("Failed to send order: {}", e))?;
        
        Ok(order_id)
    }

    // Static 버전의 헬퍼 메서드들
    async fn extract_request_id_from_tx_json_static(
        tx_json: &serde_json::Value,
        _provider: &Arc<P>
    ) -> Result<(RequestId, IBoundlessMarket::submitRequestCall)> 
    where 
        P: Provider<Ethereum> + 'static + Clone,
    {
        // Extract and decode submitRequest transaction calldata from JSON
        let input_hex = tx_json.get("input")
            .and_then(|i| i.as_str())
            .ok_or_else(|| anyhow::anyhow!("No input field in transaction"))?;
        
        // Convert hex string to bytes
        let input = hex::decode(&input_hex[2..]) // Remove 0x prefix
            .map_err(|e| anyhow::anyhow!("Failed to decode hex input: {}", e))?;
        
        tracing::debug!(
            "🔍 Extracting request from tx with {} bytes of calldata",
            input.len()
        );

        // Decode the submitRequest call data
        let decoded_call = IBoundlessMarket::submitRequestCall::abi_decode(&input)
            .map_err(|e| anyhow::anyhow!("Failed to decode submitRequest calldata: {}", e))?;
        
        let request_id_u256 = decoded_call.request.id;
        let request_id = RequestId::from_lossy(request_id_u256);
        
        tracing::info!(
            "🚀 DECODED MEMPOOL SUBMITREQUEST: request_id={:x}, from tx hash {}",
            request_id_u256,
            tx_json.get("hash").and_then(|h| h.as_str()).unwrap_or("N/A")
        );
        
        Ok((request_id, decoded_call))
    }

    async fn create_mempool_order_request_static(
        decoded_call: IBoundlessMarket::submitRequestCall,
        provider: Arc<P>,
        market_addr: Address
    ) -> Result<OrderRequest> 
    where 
        P: Provider<Ethereum> + 'static + Clone,
    {
        // Create OrderRequest directly from decoded mempool transaction
        let chain_id = provider.get_chain_id().await
            .map_err(|e| anyhow::anyhow!("Failed to get chain_id: {}", e))?;
            
        let order_request = OrderRequest::new(
            decoded_call.request,
            decoded_call.clientSignature,
            crate::FulfillmentType::MempoolLockAndFulfill, // High priority mempool detection
            market_addr,
            chain_id,
        );
        
        tracing::info!(
            "⚡ MEMPOOL ORDER CREATED: {} - READY FOR IMMEDIATE LOCK ATTEMPT",
            order_request.id()
        );
        
        Ok(order_request)
    }

    async fn create_mempool_order_request(&self, decoded_call: IBoundlessMarket::submitRequestCall) -> Result<OrderRequest> {
        // Create OrderRequest directly from decoded mempool transaction
        let chain_id = self.provider.get_chain_id().await
            .map_err(|e| anyhow::anyhow!("Failed to get chain_id: {}", e))?;
            
        let order_request = OrderRequest::new(
            decoded_call.request,
            decoded_call.clientSignature,
            crate::FulfillmentType::MempoolLockAndFulfill, // High priority mempool detection
            self.market_addr,
            chain_id,
        );
        
        tracing::info!(
            "⚡ MEMPOOL ORDER CREATED: {} - READY FOR IMMEDIATE LOCK ATTEMPT",
            order_request.id()
        );
        
        Ok(order_request)
    }
}

impl<P> crate::task::RetryTask for MempoolMonitor<P>
where
    P: Provider<Ethereum> + 'static + Clone,
{
    type Error = MempoolMonitorErr;

    fn spawn(&self, cancel_token: CancellationToken) -> crate::task::RetryRes<Self::Error> {
        let monitor = self.clone();
        Box::pin(async move {
            monitor.start_monitor(cancel_token).await
                .map_err(crate::task::SupervisorErr::Recover)
        })
    }
}