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
    primitives::{Address, TxHash},
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
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

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
    websocket_url: Option<String>,
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

        // Get WebSocket URL from environment or use default
        let websocket_url = std::env::var("WEBSOCKET_URL").ok()
            .or_else(|| std::env::var("RPC_URL").ok()
                .and_then(|url| {
                    // Convert http/https to ws/wss
                    if url.starts_with("http://") {
                        Some(url.replace("http://", "ws://"))
                    } else if url.starts_with("https://") {
                        Some(url.replace("https://", "wss://"))
                    } else {
                        None
                    }
                }));

        Self {
            provider,
            market_addr,
            new_order_tx,
            market_service,
            order_stream,
            processed_tx_hashes: Arc::new(Mutex::new(HashSet::new())),
            websocket_url,
        }
    }

    pub async fn start_monitor(
        &self,
        cancel_token: CancellationToken,
    ) -> Result<(), MempoolMonitorErr> {
        tracing::info!("🚀 MEMPOOL MONITOR: Starting WebSocket-based monitoring");
        tracing::info!("🎯 WATCHING CONTRACT: {:?}", self.market_addr);
        
        // WebSocket 연결 시도 (fallback to polling if WebSocket fails)
        match self.start_websocket_monitor(cancel_token.clone()).await {
            Ok(_) => {
                tracing::info!("✅ WebSocket monitoring completed successfully");
            }
            Err(e) => {
                tracing::warn!("⚠️ WebSocket monitoring failed: {:?}, falling back to polling", e);
                // Fallback to polling mode
                self.start_polling_monitor(cancel_token).await?;
            }
        }

        tracing::info!("MEMPOOL MONITOR: Stopped");
        Ok(())
    }

    async fn start_websocket_monitor(
        &self,
        cancel_token: CancellationToken,
    ) -> Result<(), MempoolMonitorErr> {
        const RECONNECT_INTERVAL: Duration = Duration::from_secs(30 * 60); // 30분마다 재연결
        
        let ws_url = self.websocket_url.as_ref()
            .ok_or_else(|| MempoolMonitorErr::MempoolPollingErr(
                anyhow::anyhow!("No WebSocket URL configured")
            ))?;

        tracing::info!("🔌 Connecting to WebSocket for mempool monitoring: {}", ws_url);
        
        // Set ping interval from environment variable (default: 30s)
        let ping_ms = std::env::var("MEMPOOL_WEBSOCKET_PING_MS")
            .unwrap_or_else(|_| "30000".to_string());
        let ping_duration = ping_ms.parse::<u64>()
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_secs(30));
            
        // Connection timeout from environment variable (default: 10 seconds)
        let timeout_secs = std::env::var("MEMPOOL_WEBSOCKET_TIMEOUT_SECS")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<u64>()
            .unwrap_or(10);
        
        tracing::info!("⚡ WebSocket config: {}ms ping, {}s timeout", ping_duration.as_millis(), timeout_secs);
        
        // Connect to WebSocket with timeout
        let ws_stream = tokio::time::timeout(
            Duration::from_secs(timeout_secs),
            connect_async(ws_url)
        ).await
        .map_err(|_| MempoolMonitorErr::MempoolPollingErr(
            anyhow::anyhow!("WebSocket connection timeout ({}s)", timeout_secs)
        ))?
        .map_err(|e| MempoolMonitorErr::MempoolPollingErr(
            anyhow::anyhow!("WebSocket connection failed: {}", e)
        ))?;
        
        let (ws_stream, _) = ws_stream;
        let (mut write, mut read) = ws_stream.split();
        
        // Subscribe to pending transactions - try different subscription methods
        tracing::info!("🔄 Attempting subscription to newPendingTransactions...");
        
        let subscribe_msg = if std::env::var("MEMPOOL_SIMPLE_SUBSCRIPTION").is_ok() {
            // 간단한 subscription (트랜잭션 해시만)
            tracing::info!("📋 Using simple subscription (hash only)");
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newPendingTransactions"]
            })
        } else {
            // 전체 트랜잭션 정보 subscription
            tracing::info!("📋 Using full transaction subscription");
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newPendingTransactions", {"includeTransactions": true}]
            })
        };
        
        tracing::debug!("📤 Sending subscription request: {}", subscribe_msg);
        
        write.send(Message::Text(subscribe_msg.to_string())).await
            .map_err(|e| MempoolMonitorErr::MempoolPollingErr(
                anyhow::anyhow!("Failed to send subscription request: {}", e)
            ))?;
        
        tracing::info!("✅ WebSocket connected, waiting for subscription confirmation...");
        
        // Ultra-fast processing channel (unbounded for speed)
        let (ultra_tx, mut ultra_rx) = tokio::sync::mpsc::unbounded_channel::<Box<OrderRequest>>();
        let main_tx = self.new_order_tx.clone();
        
        // Ultra-fast parallel forwarder (similar to offchain monitor)
        tokio::spawn(async move {
            while let Some(order) = ultra_rx.recv().await {
                let tx = main_tx.clone();
                // Immediate fire-and-forget forwarding for speed
                tokio::spawn(async move {
                    if let Err(_) = tx.send(order).await {
                        // Silent fail - prioritize speed over error reporting
                    }
                });
            }
        });
        
        // Wait for subscription confirmation
        let mut subscription_id = None;
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(response) = serde_json::from_str::<serde_json::Value>(&text) {
                        if response.get("id").and_then(|v| v.as_u64()) == Some(1) {
                            if let Some(result) = response.get("result").and_then(|v| v.as_str()) {
                                subscription_id = Some(result.to_string());
                                tracing::info!("📡 WebSocket subscription established: {}", result);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(MempoolMonitorErr::MempoolPollingErr(
                        anyhow::anyhow!("WebSocket error: {}", e)
                    ));
                }
                _ => {}
            }
        }
        
        let sub_id = subscription_id.ok_or_else(|| MempoolMonitorErr::MempoolPollingErr(
            anyhow::anyhow!("Failed to get subscription ID")
        ))?;
        
        tracing::info!("🚀 WebSocket mempool monitoring active! Subscription: {}", sub_id);
        
        // 재연결 타이머 (offchain 패턴과 동일)
        let reconnect_timer = tokio::time::sleep(RECONNECT_INTERVAL);
        tokio::pin!(reconnect_timer);
        
        // Ping interval setup
        let mut ping_interval = tokio::time::interval(ping_duration);
        let mut ping_data: Option<Vec<u8>> = None;
        
        loop {
            tokio::select! {
                // 30분 타이머 만료시 재연결 (성능 최적화)
                _ = &mut reconnect_timer => {
                    tracing::info!("⚡ Scheduled 30-minute WebSocket reconnection for performance optimization");
                    return Ok(()); // RetryTask가 자동으로 재시작
                }
                _ = cancel_token.cancelled() => {
                    tracing::info!("WebSocket monitor received cancellation signal");
                    break;
                }
                // Handle incoming WebSocket messages
                Some(msg) = read.next() => {
                    match msg {
                        Ok(Message::Text(text)) => {
                            // 🔍 DEBUG: 모든 WebSocket 메시지 로깅
                            tracing::debug!("📥 WebSocket received: {}", 
                                if text.len() > 500 { 
                                    format!("{}...[truncated {} chars]", &text[..500], text.len()) 
                                } else { 
                                    text.clone() 
                                }
                            );
                            
                            if let Ok(notification) = serde_json::from_str::<serde_json::Value>(&text) {
                                if notification.get("method").and_then(|v| v.as_str()) == Some("eth_subscription") {
                                    tracing::info!("🎯 ETH_SUBSCRIPTION detected!");
                                    if let Some(params) = notification.get("params").and_then(|v| v.as_object()) {
                                        if let Some(result) = params.get("result") {
                                            tracing::info!("🚀 Processing pending transaction...");
                                            
                                            // Add debug logging for transaction format
                                            if std::env::var("MEMPOOL_DEBUG_ALL_TX").is_ok() {
                                                tracing::info!("📋 DEBUG TX FORMAT: {}", 
                                                    serde_json::to_string_pretty(result).unwrap_or_else(|_| "Invalid JSON".to_string())
                                                );
                                            }
                                            
                                            // Ultra-fast processing with unbounded channel
                                            self.process_websocket_transaction_ultra_fast(result, &ultra_tx).await;
                                        } else {
                                            tracing::warn!("❌ eth_subscription without result: {:?}", params);
                                        }
                                    } else {
                                        tracing::warn!("❌ eth_subscription without params");
                                    }
                                } else if notification.get("id").and_then(|v| v.as_u64()) == Some(1) {
                                    // Subscription confirmation
                                    if let Some(result) = notification.get("result").and_then(|v| v.as_str()) {
                                        tracing::info!("📡 WebSocket subscription established: {}", result);
                                        tracing::info!("🚀 WebSocket mempool monitoring active!");
                                    } else if let Some(error) = notification.get("error") {
                                        tracing::error!("❌ Subscription error: {:?}", error);
                                        return Err(MempoolMonitorErr::MempoolPollingErr(
                                            anyhow::anyhow!("WebSocket subscription failed: {:?}", error)
                                        ));
                                    }
                                } else {
                                    // 기타 메시지들 로깅
                                    if let Some(method) = notification.get("method").and_then(|v| v.as_str()) {
                                        tracing::debug!("📨 WebSocket method: {} (not eth_subscription)", method);
                                    } else if notification.get("id").is_some() {
                                        tracing::debug!("📨 WebSocket response: {:?}", notification);
                                    } else {
                                        tracing::debug!("📨 WebSocket unknown message type");
                                    }
                                }
                            } else {
                                tracing::warn!("📨 Failed to parse WebSocket message as JSON: {}", 
                                    if text.len() > 200 { &text[..200] } else { &text }
                                );
                            }
                        }
                        Ok(Message::Close(_)) => {
                            tracing::warn!("WebSocket connection closed by server, will reconnect");
                            return Err(MempoolMonitorErr::MempoolPollingErr(
                                anyhow::anyhow!("WebSocket connection closed, reconnecting")
                            ));
                        }
                        Ok(Message::Ping(data)) => {
                            // Respond to server ping immediately
                            tracing::trace!("Responding to server ping");
                            if let Err(e) = write.send(Message::Pong(data)).await {
                                tracing::warn!("Failed to send pong: {:?}", e);
                                break;
                            }
                        }
                        Ok(Message::Pong(data)) => {
                            // Handle pong response
                            tracing::trace!("Received pong from server");
                            if let Some(expected_data) = ping_data.take() {
                                if expected_data == data {
                                    tracing::trace!("Pong data matches ping");
                                } else {
                                    tracing::warn!("Pong data mismatch");
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("WebSocket protocol error: {:?}", e);
                            return Err(MempoolMonitorErr::MempoolPollingErr(
                                anyhow::anyhow!("WebSocket protocol error: {}", e)
                            ));
                        }
                        _ => {}
                    }
                }
                // Send periodic ping to keep connection alive
                _ = ping_interval.tick() => {
                    let ping_bytes = format!("{}", chrono::Utc::now().timestamp()).into_bytes();
                    ping_data = Some(ping_bytes.clone());
                    
                    if let Err(e) = write.send(Message::Ping(ping_bytes)).await {
                        tracing::warn!("Failed to send ping: {:?}", e);
                        break;
                    }
                    tracing::trace!("Sent ping to server");
                }
            }
        }
        
        Ok(())
    }

    async fn process_websocket_transaction_ultra_fast(&self, tx_data: &serde_json::Value, ultra_tx: &tokio::sync::mpsc::UnboundedSender<Box<OrderRequest>>) {
        // Extract transaction details
        let to = tx_data.get("to").and_then(|v| v.as_str()).map(|s| s.to_string());
        let input = tx_data.get("input").and_then(|v| v.as_str()).map(|s| s.to_string());
        let hash = tx_data.get("hash").and_then(|v| v.as_str()).map(|s| s.to_string());
        
        // DEBUG: Log every transaction we receive to understand the format
        tracing::debug!("🔍 WebSocket TX: to={:?}, input_len={:?}, hash={:?}", 
            to, input.as_ref().map(|s| s.len()), hash);
        
        if let (Some(to_str), Some(input_str), Some(hash_str)) = (to.as_ref(), input.as_ref(), hash.as_ref()) {
            // Parse the to address
            if let Ok(to_addr) = to_str.parse::<Address>() {
                // Check if this is a transaction to our market contract
                if to_addr == self.market_addr {
                    tracing::info!("📋 Transaction to market contract: {} (checking for submitRequest...)", hash_str);
                    
                    if self.is_submit_request_calldata_hex(input_str) {
                        // Check for duplicate
                        let mut processed_hashes = self.processed_tx_hashes.lock().await;
                        if !processed_hashes.contains(hash_str) {
                            processed_hashes.insert(hash_str.to_string());
                            drop(processed_hashes);
                            
                            tracing::info!("🎯 WebSocket: submitRequest detected! Hash: {}", hash_str);
                            
                            // Ultra-fast processing - create and send order immediately
                            if let Err(e) = self.create_and_send_websocket_order_ultra_fast(tx_data, ultra_tx).await {
                                tracing::warn!("Failed to process WebSocket transaction: {:?}", e);
                            }
                        } else {
                            tracing::debug!("🔄 Duplicate WebSocket transaction skipped: {}", hash_str);
                        }
                    } else {
                        tracing::debug!("📋 Transaction to market contract but not submitRequest: {} (selector: {})", 
                            hash_str, 
                            if input_str.len() >= 10 { &input_str[2..10] } else { "N/A" }
                        );
                    }
                }
                // Log transactions to other contracts if they contain submitRequest selector
                else if self.is_submit_request_calldata_hex(input_str) {
                    tracing::info!("🧐 submitRequest call to different contract: {} -> {}", hash_str, to_str);
                }
            }
        } else {
            // Handle case where transaction format is different (maybe just hash)
            if let Some(hash_str) = hash.as_ref() {
                tracing::debug!("🔍 WebSocket received hash-only transaction: {}", hash_str);
                
                // Try to fetch full transaction details using the hash
                if let Ok(tx_hash) = hash_str.parse::<alloy::primitives::TxHash>() {
                    tokio::spawn({
                        let provider = self.provider.clone();
                        let market_addr = self.market_addr;
                        let ultra_tx = ultra_tx.clone();
                        let processed_hashes = self.processed_tx_hashes.clone();
                        let hash_str = hash_str.clone();
                        
                        async move {
                            if let Ok(Some(tx)) = provider.get_transaction_by_hash(tx_hash).await {
                                if let Some(to_addr) = tx.to() {
                                    if to_addr == market_addr {
                                        let input_bytes = tx.input().to_vec();
                                        let input_hex = format!("0x{}", hex::encode(&input_bytes));
                                        
                                        if Self::is_submit_request_calldata_static(&input_bytes) {
                                            let mut processed = processed_hashes.lock().await;
                                            if !processed.contains(&hash_str) {
                                                processed.insert(hash_str.clone());
                                                drop(processed);
                                                
                                                tracing::info!("🎯 WebSocket (hash-only): submitRequest detected! Hash: {}", hash_str);
                                                
                                                // Create transaction JSON for processing
                                                let tx_json = serde_json::json!({
                                                    "hash": hash_str,
                                                    "to": format!("{:?}", to_addr),
                                                    "input": input_hex
                                                });
                                                
                                                if let Err(e) = Self::create_and_send_websocket_order_static(&tx_json, &ultra_tx, &provider, market_addr).await {
                                                    tracing::warn!("Failed to process hash-only WebSocket transaction: {:?}", e);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    });
                }
            }
        }
    }

    async fn create_and_send_websocket_order_ultra_fast(
        &self, 
        tx_data: &serde_json::Value, 
        ultra_tx: &tokio::sync::mpsc::UnboundedSender<Box<OrderRequest>>
    ) -> Result<()> {
        let input_hex = tx_data.get("input")
            .and_then(|i| i.as_str())
            .ok_or_else(|| anyhow::anyhow!("No input field"))?;
        
        // Decode the input
        let input_bytes = hex::decode(&input_hex[2..])
            .map_err(|e| anyhow::anyhow!("Failed to decode hex: {}", e))?;
        
        let decoded_call = IBoundlessMarket::submitRequestCall::abi_decode(&input_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to decode submitRequest: {}", e))?;
        
        // Create order immediately
        let mut new_order = OrderRequest::new(
            decoded_call.request,
            decoded_call.clientSignature,
            crate::FulfillmentType::MempoolLockAndFulfill, // High priority mempool detection
            self.market_addr,
            self.provider.get_chain_id().await.unwrap_or(8453), // Default to Base chain
        );
        
        // Set short expiration for mempool orders (5 minutes for ultra-fast processing)
        use chrono::Utc;
        new_order.expire_timestamp = Some((Utc::now().timestamp() + 300) as u64); // 5 minutes
        
        let order_id = new_order.id();
        
        // Ultra-fast unbounded send (no blocking, similar to offchain monitor)
        if let Err(_) = ultra_tx.send(Box::new(new_order)) {
            tracing::error!("⚡ ULTRA: Channel closed for mempool order {}", order_id);
        } else {
            // Minimal success log for speed (trace level)
            tracing::trace!("⚡ {}", order_id);
        }
        
        Ok(())
    }
    
    async fn process_websocket_submit_request(&self, tx_data: &serde_json::Value) -> Result<()> {
        let input_hex = tx_data.get("input")
            .and_then(|i| i.as_str())
            .ok_or_else(|| anyhow::anyhow!("No input field"))?;
        
        // Decode the input
        let input_bytes = hex::decode(&input_hex[2..])
            .map_err(|e| anyhow::anyhow!("Failed to decode hex: {}", e))?;
        
        let decoded_call = IBoundlessMarket::submitRequestCall::abi_decode(&input_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to decode submitRequest: {}", e))?;
        
        let request_id = RequestId::from_lossy(decoded_call.request.id);
        
        tracing::info!(
            "⚡ WebSocket ORDER: Creating order for request_id={:?}",
            request_id
        );
        
        // Create and send the order
        let order = self.create_mempool_order_request(decoded_call).await?;
        self.new_order_tx.send(Box::new(order)).await
            .map_err(|e| anyhow::anyhow!("Failed to send order: {}", e))?;
        
        tracing::info!("✅ WebSocket order sent to processing pipeline!");
        
        Ok(())
    }

    async fn process_pending_transaction(&self, tx_hash: TxHash) -> Result<()> {
        // Fetch full transaction details
        let tx = match self.provider.get_transaction_by_hash(tx_hash).await? {
            Some(tx) => tx,
            None => return Ok(()), // Transaction not found, skip
        };

        // Check if it's a submitRequest transaction to our contract
        if let Some(to) = tx.to() {
            if to == self.market_addr {
                // Check if it's a submitRequest call
                let input_bytes = tx.input().to_vec();
                if self.is_submit_request_calldata(&input_bytes) {
                    tracing::info!("🎯 WebSocket: submitRequest detected! TxHash: {:?}", tx_hash);
                    
                    // Check for duplicate
                    let mut processed_hashes = self.processed_tx_hashes.lock().await;
                    let tx_hash_str = format!("{:?}", tx_hash);
                    
                    if !processed_hashes.contains(&tx_hash_str) {
                        processed_hashes.insert(tx_hash_str.clone());
                        drop(processed_hashes);
                        
                        // Process the transaction
                        self.process_submit_request_transaction_from_rpc(tx).await?;
                    } else {
                        tracing::debug!("🔄 Duplicate transaction skipped: {}", tx_hash_str);
                    }
                }
            }
        }
        
        Ok(())
    }

    async fn process_submit_request_transaction_from_rpc(&self, tx: alloy::rpc::types::Transaction) -> Result<()> {
        // Decode the submitRequest call
        let input_bytes = tx.input().to_vec();
        let decoded_call = IBoundlessMarket::submitRequestCall::abi_decode(&input_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to decode submitRequest: {}", e))?;
        
        let request_id = RequestId::from_lossy(decoded_call.request.id);
        
        tracing::info!(
            "📦 Creating order from transaction. RequestID: {:?}",
            request_id
        );
        
        // Create and send the order
        let order = self.create_mempool_order_request(decoded_call).await?;
        self.new_order_tx.send(Box::new(order)).await
            .map_err(|e| anyhow::anyhow!("Failed to send order: {}", e))?;
        
        Ok(())
    }

    async fn start_polling_monitor(
        &self,
        cancel_token: CancellationToken,
    ) -> Result<(), MempoolMonitorErr> {
        tracing::info!("📡 Starting polling-based mempool monitoring (fallback mode)");
        
        // Test first scan immediately
        tracing::info!("🔍 Testing first mempool scan...");
        if let Err(e) = self.scan_mempool_for_orders().await {
            tracing::error!("❌ FIRST SCAN FAILED: {:?}", e);
        } else {
            tracing::info!("✅ First scan completed successfully");
        }
        
        // Polling interval: 50ms (less aggressive than 5ms)
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("Polling monitor received cancellation signal");
                    break;
                }
                _ = interval.tick() => {
                    if let Err(err) = self.scan_mempool_for_orders().await {
                        tracing::debug!("Mempool scan failed (this is normal): {:?}", err);
                    }
                }
            }
        }
        
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
            "🚀 DECODED MEMPOOL SUBMITREQUEST: request_id={:?}, from tx hash {}",
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
            "🚀 DECODED MEMPOOL SUBMITREQUEST: request_id={:?}, from block {:?}",
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
            "🚀 DECODED MEMPOOL SUBMITREQUEST: request_id={:?}, from tx hash {}",
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

    // Static helper methods for WebSocket processing
    fn is_submit_request_calldata_static(calldata: &[u8]) -> bool {
        if calldata.len() < 4 {
            return false;
        }

        // submitRequest function selector: 0x380f9c38
        let function_selector = &calldata[0..4];
        let submit_request_selector = [0x38, 0x0f, 0x9c, 0x38];
        
        function_selector == submit_request_selector
    }

    async fn create_and_send_websocket_order_static(
        tx_data: &serde_json::Value,
        ultra_tx: &tokio::sync::mpsc::UnboundedSender<Box<OrderRequest>>,
        provider: &Arc<P>,
        market_addr: Address,
    ) -> Result<()>
    where
        P: Provider<Ethereum> + 'static + Clone,
    {
        let input_hex = tx_data.get("input")
            .and_then(|i| i.as_str())
            .ok_or_else(|| anyhow::anyhow!("No input field"))?;
        
        // Decode the input
        let input_bytes = hex::decode(&input_hex[2..])
            .map_err(|e| anyhow::anyhow!("Failed to decode hex: {}", e))?;
        
        let decoded_call = IBoundlessMarket::submitRequestCall::abi_decode(&input_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to decode submitRequest: {}", e))?;
        
        // Create order immediately
        let mut new_order = OrderRequest::new(
            decoded_call.request,
            decoded_call.clientSignature,
            crate::FulfillmentType::MempoolLockAndFulfill, // High priority mempool detection
            market_addr,
            provider.get_chain_id().await.unwrap_or(8453), // Default to Base chain
        );
        
        // Set short expiration for mempool orders (5 minutes for ultra-fast processing)
        use chrono::Utc;
        new_order.expire_timestamp = Some((Utc::now().timestamp() + 300) as u64); // 5 minutes
        
        let order_id = new_order.id();
        
        // Ultra-fast unbounded send (no blocking, similar to offchain monitor)
        if let Err(_) = ultra_tx.send(Box::new(new_order)) {
            tracing::error!("⚡ ULTRA: Channel closed for mempool order {}", order_id);
        } else {
            // Minimal success log for speed (trace level)
            tracing::info!("⚡ WebSocket order created: {}", order_id);
        }
        
        Ok(())
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