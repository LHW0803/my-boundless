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

use crate::chain_monitor::ChainHead;
use crate::OrderRequest;
use crate::{
    chain_monitor::ChainMonitorService,
    config::{ConfigLock, OrderCommitmentPriority},
    db::DbObj,
    errors::CodedError,
    impl_coded_debug, now_timestamp,
    task::{RetryRes, RetryTask, SupervisorErr},
    utils, FulfillmentType, Order,
    websocket_client::WebSocketClient,
};
use alloy::{
    consensus::{TxEip1559, TypedTransaction, SignableTransaction},
    eips::Encodable2718,
    network::{Ethereum, TransactionBuilder, TxSigner},
    primitives::{
        utils::{format_ether, parse_units},
        Address, FixedBytes, U256,
    },
    providers::{Provider, WalletProvider},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use anyhow::{Context, Result};
use boundless_market::contracts::{
    boundless_market::{BoundlessMarketService, MarketError},
    IBoundlessMarket::IBoundlessMarketErrors,
    RequestStatus, TxnErr,
};
use boundless_market::selector::SupportedSelectors;
use moka::{future::Cache, Expiry};
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use std::time::{Duration, Instant};
use thiserror::Error;
use hex;
use tokio::sync::{mpsc, Mutex};
// use tokio::task::JoinSet; // 비활성화: 사용하지 않음
use tokio_util::sync::CancellationToken;

/// Hard limit on the number of orders to concurrently kick off proving work for.
const MAX_PROVING_BATCH_SIZE: u32 = 10;

/// Global capacity snapshot - updated every 30 seconds from DB
static CAPACITY_SNAPSHOT: AtomicU32 = AtomicU32::new(0);

#[derive(Error)]
pub enum OrderMonitorErr {
    #[error("{code} Failed to lock order: {0}", code = self.code())]
    LockTxFailed(String),

    #[error("{code} Failed to confirm lock tx: {0}", code = self.code())]
    LockTxNotConfirmed(String),

    #[error("{code} Insufficient balance for lock", code = self.code())]
    InsufficientBalance,

    #[error("{code} Order already locked", code = self.code())]
    AlreadyLocked,

    #[error("{code} RPC error: {0:?}", code = self.code())]
    RpcErr(anyhow::Error),

    #[error("{code} Request status error: {0:?}", code = self.code())]
    RequestStatusErr(anyhow::Error),

    #[error("{code} Lock transaction error: {0:?}", code = self.code())]
    LockTxnErr(anyhow::Error),

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedError(#[from] anyhow::Error),
}

impl_coded_debug!(OrderMonitorErr);

impl CodedError for OrderMonitorErr {
    fn code(&self) -> &str {
        match self {
            OrderMonitorErr::LockTxNotConfirmed(_) => "[B-OM-006]",
            OrderMonitorErr::LockTxFailed(_) => "[B-OM-007]",
            OrderMonitorErr::AlreadyLocked => "[B-OM-009]",
            OrderMonitorErr::InsufficientBalance => "[B-OM-010]",
            OrderMonitorErr::RpcErr(_) => "[B-OM-011]",
            OrderMonitorErr::RequestStatusErr(_) => "[B-OM-012]",
            OrderMonitorErr::LockTxnErr(_) => "[B-OM-013]",
            OrderMonitorErr::UnexpectedError(_) => "[B-OM-500]",
        }
    }
}

/// Represents the capacity for proving orders that we have available given our config.
/// Also manages vending out capacity for proving, preventing too many proofs from being
/// kicked off in each iteration.
#[derive(Debug, PartialEq)]
enum Capacity {
    /// There are orders that have been picked for proving but not fulfilled yet.
    /// Number indicates available slots.
    Available(u32),
    /// There is no concurrent lock limit.
    Unlimited,
}

impl Capacity {
    /// Returns the number of proofs we can kick off in the current iteration. Capped at
    /// [MAX_PROVING_BATCH_SIZE] to limit number of proving tasks spawned at once.
    fn request_capacity(&self, request: u32) -> u32 {
        match self {
            Capacity::Available(capacity) => {
                if request > *capacity {
                    std::cmp::min(*capacity, MAX_PROVING_BATCH_SIZE)
                } else {
                    std::cmp::min(request, MAX_PROVING_BATCH_SIZE)
                }
            }
            Capacity::Unlimited => std::cmp::min(MAX_PROVING_BATCH_SIZE, request),
        }
    }
}

struct OrderExpiry;

impl<K: std::hash::Hash + Eq, V: std::borrow::Borrow<OrderRequest>> Expiry<K, V> for OrderExpiry {
    fn expire_after_create(&self, _key: &K, value: &V, _now: Instant) -> Option<Duration> {
        let order: &OrderRequest = value.borrow();
        order.expire_timestamp.map(|t| {
            let time_until_expiry = t.saturating_sub(now_timestamp());
            Duration::from_secs(time_until_expiry)
        })
    }
}

#[derive(Default)]
struct OrderMonitorConfig {
    min_deadline: u64,
    peak_prove_khz: Option<u64>,
    max_concurrent_proofs: Option<u32>,
    additional_proof_cycles: u64,
    batch_buffer_time_secs: u64,
    order_commitment_priority: OrderCommitmentPriority,
    priority_addresses: Option<Vec<Address>>,
}

#[derive(Clone)]
pub struct RpcRetryConfig {
    pub retry_count: u64,
    pub retry_sleep_ms: u64,
}

#[derive(Clone)]
pub struct OrderMonitor<P> {
    db: DbObj,
    chain_monitor: Arc<ChainMonitorService<P>>,
    block_time: u64,
    config: ConfigLock,
    market: BoundlessMarketService<Arc<P>>,
    market_addr: Address, // 🚀 Fast Gas Filler용 market 주소
    provider: Arc<P>,
    prover_addr: Address,
    priced_order_rx: Arc<Mutex<mpsc::Receiver<Box<OrderRequest>>>>,
    lock_and_prove_cache: Arc<Cache<String, Arc<OrderRequest>>>,
    prove_cache: Arc<Cache<String, Arc<OrderRequest>>>,
    supported_selectors: SupportedSelectors,
    rpc_retry_config: RpcRetryConfig,
    websocket_client: Arc<Mutex<WebSocketClient>>,
}

impl<P> OrderMonitor<P>
where
    P: Provider<Ethereum> + WalletProvider + Clone + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: DbObj,
        provider: Arc<P>,
        chain_monitor: Arc<ChainMonitorService<P>>,
        config: ConfigLock,
        block_time: u64,
        prover_addr: Address,
        market_addr: Address,
        priced_orders_rx: mpsc::Receiver<Box<OrderRequest>>,
        stake_token_decimals: u8,
        rpc_retry_config: RpcRetryConfig,
    ) -> Result<Self> {
        let txn_timeout_opt = {
            let config = config.lock_all().context("Failed to read config")?;
            config.batcher.txn_timeout
        };

        let mut market = BoundlessMarketService::new(
            market_addr,
            provider.clone(),
            provider.default_signer_address(),
        );
        if let Some(txn_timeout) = txn_timeout_opt {
            market = market.with_timeout(Duration::from_secs(txn_timeout));
        }
        {
            let config = config.lock_all()?;

            market = market.with_stake_balance_alert(
                &config
                    .market
                    .stake_balance_warn_threshold
                    .as_ref()
                    .map(|s| parse_units(s, stake_token_decimals).unwrap().into()),
                &config
                    .market
                    .stake_balance_error_threshold
                    .as_ref()
                    .map(|s| parse_units(s, stake_token_decimals).unwrap().into()),
            );
        }
        // 🚀 WebSocket 클라이언트 초기화 (연결은 lazy loading으로 처리)
        let websocket_client = Arc::new(Mutex::new(WebSocketClient::new_disconnected()));

        let monitor = Self {
            db,
            chain_monitor,
            block_time,
            config,
            market,
            market_addr, // 🚀 Fast Gas Filler용 market 주소 추가
            provider,
            prover_addr,
            priced_order_rx: Arc::new(Mutex::new(priced_orders_rx)),
            lock_and_prove_cache: Arc::new(Cache::builder().expire_after(OrderExpiry).build()),
            prove_cache: Arc::new(Cache::builder().expire_after(OrderExpiry).build()),
            supported_selectors: SupportedSelectors::default(),
            rpc_retry_config,
            websocket_client,
        };
        Ok(monitor)
    }

    // 🚀 MEMPOOL ONLY: Fast Gas Filler - RPC 호출 최소화 (broker.toml 설정 활용)
    async fn fast_gas_fill_for_mempool(&self) -> Result<(u64, u64, u64), OrderMonitorErr> {
        // Config에서 가스 값들 한 번에 가져오기
        let (gas_limit, priority_gas) = {
            let conf = self.config.lock_all().context("Failed to lock config")?;
            (
                conf.market.lockin_gas_estimate, // broker.toml의 lockin_gas_estimate 사용 (기본값 200k)
                conf.market.lockin_priority_gas.unwrap_or(300_000_000) // 기본값 300 gwei
            )
        };
        
        // Base fee 추정 (현재 네트워크 상황 고려)
        // 일반적으로 Base에서 base fee는 0.1-2 gwei 사이
        let base_fee = 1_000_000_000u64; // 1 gwei 고정 (안전한 값)
        let max_fee_per_gas = base_fee + priority_gas + (priority_gas / 2); // 추가 여유분
        
        
        Ok((gas_limit, max_fee_per_gas, priority_gas))
    }

    // 🚀 MEMPOOL ONLY: 직접 트랜잭션 구성으로 RPC 호출 최소화
    async fn mempool_lock_request_fast(
        &self,
        request: &boundless_market::contracts::ProofRequest,
        client_sig: Vec<u8>,
        gas_limit: u64,
        max_fee_per_gas: u64,
        priority_gas: u64,
    ) -> Result<u64, MarketError> {
        use alloy::sol_types::SolCall;
        use boundless_market::contracts::IBoundlessMarket::lockRequestCall;
        use boundless_market::contracts::boundless_market::MarketError;
        
        // ⚡ 상태 확인 생략 (mempool에서 이미 확인됨)

        // 직접 트랜잭션 구성
        let call_data = lockRequestCall {
            request: request.clone(),
            clientSignature: client_sig.into(),
        };

        // Provider에서 직접 트랜잭션 전송
        let tx = TransactionRequest::default()
            .to(*self.market.instance().address())
            .from(self.prover_addr)
            .input(call_data.abi_encode().into())
            .gas_limit(gas_limit)
            .max_fee_per_gas(max_fee_per_gas as u128)
            .max_priority_fee_per_gas(priority_gas as u128)
            .transaction_type(2); // EIP-1559

        
        // 🔄 환경변수에 따른 전송 방식 선택
        let tx_hash_string = if std::env::var("USE_HTTP").unwrap_or_default() == "true" {
            tracing::info!("🌐 Using HTTP method for transaction transmission");
            self.send_transaction_via_http(tx).await
                .map_err(|e| MarketError::Error(anyhow::anyhow!("HTTP transaction failed: {}", e)))?
        } else {
            tracing::info!("🚀 Using WebSocket method for transaction transmission");
            self.send_transaction_via_websocket(tx).await
                .map_err(|e| MarketError::Error(anyhow::anyhow!("WebSocket transaction failed: {}", e)))?
        };
        
        let tx_hash: FixedBytes<32> = tx_hash_string.parse()
            .map_err(|e| MarketError::Error(anyhow::anyhow!("Invalid transaction hash format: {}", e)))?;


        // 트랜잭션 확인 (WebSocket 후 provider로 receipt 조회)
        // Receipt 재시도 로직 추가 (3회)
        let mut receipt = None;
        let max_retries = 3;
        let retry_delay = std::time::Duration::from_millis(500);
        
        for attempt in 0..max_retries {
            match self.provider.get_transaction_receipt(tx_hash).await {
                Ok(Some(r)) => {
                    // Receipt 받으면 즉시 revert 확인하고 재시도 중단
                    if !r.status() {
                        return Err(MarketError::LockRevert(r.transaction_hash));
                    }
                    receipt = Some(r);
                    break;
                }
                Ok(None) => {
                    if attempt < max_retries - 1 {
                        // 블록체인에 TX가 존재하는지 확인
                        if let Ok(Some(_tx)) = self.provider.get_transaction_by_hash(tx_hash).await {
                            tracing::debug!("TX {} exists in mempool/pending, waiting for receipt (attempt {}/{})", 
                                          tx_hash, attempt + 1, max_retries);
                        } else {
                            tracing::warn!("TX {} not found in blockchain, attempt {}/{}", 
                                         tx_hash, attempt + 1, max_retries);
                        }
                        tokio::time::sleep(retry_delay).await;
                    }
                }
                Err(e) => {
                    if attempt == max_retries - 1 {
                        return Err(MarketError::TxnConfirmationError(e.into()));
                    }
                    tokio::time::sleep(retry_delay).await;
                }
            }
        }
        
        let receipt = receipt.ok_or_else(|| {
            MarketError::TxnConfirmationError(anyhow::anyhow!("Transaction receipt not found after {} retries", max_retries))
        })?;

        tracing::info!(
            "🚀 FAST LOCK SUCCESS: Request 0x{:x}, tx: {}, block: {}",
            request.id,
            receipt.transaction_hash,
            receipt.block_number.unwrap_or_default()
        );

        Ok(receipt.block_number.unwrap_or_default())
    }

    // 🚀 WebSocket을 통한 완전한 Raw Transaction 전송
    async fn send_transaction_via_websocket(&self, tx: TransactionRequest) -> Result<String, anyhow::Error> {
        let start_time = std::time::Instant::now();
        
        // 1. 트랜잭션 필드 완성 (~3-5ms)
        let chain_id = 8453u64; // Base mainnet chain ID (하드코딩으로 최적화)
        
        let nonce = if let Some(n) = tx.nonce {
            n
        } else {
            self.provider.get_transaction_count(self.prover_addr).await?
        };
        
        // 2. EIP-1559 트랜잭션 구성 (~1-2ms)
        let typed_tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: tx.gas.unwrap_or(21000),
            max_fee_per_gas: tx.max_fee_per_gas.unwrap_or(0),
            max_priority_fee_per_gas: tx.max_priority_fee_per_gas.unwrap_or(0),
            to: tx.to.unwrap_or_default(),
            value: tx.value.unwrap_or_default(),
            input: tx.input.input.unwrap_or_default(),
            access_list: Default::default(),
        };
        
        
        // 3. 완전한 WebSocket 전용 raw transaction 생성
        // HTTP 없이 순수 WebSocket만 사용
        
        // 서명을 위해 현재 환경의 private key 사용 (기존 설정 재활용)
        // 실제 환경에서는 broker가 이미 private key를 가지고 있음
        
        // ✅ PRODUCTION: PRIVATE_KEY 환경변수를 사용한 실제 서명
        let private_key_str = std::env::var("PRIVATE_KEY")
            .map_err(|_| anyhow::anyhow!("환경변수 PRIVATE_KEY가 설정되지 않았습니다"))?;
        
        let signer = private_key_str.parse::<PrivateKeySigner>()
            .map_err(|e| anyhow::anyhow!("PRIVATE_KEY 파싱 오류: {}", e))?;
        
        
        // EIP-1559 트랜잭션을 TypedTransaction으로 변환
        let mut typed_transaction = TypedTransaction::Eip1559(typed_tx);
        
        // 트랜잭션 서명
        let signature = signer.sign_transaction(&mut typed_transaction).await
            .map_err(|e| anyhow::anyhow!("트랜잭션 서명 실패: {}", e))?;
        
        
        // 서명된 트랜잭션 생성 및 RLP 인코딩
        let signed_tx = typed_transaction.into_signed(signature);
        // Signed 타입에서 encoded_2718 메서드를 사용하여 바이트 얻기
        let raw_tx_bytes = signed_tx.encoded_2718();
        let raw_tx = format!("0x{}", hex::encode(&raw_tx_bytes));
        
        
        // 5. WebSocket으로 전송 (~30-50ms)
        let mut websocket_client = self.websocket_client.lock().await;
        
        let tx_hash = websocket_client.send_raw_transaction(&raw_tx).await
            .map_err(|e| anyhow::anyhow!("WebSocket transmission failed: {}", e))?;
        
        let total_time = start_time.elapsed();
        tracing::info!("⚡ WebSocket TX Success: {} ({:?})", tx_hash, total_time);
        
        Ok(tx_hash)
    }

    // 🌐 HTTP를 통한 기존 트랜잭션 전송 (비교용)
    async fn send_transaction_via_http(&self, tx: TransactionRequest) -> Result<String, anyhow::Error> {
        let start_time = std::time::Instant::now();
        
        let pending_tx = self.provider.send_transaction(tx).await
            .map_err(|e| anyhow::anyhow!("HTTP transaction failed: {}", e))?;
        
        let tx_hash = format!("{:?}", pending_tx.tx_hash());
        
        let total_time = start_time.elapsed();
        tracing::info!("🌐 HTTP TX Success: {} ({:?})", tx_hash, total_time);
        
        Ok(tx_hash)
    }

    // 🚀 MEMPOOL ONLY: 극한 최적화된 락킹 (검증 최소화)
    async fn mempool_lock_order_fast(&self, order: &OrderRequest) -> Result<U256, OrderMonitorErr> {
        let request_id = order.request.id;

        // ⚡ Phase 1.1: Status 검증 완전 생략 (20-100ms 절약)
        // mempool에서 감지된 주문은 이미 유효한 것으로 간주
        
        // ⚡ Phase 1.2: DB 중복 검사 생략 (1-5ms 절약) 
        // mempool only 모드에서는 경쟁이 적어 불필요

        // 🚀 MEMPOOL ONLY: Fast Gas Filler 사용 (1.2초 → 0.1초)
        let (gas_limit, max_fee_per_gas, priority_gas) = self.fast_gas_fill_for_mempool().await?;

        tracing::info!(
            "🚀 MEMPOOL FAST LOCK: 0x{:x} | Stake: {} | FastGas: Limit={}, MaxFee={} gwei, Priority={} gwei",
            request_id,
            order.request.offer.lockStake,
            gas_limit,
            max_fee_per_gas / 1_000_000_000,
            priority_gas / 1_000_000_000
        );

        // ⚡ Phase 2.1: 직접 트랜잭션 구성으로 RPC 호출 최소화
        let lock_block = self
            .mempool_lock_request_fast(&order.request, order.client_sig.to_vec(), gas_limit, max_fee_per_gas, priority_gas)
            .await
            .map_err(|e| -> OrderMonitorErr {
                match e {
                    MarketError::TxnError(txn_err) => match txn_err {
                        TxnErr::BoundlessMarketErr(IBoundlessMarketErrors::RequestIsLocked(_)) => {
                            OrderMonitorErr::AlreadyLocked
                        }
                        _ => OrderMonitorErr::LockTxFailed(txn_err.to_string()),
                    },
                    MarketError::RequestAlreadyLocked(_e) => OrderMonitorErr::AlreadyLocked,
                    MarketError::TxnConfirmationError(e) => {
                        OrderMonitorErr::LockTxNotConfirmed(e.to_string())
                    }
                    MarketError::LockRevert(e) => {
                        OrderMonitorErr::LockTxFailed(format!("Tx hash 0x{e:x}"))
                    }
                    MarketError::Error(e) => {
                        let prover_addr_str =
                            self.prover_addr.to_string().to_lowercase().replace("0x", "");
                        if e.to_string().contains("InsufficientBalance") {
                            if e.to_string().to_lowercase().contains(&prover_addr_str) {
                                OrderMonitorErr::InsufficientBalance
                            } else {
                                OrderMonitorErr::LockTxFailed(format!(
                                    "Requestor has insufficient balance at lock time: {e}"
                                ))
                            }
                        } else if e.to_string().contains("RequestIsLocked") {
                            OrderMonitorErr::AlreadyLocked
                        } else {
                            OrderMonitorErr::UnexpectedError(e)
                        }
                    }
                    _ => {
                        if e.to_string().contains("RequestIsLocked") {
                            OrderMonitorErr::AlreadyLocked
                        } else {
                            OrderMonitorErr::UnexpectedError(e.into())
                        }
                    }
                }
            })?;

        // ⚡ Phase 2.2: 즉시 추정 가격 반환 (라이프타임 문제 해결)
        // 백그라운드 처리 대신 즉시 추정값 반환으로 극한 성능 달성
        let estimated_price = order.request.offer.minPrice; // 최소 가격으로 추정
        tracing::info!("⚡ MEMPOOL FAST LOCK COMPLETED: 0x{:x} | Estimated Price: {}", request_id, estimated_price);
        
        Ok(estimated_price)
    }

    async fn lock_order(&self, order: &OrderRequest) -> Result<U256, OrderMonitorErr> {
        // Check if offchain_only_mode is enabled
        let offchain_mode = {
            let config = self.config.lock_all().context("Failed to read config")?;
            config.market.offchain_only_mode
        };
        
        if offchain_mode {
            return self.lock_order_offchain_direct(order).await;
        }
        
        let request_id = order.request.id;

        let order_status = self
            .market
            .get_status(request_id, Some(order.request.expires_at()))
            .await
            .context("Failed to get request status")
            .map_err(OrderMonitorErr::RpcErr)?;
        if order_status != RequestStatus::Unknown {
            tracing::info!("Request {:x} not open: {order_status:?}, skipping", request_id);
            // TODO: fetch some chain data to find out who / and for how much the order
            // was locked in at
            return Err(OrderMonitorErr::AlreadyLocked);
        }

        let is_locked = self
            .db
            .is_request_locked(U256::from(order.request.id))
            .await
            .context("Failed to check if request is locked")?;
        if is_locked {
            tracing::warn!("Request 0x{:x} already locked: {order_status:?}, skipping", request_id);
            return Err(OrderMonitorErr::AlreadyLocked);
        }

        let conf_priority_gas = {
            let conf = self.config.lock_all().context("Failed to lock config")?;
            conf.market.lockin_priority_gas
        };

        tracing::info!(
            "🔐 SUBMITTING LOCK TX: 0x{:x} | Stake: {} | Priority Gas: {:?}",
            request_id,
            order.request.offer.lockStake,
            conf_priority_gas
        );
        
        let lock_block = self
            .market
            .lock_request(&order.request, order.client_sig.clone(), conf_priority_gas)
            .await
            .map_err(|e| -> OrderMonitorErr {
                match e {
                    MarketError::TxnError(txn_err) => match txn_err {
                        TxnErr::BoundlessMarketErr(IBoundlessMarketErrors::RequestIsLocked(_)) => {
                            OrderMonitorErr::AlreadyLocked
                        }
                        _ => OrderMonitorErr::LockTxFailed(txn_err.to_string()),
                    },
                    MarketError::RequestAlreadyLocked(_e) => OrderMonitorErr::AlreadyLocked,
                    MarketError::TxnConfirmationError(e) => {
                        OrderMonitorErr::LockTxNotConfirmed(e.to_string())
                    }
                    MarketError::LockRevert(e) => {
                        // Note: lock revert could be for any number of reasons;
                        // 1/ someone may have locked in the block before us,
                        // 2/ the lock may have expired,
                        // 3/ the request may have been fulfilled,
                        // 4/ the requestor may have withdrawn their funds
                        // Currently we don't have a way to determine the cause of the revert.
                        OrderMonitorErr::LockTxFailed(format!("Tx hash 0x{e:x}"))
                    }
                    MarketError::Error(e) => {
                        // Insufficient balance error is thrown both when the requestor has insufficient balance,
                        // Requestor having insufficient balance can happen and is out of our control. The prover
                        // having insufficient balance is unexpected as we should have checked for that before
                        // committing to locking the order.
                        let prover_addr_str =
                            self.prover_addr.to_string().to_lowercase().replace("0x", "");
                        if e.to_string().contains("InsufficientBalance") {
                            if e.to_string().to_lowercase().contains(&prover_addr_str) {
                                OrderMonitorErr::InsufficientBalance
                            } else {
                                OrderMonitorErr::LockTxFailed(format!(
                                    "Requestor has insufficient balance at lock time: {e}"
                                ))
                            }
                        } else if e.to_string().contains("RequestIsLocked") {
                            OrderMonitorErr::AlreadyLocked
                        } else {
                            OrderMonitorErr::UnexpectedError(e)
                        }
                    }
                    _ => {
                        if e.to_string().contains("RequestIsLocked") {
                            OrderMonitorErr::AlreadyLocked
                        } else {
                            OrderMonitorErr::UnexpectedError(e.into())
                        }
                    }
                }
            })?;

        // Fetch the block to retrieve the lock timestamp. This has been observed to return
        // inconsistent state between the receipt being available but the block not yet.
        let lock_timestamp = crate::futures_retry::retry(
            self.rpc_retry_config.retry_count,
            self.rpc_retry_config.retry_sleep_ms,
            || async {
                Ok(self
                    .provider
                    .get_block_by_number(lock_block.into())
                    .await
                    .with_context(|| format!("failed to get block {lock_block}"))?
                    .with_context(|| format!("failed to get block {lock_block}: block not found"))?
                    .header
                    .timestamp)
            },
            "get_block_by_number",
        )
        .await
        .map_err(OrderMonitorErr::UnexpectedError)?;

        let lock_price = order
            .request
            .offer
            .price_at(lock_timestamp)
            .context("Failed to calculate lock price")?;

        Ok(lock_price)
    }

    async fn lock_order_offchain_direct(&self, order: &OrderRequest) -> Result<U256, OrderMonitorErr> {
        let request_id = order.request.id;
        
        // Skip all pre-validation - contract will handle it
        // No get_status check
        // No is_request_locked DB check
        
        // Get priority gas from config
        let conf_priority_gas = {
            let conf = self.config.lock_all().context("Failed to lock config")?;
            conf.market.lockin_priority_gas
        };
        
        tracing::info!(
            "⚡ Offchain: Direct lock 0x{:x} | gas: {} gwei",
            request_id,
            conf_priority_gas.unwrap_or(0) / 1_000_000_000
        );
        
        // Send lock transaction directly
        let lock_block = self
            .market
            .lock_request(&order.request, order.client_sig.clone(), conf_priority_gas)
            .await
            .map_err(|e| -> OrderMonitorErr {
                match e {
                    MarketError::TxnError(txn_err) => match txn_err {
                        TxnErr::BoundlessMarketErr(IBoundlessMarketErrors::RequestIsLocked(_)) => {
                            tracing::debug!("Order already locked");
                            OrderMonitorErr::AlreadyLocked
                        }
                        _ => {
                            tracing::warn!("Lock failed: {}", txn_err);
                            OrderMonitorErr::LockTxFailed(txn_err.to_string())
                        }
                    },
                    MarketError::RequestAlreadyLocked(_e) => OrderMonitorErr::AlreadyLocked,
                    _ => {
                        tracing::warn!("Lock failed: {}", e);
                        OrderMonitorErr::UnexpectedError(e.into())
                    }
                }
            })?;
        
        // Use minPrice instead of calculating from timestamp
        let lock_price = order.request.offer.minPrice;
        
        tracing::info!(
            "✅ Offchain: Lock success 0x{:x} | block: {} | price: {}",
            request_id,
            lock_block,
            format_ether(lock_price)
        );
        
        Ok(lock_price)
    }

    async fn get_proving_order_capacity(
        &self,
        max_concurrent_proofs: Option<u32>,
        prev_orders_by_status: &mut String,
    ) -> Result<Capacity, OrderMonitorErr> {
        if max_concurrent_proofs.is_none() {
            return Ok(Capacity::Unlimited);
        };

        let max = max_concurrent_proofs.unwrap();
        let committed_orders = self
            .db
            .get_committed_orders()
            .await
            .map_err(|e| OrderMonitorErr::UnexpectedError(e.into()))?;
        let committed_orders_count: u32 = committed_orders.len().try_into().unwrap();

        Self::log_capacity(prev_orders_by_status, committed_orders, max).await;

        let available_slots = max.saturating_sub(committed_orders_count);
        Ok(Capacity::Available(available_slots))
    }

    async fn log_capacity(
        prev_orders_by_status: &mut String,
        commited_orders: Vec<Order>,
        max: u32,
    ) {
        let committed_orders_count: u32 = commited_orders.len().try_into().unwrap();
        let request_id_and_status = commited_orders
            .iter()
            .map(|order| format!("[{:?}]: {order}", order.status))
            .collect::<Vec<_>>();

        let capacity_log = format!("Current num committed orders: {committed_orders_count}. Maximum commitment: {max}. Committed orders: {request_id_and_status:?}");

        // Note: we don't compare previous to capacity_log as it contains timestamps which cause it to always change.
        // We only want to log if status or num orders changes.
        let cur_orders_by_status = commited_orders
            .iter()
            .map(|order| format!("{:?}-{}", order.status, order.id()))
            .collect::<Vec<_>>()
            .join(",");
        if *prev_orders_by_status != cur_orders_by_status {
            tracing::info!("{}", capacity_log);
            *prev_orders_by_status = cur_orders_by_status;
        }
    }

    /// Helper method to skip an order in the database and invalidate the appropriate cache
    async fn skip_order(&self, order: &OrderRequest, reason: &str) {
        if let Err(e) = self.db.insert_skipped_request(order).await {
            tracing::error!("Failed to skip order ({}): {} - {e:?}", reason, order.id());
        }

        match order.fulfillment_type {
            FulfillmentType::LockAndFulfill | FulfillmentType::MempoolLockAndFulfill => {
                self.lock_and_prove_cache.invalidate(&order.id()).await;
            }
            FulfillmentType::FulfillAfterLockExpire | FulfillmentType::FulfillWithoutLocking => {
                self.prove_cache.invalidate(&order.id()).await;
            }
        }
    }

    async fn get_valid_orders(
        &self,
        current_block_timestamp: u64,
        min_deadline: u64,
    ) -> Result<Vec<Arc<OrderRequest>>> {
        let mut candidate_orders: Vec<Arc<OrderRequest>> = Vec::new();

        fn is_within_deadline(
            order: &OrderRequest,
            current_block_timestamp: u64,
            min_deadline: u64,
        ) -> bool {
            let expiration = order.expiry();
            if expiration < current_block_timestamp {
                tracing::debug!("Request {:x} has now expired. Skipping.", order.request.id);
                false
            } else if expiration.saturating_sub(now_timestamp()) < min_deadline {
                tracing::debug!("Request {:x} deadline at {} is less than the minimum deadline {} seconds required to prove an order. Skipping.", order.request.id, expiration, min_deadline);
                false
            } else {
                true
            }
        }

        fn is_target_time_reached(order: &OrderRequest, current_block_timestamp: u64) -> bool {
            // 🚀 MEMPOOL PRIORITY: Skip timestamp check for mempool orders (immediate processing)
            if order.fulfillment_type == FulfillmentType::MempoolLockAndFulfill {
                tracing::debug!("🚀 MEMPOOL FAST TRACK: Skipping timestamp check for immediate processing");
                return true;
            }
            
            // Note: this could use current timestamp, but avoiding cases where clock has drifted.
            match order.target_timestamp {
                Some(target_timestamp) => {
                    if current_block_timestamp < target_timestamp {
                        tracing::trace!(
                            "Request {:x} target timestamp {} not yet reached (current: {}). Waiting.",
                            order.request.id,
                            target_timestamp,
                            current_block_timestamp
                        );
                        false
                    } else {
                        true
                    }
                }
                None => {
                    // Should not happen, just warning for safety as this condition is not strictly
                    // enforced at compile time.
                    tracing::warn!("Request {:x} has no target timestamp set", order.request.id);
                    false
                }
            }
        }

        for (_, order) in self.prove_cache.iter() {
            let is_fulfilled = self
                .db
                .is_request_fulfilled(U256::from(order.request.id))
                .await
                .context("Failed to check if request is fulfilled")?;
            if is_fulfilled {
                tracing::debug!(
                    "Request 0x{:x} was locked by another prover and was fulfilled. Skipping.",
                    order.request.id
                );
                self.skip_order(&order, "was fulfilled by other").await;
            } else if !is_within_deadline(&order, current_block_timestamp, min_deadline) {
                self.skip_order(&order, "expired").await;
            } else if is_target_time_reached(&order, current_block_timestamp) {
                tracing::info!("Request 0x{:x} was locked by another prover but expired unfulfilled, setting status to pending proving", order.request.id);
                candidate_orders.push(order);
            }
        }

        for (_, order) in self.lock_and_prove_cache.iter() {
            let is_lock_expired = order.request.lock_expires_at() < current_block_timestamp;
            if is_lock_expired {
                tracing::debug!("Request {:x} was scheduled to be locked by us, but its lock has now expired. Skipping.", order.request.id);
                self.skip_order(&order, "lock expired before we locked").await;
            } else if let Some((locker, _)) =
                self.db.get_request_locked(U256::from(order.request.id)).await?
            {
                let our_address = self.provider.default_signer_address().to_string().to_lowercase();
                let locker_address = locker.to_lowercase();
                // Compare normalized addresses (lowercase without 0x prefix)
                let our_address_normalized = our_address.trim_start_matches("0x");
                let locker_address_normalized = locker_address.trim_start_matches("0x");

                if locker_address_normalized != our_address_normalized {
                    tracing::debug!("Request 0x{:x} was scheduled to be locked by us ({}), but is already locked by another prover ({}). Skipping.", order.request.id, our_address, locker_address);
                    self.skip_order(&order, "locked by another prover").await;
                } else {
                    // Edge case where we locked the order, but due to some reason was not moved to proving state. Should not happen.
                    tracing::debug!("Request 0x{:x} was scheduled to be locked by us, but is already locked by us. Proceeding to prove.", order.request.id);
                    candidate_orders.push(order);
                }
            } else if !is_within_deadline(&order, current_block_timestamp, min_deadline) {
                self.skip_order(&order, "insufficient deadline").await;
            } else if is_target_time_reached(&order, current_block_timestamp) {
                candidate_orders.push(order);
            }
        }

        if candidate_orders.is_empty() {
            tracing::trace!(
                "No orders to lock and/or prove as of block timestamp {}",
                current_block_timestamp
            );
            return Ok(Vec::new());
        }

        tracing::debug!(
            "Valid orders that reached target timestamp; ready for locking/proving, num: {}, ids: {}",
            candidate_orders.len(),
            candidate_orders.iter().map(|order| order.id()).collect::<Vec<_>>().join(", ")
        );

        Ok(candidate_orders)
    }

    async fn lock_and_prove_orders(&self, orders: &[Arc<OrderRequest>]) -> Result<()> {
        let lock_jobs = orders.iter().map(|order| {
            async move {
                let order_id = order.id();
                // 🔧 MempoolLockAndFulfill도 락킹이 필요함
                if order.fulfillment_type == FulfillmentType::LockAndFulfill || 
                   order.fulfillment_type == FulfillmentType::MempoolLockAndFulfill {
                    let request_id = order.request.id;
                    tracing::info!("🔒 ATTEMPTING LOCK: {} (type: {:?})", order_id, order.fulfillment_type);
                    match self.lock_order(order).await {
                        Ok(lock_price) => {
                            tracing::info!("✅ LOCK SUCCESS: 0x{:x} | Price: {} | Type: {:?}", 
                                         request_id, lock_price, order.fulfillment_type);
                            if let Err(err) = self.db.insert_accepted_request(order, lock_price).await {
                                tracing::error!(
                                    "FATAL STAKE AT RISK: {} failed to move from locking -> proving status {}",
                                    order_id,
                                    err
                                );
                            }
                        }
                        Err(ref err) => {
                            match err {
                                OrderMonitorErr::UnexpectedError(inner) => {
                                    tracing::error!(
                                        "❌ LOCK FAILED (UNEXPECTED): 0x{:x} - {} - {inner:?}",
                                        request_id, err.code()
                                    );
                                }
                                OrderMonitorErr::AlreadyLocked => {
                                    tracing::warn!("⚠️ LOCK FAILED (ALREADY LOCKED): 0x{:x} - {}", 
                                                 request_id, err.code());
                                }
                                _ => {
                                    tracing::warn!(
                                        "⚠️ LOCK FAILED: 0x{:x} - {} - {err:?}",
                                        request_id, err.code()
                                    );
                                }
                            }
                            if let Err(err) = self.db.insert_skipped_request(order).await {
                                tracing::error!(
                                    "Failed to set DB failure state for order: {order_id} - {err:?}"
                                );
                            }
                        }
                    }
                    self.lock_and_prove_cache.invalidate(&order_id).await;
                } else {
                    if let Err(err) = self.db.insert_accepted_request(order, U256::ZERO).await {
                        tracing::error!(
                            "Failed to set order status to pending proving: {} - {err:?}",
                            order_id
                        );
                    }
                    self.prove_cache.invalidate(&order_id).await;
                }
            }
        });

        futures::future::join_all(lock_jobs).await;

        Ok(())
    }

    /// Calculate the gas units needed for an order and the corresponding cost in wei
    async fn calculate_order_gas_cost_wei(
        &self,
        order: &OrderRequest,
        gas_price: u128,
    ) -> Result<U256, OrderMonitorErr> {
        // Calculate gas units needed for this order (lock + fulfill)
        let order_gas_units = if order.fulfillment_type == FulfillmentType::LockAndFulfill {
            U256::from(utils::estimate_gas_to_lock(&self.config, order).await?).saturating_add(
                U256::from(
                    utils::estimate_gas_to_fulfill(
                        &self.config,
                        &self.supported_selectors,
                        &order.request,
                    )
                    .await?,
                ),
            )
        } else {
            U256::from(
                utils::estimate_gas_to_fulfill(
                    &self.config,
                    &self.supported_selectors,
                    &order.request,
                )
                .await?,
            )
        };

        let order_cost_wei = U256::from(gas_price) * order_gas_units;

        Ok(order_cost_wei)
    }

    async fn apply_capacity_limits(
        &self,
        orders: Vec<Arc<OrderRequest>>,
        config: &OrderMonitorConfig,
        prev_orders_by_status: &mut String,
    ) -> Result<Vec<Arc<OrderRequest>>> {
        let num_orders = orders.len();
        // Get our current capacity for proving orders given our config and the number of orders that are currently committed to be proven + fulfilled.
        let capacity = self
            .get_proving_order_capacity(config.max_concurrent_proofs, prev_orders_by_status)
            .await?;
        let capacity_granted: usize = capacity
            .request_capacity(num_orders.try_into().expect("Failed to convert order count to u32"))
            as usize;

        // 📊 Also log current snapshot for comparison
        let snapshot_count = CAPACITY_SNAPSHOT.load(Ordering::Relaxed);
        let max_proofs = config.max_concurrent_proofs.unwrap_or(999);
        
        tracing::info!(
            "📊 REGULAR ORDER PROCESSING: {} orders ready | DB capacity: {capacity:?}, Granted: {capacity_granted} | Snapshot: {snapshot_count}/{max_proofs}",
            num_orders
        );

        let mut final_orders: Vec<Arc<OrderRequest>> = Vec::with_capacity(capacity_granted);

        // Get current gas price and available balance
        let gas_price =
            self.chain_monitor.current_gas_price().await.context("Failed to get gas price")?;
        let available_balance_wei = self
            .provider
            .get_balance(self.provider.default_signer_address())
            .await
            .map_err(|err| OrderMonitorErr::RpcErr(err.into()))?;

        // Calculate gas units required for committed orders
        let committed_orders = self.db.get_committed_orders().await?;
        let committed_gas_units =
            futures::future::try_join_all(committed_orders.iter().map(|order| {
                utils::estimate_gas_to_fulfill(
                    &self.config,
                    &self.supported_selectors,
                    &order.request,
                )
            }))
            .await?
            .iter()
            .sum::<u64>();

        // Calculate cost in wei
        let committed_cost_wei = U256::from(gas_price) * U256::from(committed_gas_units);

        // Log committed order gas requirements
        if !committed_orders.is_empty() {
            tracing::debug!(
                "Cost for {} committed orders: {} ether",
                committed_orders.len(),
                format_ether(committed_cost_wei),
            );
        }

        // Ensure we have enough for committed orders
        if committed_cost_wei > available_balance_wei {
            tracing::error!(
                "Insufficient balance for committed orders. Required: {} ether, Available: {} ether",
                format_ether(committed_cost_wei),
                format_ether(available_balance_wei)
            );
            return Ok(Vec::new());
        }

        // Calculate remaining balance after accounting for committed orders
        let mut remaining_balance_wei = available_balance_wei - committed_cost_wei;

        // Apply peak khz limit if specified
        let num_commited_orders = committed_orders.len();
        if config.peak_prove_khz.is_some() && !orders.is_empty() {
            let peak_prove_khz = config.peak_prove_khz.unwrap();
            let total_commited_cycles = committed_orders
                .iter()
                .map(|order| order.total_cycles.unwrap() + config.additional_proof_cycles)
                .sum::<u64>();

            let now = now_timestamp();
            // Estimate the time the prover will be available given our current committed orders.
            let started_proving_at = committed_orders
                .iter()
                .map(|order| order.proving_started_at.unwrap())
                .min()
                .unwrap_or(now);

            let proof_time_seconds = total_commited_cycles.div_ceil(1_000).div_ceil(peak_prove_khz);
            let mut prover_available_at = started_proving_at + proof_time_seconds;
            if prover_available_at < now {
                let seconds_behind = now - prover_available_at;
                tracing::warn!("Proofs are behind what is estimated from peak_prove_khz config by {} seconds. Consider lowering this value to avoid overlocking orders.", seconds_behind);
                prover_available_at = now;
            }

            // For each order in consideration, check if it can be completed before its expiration
            // and that there is enough gas to pay for the lock and fulfillment of all orders
            // including the committed orders.
            for order in orders {
                if final_orders.len() >= capacity_granted {
                    break;
                }
                // Calculate gas and cost for this order using our helper method
                let order_cost_wei = self.calculate_order_gas_cost_wei(&order, gas_price).await?;

                // Skip if not enough balance
                if order_cost_wei > remaining_balance_wei {
                    tracing::warn!(
                        "Insufficient balance for order {}. Required: {} ether, Remaining: {} ether",
                        order.id(),
                        format_ether(order_cost_wei),
                        format_ether(remaining_balance_wei)
                    );
                    self.skip_order(&order, "insufficient balance").await;
                    continue;
                }

                let Some(order_cycles) = order.total_cycles else {
                    tracing::warn!("Order 0x{:x} has no total cycles, preflight was skipped? Not considering for peak khz limit", order.request.id);
                    final_orders.push(order);
                    remaining_balance_wei -= order_cost_wei;
                    continue;
                };

                // Calculate total cycles including application proof, assessor, and set builder estimates
                let total_cycles = order_cycles + config.additional_proof_cycles;

                let proof_time_seconds = total_cycles.div_ceil(1_000).div_ceil(peak_prove_khz);
                let completion_time = prover_available_at + proof_time_seconds;
                let expiration = order.expiry();

                if completion_time + config.batch_buffer_time_secs > expiration {
                    // If the order cannot be completed before its expiration, skip it permanently.
                    // Otherwise, we keep the order for the next iteration as capacity may free up in the future.

                    if now + proof_time_seconds > expiration {
                        tracing::info!("Order 0x{:x} cannot be completed before its expiration at {}, proof estimated to take {} seconds and complete at {}. Skipping", 
                            order.request.id,
                            expiration,
                            proof_time_seconds,
                            completion_time
                        );
                        // If the order cannot be completed regardless of other orders, skip it
                        // permanently. Otherwise, will retry including the order.
                        self.skip_order(&order, "cannot be completed before expiration").await;
                    } else {
                    }
                    continue;
                }


                final_orders.push(order);
                prover_available_at = completion_time;
                remaining_balance_wei -= order_cost_wei;
            }
        } else {
            // If no peak khz limit, just check gas for each order
            for order in orders {
                if final_orders.len() >= capacity_granted {
                    break;
                }
                let order_cost_wei = self.calculate_order_gas_cost_wei(&order, gas_price).await?;

                // Skip if not enough balance
                if order_cost_wei > remaining_balance_wei {
                    tracing::warn!(
                        "Insufficient balance for order {}. Required: {} ether, Remaining: {} ether",
                        order.id(),
                        format_ether(order_cost_wei),
                        format_ether(remaining_balance_wei)
                    );
                    self.skip_order(&order, "insufficient balance").await;
                    continue;
                }

                final_orders.push(order);
                remaining_balance_wei -= order_cost_wei;
            }
        }

        tracing::info!(
            "Started with {} orders ready to be locked and/or proven. Already commited to {} orders. After applying capacity limits of {} max concurrent proofs and {} peak khz, filtered to {} orders: {:?}",
            num_orders,
            num_commited_orders,
            if let Some(max_concurrent_proofs) = config.max_concurrent_proofs {
                max_concurrent_proofs.to_string()
            } else {
                "unlimited".to_string()
            },
            if let Some(peak_prove_khz) = config.peak_prove_khz {
                peak_prove_khz.to_string()
            } else {
                "unlimited".to_string()
            },
            final_orders.len(),
            final_orders.iter().map(|order| order.id()).collect::<Vec<_>>()
        );

        Ok(final_orders)
    }

    pub async fn start_monitor(
        self,
        cancel_token: CancellationToken,
    ) -> Result<(), OrderMonitorErr> {
        // 📊 Initialize capacity snapshot from DB
        let initial_committed_orders = self
            .db
            .get_committed_orders()
            .await
            .map_err(|e| OrderMonitorErr::UnexpectedError(e.into()))?;
        CAPACITY_SNAPSHOT.store(initial_committed_orders.len() as u32, Ordering::Relaxed);
        
        let max_proofs = {
            let config = self.config.lock_all().context("Failed to read config")?;
            config.market.max_concurrent_proofs.unwrap_or(999)
        };
        
        tracing::info!(
            "🔄 CAPACITY INITIALIZED: {}/{} active proofs",
            initial_committed_orders.len(),
            max_proofs
        );

        // 🔄 Start background capacity sync task (every 30 seconds)
        let db_clone = self.db.clone();
        let config_clone = self.config.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                
                match db_clone.get_committed_orders().await {
                    Ok(orders) => {
                        let count = orders.len() as u32;
                        CAPACITY_SNAPSHOT.store(count, Ordering::Relaxed);
                        
                        // Get current max_proofs from config
                        let max_proofs = {
                            match config_clone.lock_all() {
                                Ok(config) => config.market.max_concurrent_proofs.unwrap_or(999),
                                Err(_) => 999, // fallback if config lock fails
                            }
                        };
                        
                        tracing::info!(
                            "📊 CAPACITY SYNC: {}/{} active proofs (updated every 30s)",
                            count,
                            max_proofs
                        );
                    }
                    Err(e) => {
                        tracing::error!("Failed to sync capacity snapshot: {:?}", e);
                    }
                }
            }
        });

        let mut last_block = 0;
        let mut first_block = 0;
        
        // 🚀 이벤트 기반: 폴링 대신 즉시 처리
        // 멤풀 모드에서는 블록 체크를 최소화
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now(),
            tokio::time::Duration::from_secs(30), // 블록 체크는 30초마다만 (가스비 업데이트용)
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut new_orders = self.priced_order_rx.lock().await;
        let mut prev_orders_by_status = String::new();
        
        // 단순화: 병렬 처리 제거

        loop {
            tokio::select! {
                // 🎯 새 주문 도착 시 즉시 처리 (최우선)
                biased;

                Some(result) = new_orders.recv() => {
                    // 🚀 이벤트 기반: 새 주문이 오면 즉시 처리
                    if let Err(e) = self.handle_new_order_immediate(result).await {
                        tracing::error!("Failed to handle new order: {:?}", e);
                    }
                }
                
                // 병렬 처리 제거됨

                // 블록 체크는 가끔만 (가스비 업데이트 등)
                _ = interval.tick() => {
                    let ChainHead { block_number, block_timestamp } =
                        self.chain_monitor.current_chain_head().await?;
                    if block_number != last_block {
                        last_block = block_number;
                        if first_block == 0 {
                            first_block = block_number;
                        }
                        tracing::trace!(
                            "Order monitor processing block {block_number} at timestamp {block_timestamp}"
                        );

                        let monitor_config = {
                            let config = self.config.lock_all().context("Failed to read config")?;
                            OrderMonitorConfig {
                                min_deadline: config.market.min_deadline,
                                peak_prove_khz: config.market.peak_prove_khz,
                                max_concurrent_proofs: config.market.max_concurrent_proofs,
                                additional_proof_cycles: config.market.additional_proof_cycles,
                                batch_buffer_time_secs: config.batcher.block_deadline_buffer_secs,
                                order_commitment_priority: config.market.order_commitment_priority,
                                priority_addresses: config.market.priority_requestor_addresses.clone(),
                            }
                        }; // config guard is dropped here, before any await points

                        // Get orders that are valid and ready for locking/proving, skipping orders that are now invalid for proving, due to expiring, being locked by another prover, etc.
                        let mut valid_orders = self.get_valid_orders(block_timestamp, monitor_config.min_deadline).await?;

                        if valid_orders.is_empty() {
                            tracing::trace!(
                                "No orders to lock and/or prove as of block timestamp {}",
                                block_timestamp
                            );
                            continue;
                        }

                        // Prioritize the orders that intend to fulfill based on configured commitment priority.
                        valid_orders = self.prioritize_orders(valid_orders, monitor_config.order_commitment_priority, monitor_config.priority_addresses.as_deref());

                        // Filter down the orders given our max concurrent proofs, peak khz limits, and gas limitations.
                        let final_orders = self
                            .apply_capacity_limits(
                                valid_orders,
                                &monitor_config,
                                &mut prev_orders_by_status,
                            )
                            .await?;

                        tracing::trace!("After processing block {}[timestamp {}], we will now start locking and/or proving {} orders.",
                            block_number,
                            block_timestamp,
                            final_orders.len(),
                        );

                        if !final_orders.is_empty() {
                            // Lock and prove filtered orders.
                            self.lock_and_prove_orders(&final_orders).await?;
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    tracing::debug!("Order monitor received cancellation");
                    break;
                }
            }
        }
        Ok(())
    }

    // 🚀 이벤트 기반: 새 주문 즉시 처리 (멤풀 주문은 즉시 락킹)
    async fn handle_new_order_immediate(
        &self,
        order: Box<OrderRequest>,
    ) -> Result<(), OrderMonitorErr> {
        // 멤풀 주문인지 확인
        let is_mempool = order.fulfillment_type == FulfillmentType::MempoolLockAndFulfill;
        
        if is_mempool {
            // 📊 Check capacity before processing mempool order
            let current_count = CAPACITY_SNAPSHOT.load(Ordering::Relaxed);
            let max_proofs = {
                let config = self.config.lock_all().context("Failed to read config")?;
                config.market.max_concurrent_proofs.unwrap_or(999)
            };
            
            tracing::info!(
                "⚡ MEMPOOL ORDER RECEIVED: {} - Current capacity: {}/{}",
                order.id(), current_count, max_proofs
            );
            
            if current_count >= max_proofs {
                tracing::warn!(
                    "❌ MEMPOOL ORDER REJECTED: {} - Capacity full ({}/{})",
                    order.id(), current_count, max_proofs
                );
                
                // Record rejected order in DB
                let order_arc: Arc<OrderRequest> = Arc::from(order);
                if let Err(db_err) = self.db.insert_skipped_request(&*order_arc).await {
                    tracing::error!("Failed to record rejected mempool order: {} - {:?}", order_arc.id(), db_err);
                }
                return Ok(());
            }
            
            // 🔥 멤풀 주문은 즉시 락킹 시도 (capacity 여유 있을 때만)
            let order_arc: Arc<OrderRequest> = Arc::from(order);
            
            // 🚀 MEMPOOL ONLY: 최적화된 락킹 메서드 사용
            match self.mempool_lock_order_fast(&*order_arc).await {
                Ok(lock_price) => {
                    // 📈 Increment capacity counter immediately 
                    CAPACITY_SNAPSHOT.fetch_add(1, Ordering::Relaxed);
                    let new_count = current_count + 1;
                    
                    tracing::info!(
                        "✅ MEMPOOL ORDER LOCKED: {} - Price: {} - New capacity: {}/{}",
                        order_arc.id(), lock_price, new_count, max_proofs
                    );
                    
                    // DB에 락 성공 기록 (상태를 PendingProving으로 변경)
                    if let Err(db_err) = self.db.insert_accepted_request(&*order_arc, lock_price).await {
                        tracing::error!(
                            "FATAL STAKE AT RISK: {} failed to move from locking -> proving status {}",
                            order_arc.id(),
                            db_err
                        );
                        // Decrement counter on DB error
                        CAPACITY_SNAPSHOT.fetch_sub(1, Ordering::Relaxed);
                    }
                    
                    // 🚀 증명 시작: 캐시에서 제거하고 증명 단계로 이동
                    self.lock_and_prove_cache.invalidate(&order_arc.id()).await;
                    // (증명은 별도 시스템에서 OrderStatus::PendingProving 상태를 보고 처리)
                    tracing::info!("🔬 MEMPOOL ORDER MOVED TO PROVING: {} - Proof generation will start", order_arc.id());
                }
                Err(e) => {
                    tracing::error!("❌ MEMPOOL ORDER LOCKING FAILED: {} - {:?}", order_arc.id(), e);
                    // 락킹 실패 시 DB에 실패 기록
                    if let Err(db_err) = self.db.insert_skipped_request(&*order_arc).await {
                        tracing::error!("Failed to record skipped mempool order: {} - {:?}", order_arc.id(), db_err);
                    }
                }
            }
        } else {
            // 일반 주문은 기존 방식으로 처리 (캐시에 추가)
            self.handle_new_order_result(order).await?;
        }
        
        Ok(())
    }
    
    // Called when a new order result is received from the channel
    async fn handle_new_order_result(
        &self,
        order: Box<OrderRequest>,
    ) -> Result<(), OrderMonitorErr> {
        match order.fulfillment_type {
            FulfillmentType::LockAndFulfill | FulfillmentType::MempoolLockAndFulfill => {
                // Note: this could be done without waiting for the batch to minimize latency, but
                //       avoiding more complicated logic for checking capacity for each order.

                // If not, add it to the cache to be locked after target time
                self.lock_and_prove_cache.insert(order.id(), Arc::from(order) as Arc<OrderRequest>).await;
            }
            FulfillmentType::FulfillAfterLockExpire | FulfillmentType::FulfillWithoutLocking => {
                self.prove_cache.insert(order.id(), Arc::from(order) as Arc<OrderRequest>).await;
            }
        }
        Ok(())
    }
}

impl<P> RetryTask for OrderMonitor<P>
where
    P: Provider<Ethereum> + WalletProvider + 'static + Clone,
{
    type Error = OrderMonitorErr;
    fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        let monitor_clone = self.clone();
        Box::pin(async move {
            tracing::info!("Starting order monitor");
            monitor_clone.start_monitor(cancel_token).await.map_err(SupervisorErr::Recover)?;
            Ok(())
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::OrderStatus;
    use crate::{db::SqliteDb, now_timestamp, FulfillmentType};
    use alloy::node_bindings::AnvilInstance;
    use alloy::{
        network::EthereumWallet,
        node_bindings::Anvil,
        primitives::{Address, U256},
        providers::{
            fillers::{
                BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
                WalletFiller,
            },
            ProviderBuilder, RootProvider,
        },
        signers::local::PrivateKeySigner,
    };
    use boundless_market::contracts::{
        Offer, Predicate, PredicateType, ProofRequest, RequestId, RequestInput, RequestInputType,
        Requirements,
    };
    use boundless_market_test_utils::{
        deploy_boundless_market, deploy_hit_points, ASSESSOR_GUEST_ID, ASSESSOR_GUEST_PATH,
    };

    use risc0_zkvm::Digest;
    use std::{future::Future, sync::Arc};
    use tokio::task::JoinSet;
    use tracing_test::traced_test;

    type TestProvider = FillProvider<
        JoinFill<
            JoinFill<
                alloy::providers::Identity,
                JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
            >,
            WalletFiller<EthereumWallet>,
        >,
        RootProvider,
    >;

    pub struct TestCtx {
        pub monitor: OrderMonitor<TestProvider>,
        pub anvil: AnvilInstance,
        pub db: DbObj,
        pub market_address: Address,
        #[allow(dead_code)]
        pub config: ConfigLock,
        pub priced_order_tx: mpsc::Sender<Box<OrderRequest>>,
        pub signer: PrivateKeySigner,
        pub market_service: BoundlessMarketService<Arc<TestProvider>>,
        next_order_id: u32, // Counter to assign unique order IDs
    }

    impl TestCtx {
        // Convert the standalone function to a method on TestCtx
        pub async fn create_test_order(
            &mut self,
            fulfillment_type: FulfillmentType,
            bidding_start: u64,
            lock_timeout: u64,
            timeout: u64,
        ) -> Box<OrderRequest> {
            let request_id = self.next_order_id;
            self.next_order_id += 1;

            let request = ProofRequest::new(
                RequestId::new(self.signer.address(), request_id),
                Requirements::new(
                    Digest::ZERO,
                    Predicate {
                        predicateType: PredicateType::PrefixMatch,
                        data: Default::default(),
                    },
                ),
                "http://risczero.com/image",
                RequestInput { inputType: RequestInputType::Inline, data: Default::default() },
                Offer {
                    minPrice: U256::from(1),
                    maxPrice: U256::from(2),
                    biddingStart: bidding_start,
                    rampUpPeriod: 1,
                    timeout: timeout as u32,
                    lockTimeout: lock_timeout as u32,
                    lockStake: U256::from(0),
                },
            );

            let client_sig = request
                .sign_request(&self.signer, self.market_address, self.anvil.chain_id())
                .await
                .unwrap()
                .as_bytes()
                .into();

            Box::new(OrderRequest {
                target_timestamp: Some(0),
                request,
                image_id: None,
                input_id: None,
                expire_timestamp: None,
                client_sig,
                fulfillment_type,
                boundless_market_address: self.market_address,
                chain_id: self.anvil.chain_id(),
                total_cycles: None,
            })
        }
    }

    pub async fn setup_om_test_context() -> TestCtx {
        let anvil = Anvil::new().spawn();
        let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
        let provider = Arc::new(
            ProviderBuilder::new()
                .wallet(EthereumWallet::from(signer.clone()))
                .connect(&anvil.endpoint())
                .await
                .unwrap(),
        );

        // Deploy contracts
        let hit_points = deploy_hit_points(signer.address(), provider.clone()).await.unwrap();

        let market_address = deploy_boundless_market(
            signer.address(),
            provider.clone(),
            Address::ZERO,
            hit_points,
            Digest::from(ASSESSOR_GUEST_ID),
            format!("file://{ASSESSOR_GUEST_PATH}"),
            Some(signer.address()),
        )
        .await
        .unwrap();

        // Set up market service
        let market_service = BoundlessMarketService::new(
            market_address,
            provider.clone(),
            provider.default_signer_address(),
        );

        // Deposit ETH into the contract for the prover to use when locking orders
        // Using 10 ETH to ensure plenty of funds for tests
        let stake_token_decimals = market_service.stake_token_decimals().await.unwrap();
        market_service
            .deposit(parse_units("10.0", stake_token_decimals).unwrap().into())
            .await
            .unwrap();

        let db: DbObj = Arc::new(SqliteDb::new("sqlite::memory:").await.unwrap());
        let config = ConfigLock::default();

        config.load_write().unwrap().market.min_deadline = 50;
        config.load_write().unwrap().market.lockin_gas_estimate = 200_000;
        config.load_write().unwrap().market.fulfill_gas_estimate = 300_000;
        config.load_write().unwrap().market.groth16_verify_gas_estimate = 50_000;

        let block_time = 2;

        let chain_monitor = Arc::new(ChainMonitorService::new(provider.clone()).await.unwrap());
        tokio::spawn(chain_monitor.spawn(Default::default()));

        // Create required channels for tests
        let (priced_order_tx, priced_order_rx) = mpsc::channel(16);

        let monitor = OrderMonitor::new(
            db.clone(),
            provider.clone(),
            chain_monitor.clone(),
            config.clone(),
            block_time,
            signer.address(),
            market_address,
            priced_order_rx,
            stake_token_decimals,
            RpcRetryConfig { retry_count: 2, retry_sleep_ms: 500 },
        )
        .unwrap();

        TestCtx {
            monitor,
            anvil,
            db,
            market_address,
            config,
            priced_order_tx,
            signer,
            market_service,
            next_order_id: 1, // Initialize with 1 instead of 0
        }
    }

    async fn run_with_monitor<P, F, T>(monitor: OrderMonitor<P>, f: F) -> T
    where
        P: Provider + WalletProvider + Clone + 'static,
        F: Future<Output = T>,
    {
        // A JoinSet automatically aborts all its tasks when dropped
        let mut tasks = JoinSet::new();
        // Spawn the monitor
        tasks.spawn(async move { monitor.start_monitor(Default::default()).await });

        tokio::select! {
            result = f => result,
            monitor_task_result = tasks.join_next() => {
                panic!("Monitor exited unexpectedly: {:?}", monitor_task_result.unwrap());
            },
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn monitor_block() {
        let mut ctx = setup_om_test_context().await;

        // Create a test order using the TestCtx helper method
        let order =
            ctx.create_test_order(FulfillmentType::LockAndFulfill, now_timestamp(), 100, 200).await;

        let order_id = order.id();

        let _request_id =
            ctx.market_service.submit_request(&order.request, &ctx.signer).await.unwrap();

        // Send the order to the monitor
        ctx.priced_order_tx.send(order).await.unwrap();

        run_with_monitor(ctx.monitor, async move {
            // loop for 20 seconds
            for _ in 0..20 {
                let order = ctx.db.get_order(&order_id).await.unwrap();
                if order.is_some() {
                    assert_eq!(order.unwrap().status, OrderStatus::PendingProving);
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }

            let order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
            assert_eq!(order.status, OrderStatus::PendingProving);
        })
        .await;
    }

    // Capacity tests
    #[test]
    fn test_capacity_unlimited() {
        let capacity = Capacity::Unlimited;
        assert_eq!(capacity.request_capacity(0), 0);
        assert_eq!(capacity.request_capacity(15), MAX_PROVING_BATCH_SIZE);
        assert_eq!(capacity.request_capacity(MAX_PROVING_BATCH_SIZE), MAX_PROVING_BATCH_SIZE);
    }

    #[test]
    fn test_capacity_proving() {
        let capacity = Capacity::Available(50);
        assert_eq!(capacity.request_capacity(0), 0);
        assert_eq!(capacity.request_capacity(4), 4);
        assert_eq!(capacity.request_capacity(10), MAX_PROVING_BATCH_SIZE);
    }

    // Filtering tests
    #[tokio::test]
    #[traced_test]
    async fn test_filter_expired_orders() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        // Create an expired order
        let expired_order = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp - 100, 50, 50)
            .await;
        let expired_order_id = expired_order.id();
        ctx.monitor
            .lock_and_prove_cache
            .insert(expired_order_id.clone(), Arc::from(expired_order))
            .await;

        let result = ctx.monitor.get_valid_orders(current_timestamp, 0).await.unwrap();

        assert!(result.is_empty());

        let order = ctx.db.get_order(&expired_order_id).await.unwrap().unwrap();
        assert_eq!(order.status, OrderStatus::Skipped);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_filter_insufficient_deadline() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        // Create an order with insufficient deadline
        let order =
            ctx.create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 45, 45).await;
        let order_1_id = order.id();
        ctx.monitor.lock_and_prove_cache.insert(order_1_id.clone(), Arc::from(order)).await;

        // Create an order with insufficient deadline
        let order = ctx
            .create_test_order(FulfillmentType::FulfillAfterLockExpire, current_timestamp, 1, 45)
            .await;
        let order_2_id = order.id();
        ctx.monitor.prove_cache.insert(order_2_id.clone(), Arc::from(order)).await;

        let result = ctx.monitor.get_valid_orders(current_timestamp, 100).await.unwrap();

        assert!(result.is_empty());

        let order = ctx.db.get_order(&order_1_id).await.unwrap().unwrap();
        assert_eq!(order.status, OrderStatus::Skipped);

        let order = ctx.db.get_order(&order_2_id).await.unwrap().unwrap();
        assert_eq!(order.status, OrderStatus::Skipped);
    }

    #[tokio::test]
    async fn test_filter_locked_by_others() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        // Create an order that's locked by another prover
        let order = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
            .await;
        let order_id = order.id();
        ctx.db
            .set_request_locked(
                U256::from(order.request.id),
                &Address::ZERO.to_string(),
                current_timestamp,
            )
            .await
            .unwrap();
        ctx.monitor.lock_and_prove_cache.insert(order.id(), Arc::from(order)).await;

        let result =
            ctx.monitor.get_valid_orders(current_timestamp, current_timestamp + 100).await.unwrap();

        assert!(result.is_empty());

        let order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
        assert_eq!(order.status, OrderStatus::Skipped);
    }

    // Processing tests
    #[tokio::test]
    #[traced_test]
    async fn test_process_fulfill_after_lock_expire_orders() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        let order = ctx
            .create_test_order(FulfillmentType::FulfillAfterLockExpire, current_timestamp, 100, 200)
            .await;
        let order_id = order.id();

        ctx.monitor.lock_and_prove_orders(&[Arc::from(order)]).await.unwrap();

        let updated_order = ctx.db.get_order(&order_id).await.unwrap().unwrap();
        assert_eq!(updated_order.status, OrderStatus::PendingProving);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_apply_capacity_limits_unlimited() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        // Create multiple orders
        let mut orders = Vec::new();
        for _ in 1..=5 {
            let order = ctx
                .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
                .await;

            // Submit requests to blockchain
            let _request_id =
                ctx.market_service.submit_request(&order.request, &ctx.signer).await.unwrap();

            orders.push(Arc::from(order));
        }

        // Set unlimited capacity in config
        ctx.config.load_write().unwrap().market.max_concurrent_proofs = None;

        // Process all orders with unlimited capacity
        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(
                orders.clone(),
                &OrderMonitorConfig::default(),
                &mut String::new(),
            )
            .await
            .unwrap();
        let result = ctx.monitor.lock_and_prove_orders(&filtered_orders).await;
        assert!(result.is_ok(), "lock_and_prove_orders should succeed");

        // All orders should be processed since capacity is unlimited
        let mut processed_count = 0;
        for order in orders {
            if let Some(order) = ctx.db.get_order(&order.id()).await.unwrap() {
                processed_count += 1;
                assert_eq!(order.status, OrderStatus::PendingProving);
            }
        }

        // Should process all 5 orders
        assert_eq!(
            processed_count, 5,
            "Should have processed all 5 orders with unlimited capacity"
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn test_apply_capacity_limits_proving() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        // Add a committed order to simulate existing workload
        let committed_order = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
            .await;
        let mut committed_order = committed_order.to_proving_order(Default::default());
        committed_order.status = OrderStatus::Proving;
        committed_order.proving_started_at = Some(current_timestamp);
        ctx.db.add_order(&committed_order).await.unwrap();

        // Create multiple new orders
        let mut orders = Vec::new();
        for _ in 1..=5 {
            let order = ctx
                .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
                .await;

            // Submit requests to blockchain
            let _request_id =
                ctx.market_service.submit_request(&order.request, &ctx.signer).await.unwrap();

            orders.push(Arc::from(order));
        }

        // Process orders with limited capacity
        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(
                orders,
                &OrderMonitorConfig {
                    max_concurrent_proofs: Some(3),
                    order_commitment_priority: OrderCommitmentPriority::ShortestExpiry,
                    ..Default::default()
                },
                &mut String::new(),
            )
            .await
            .unwrap();
        ctx.monitor.lock_and_prove_orders(&filtered_orders).await.unwrap();

        // Count processed orders
        let mut processed_count = 0;
        for order in filtered_orders {
            if let Some(order) = ctx.db.get_order(&order.id()).await.unwrap() {
                processed_count += 1;
                assert_eq!(order.status, OrderStatus::PendingProving);
            }
        }

        // Should only process 2 more orders (3 total with 1 already committed)
        assert_eq!(
            processed_count, 2,
            "Should have processed only 2 more orders due to concurrent proving capacity limit"
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn test_apply_capacity_limits_committed_work_too_large() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        // Add a large committed order to simulate existing workload
        let committed_order = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
            .await;
        let mut committed_order = committed_order.to_proving_order(Default::default());
        committed_order.status = OrderStatus::Proving;
        committed_order.total_cycles = Some(10_000_000_000_000_000);
        committed_order.proving_started_at = Some(current_timestamp);
        ctx.db.add_order(&committed_order).await.unwrap();

        let mut orders = Vec::new();

        let mut order1 = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
            .await;
        order1.total_cycles = Some(1000);

        orders.push(Arc::from(order1));

        let mut order2 = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
            .await;
        order2.total_cycles = Some(100);
        orders.push(Arc::from(order2));

        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(
                orders,
                &OrderMonitorConfig { peak_prove_khz: Some(100), ..Default::default() },
                &mut String::new(),
            )
            .await
            .unwrap();

        assert_eq!(filtered_orders.len(), 0);
        assert!(logs_contain("cannot be completed before its expiration"));
        assert!(logs_contain("Started with 2 orders"));
        assert!(logs_contain("filtered to 0 orders: []"));
    }

    #[tokio::test]
    async fn test_apply_capacity_limits_skip_proof_time_past_expiration() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();

        // Create orders with different expiration times
        let mut candidate_orders = Vec::new();

        // Order 1: Will expire soon (not enough time to prove)
        let mut order1 =
            ctx.create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 5, 5).await;
        order1.total_cycles = Some(1000000000000);
        let order1_id = order1.id();

        let _request_id =
            ctx.market_service.submit_request(&order1.request, &ctx.signer).await.unwrap();
        candidate_orders.push(Arc::from(order1));

        // Order 2: Longer expiration (enough time to prove)
        let mut order2 = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 100, 200)
            .await;
        order2.total_cycles = Some(2000);
        let order2_id = order2.id();
        let _request_id =
            ctx.market_service.submit_request(&order2.request, &ctx.signer).await.unwrap();
        candidate_orders.push(Arc::from(order2));

        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(
                candidate_orders,
                &OrderMonitorConfig { peak_prove_khz: Some(1), ..Default::default() },
                &mut String::new(),
            )
            .await
            .unwrap();

        assert_eq!(filtered_orders[0].total_cycles, Some(2000));
        assert_eq!(filtered_orders[0].id(), order2_id);

        // The first order should be skipped due to insufficient proof time before expiration
        let order1_db = ctx.db.get_order(&order1_id).await.unwrap();
        assert_eq!(
            order1_db.unwrap().status,
            OrderStatus::Skipped,
            "Order 1 should be skipped due to insufficient time to complete proof"
        );
    }

    #[tokio::test]
    async fn test_gas_estimation_functions() {
        let mut ctx = setup_om_test_context().await;

        // Create orders with different fulfillment types to test gas estimation for each type
        let lock_and_fulfill_order =
            ctx.create_test_order(FulfillmentType::LockAndFulfill, now_timestamp(), 100, 200).await;
        let lock_and_fulfill_id = lock_and_fulfill_order.id();

        let fulfill_only_order = ctx
            .create_test_order(FulfillmentType::FulfillAfterLockExpire, now_timestamp(), 100, 200)
            .await;
        let fulfill_only_id = fulfill_only_order.id();

        let _lock_request_id = ctx
            .market_service
            .submit_request(&lock_and_fulfill_order.request, &ctx.signer)
            .await
            .unwrap();
        let _fulfill_request_id = ctx
            .market_service
            .submit_request(&fulfill_only_order.request, &ctx.signer)
            .await
            .unwrap();

        let orders = vec![Arc::from(lock_and_fulfill_order), Arc::from(fulfill_only_order)];
        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(orders, &OrderMonitorConfig::default(), &mut String::new())
            .await
            .unwrap();
        let result = ctx.monitor.lock_and_prove_orders(&filtered_orders).await;
        assert!(result.is_ok(), "lock_and_prove_orders should succeed");

        // Verify both orders were processed correctly
        let lock_order_result = ctx.db.get_order(&lock_and_fulfill_id).await.unwrap();
        let fulfill_order_result = ctx.db.get_order(&fulfill_only_id).await.unwrap();

        assert!(lock_order_result.is_some(), "Lock and fulfill order should be processed");
        assert!(fulfill_order_result.is_some(), "Fulfill only order should be processed");

        assert_eq!(lock_order_result.unwrap().status, OrderStatus::PendingProving);
        assert_eq!(fulfill_order_result.unwrap().status, OrderStatus::PendingProving);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_multiple_orders_khz_capacity() {
        let mut ctx = setup_om_test_context().await;
        ctx.config.load_write().unwrap().market.max_concurrent_proofs = None;

        // Create multiple orders with increasing cycle counts to test gas allocation
        let mut orders = Vec::new();
        for i in 1..6 {
            let mut order = ctx
                .create_test_order(FulfillmentType::LockAndFulfill, now_timestamp(), 120, 120)
                .await;

            // Set increasing cycle counts to test different gas requirements
            order.total_cycles = Some(i as u64 * 1_000_000);

            let _request_id =
                ctx.market_service.submit_request(&order.request, &ctx.signer).await.unwrap();

            orders.push(Arc::from(order));
        }

        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(
                orders,
                &OrderMonitorConfig { peak_prove_khz: Some(100), ..Default::default() },
                &mut String::new(),
            )
            .await
            .unwrap();

        println!("filtered_orders: {filtered_orders:?}");
        // 100khz can prove 1m+2m+3m+4m (10m) cycles in 100 seconds
        assert_eq!(filtered_orders.len(), 4);

        assert_eq!(filtered_orders[0].total_cycles, Some(1_000_000));
        assert_eq!(filtered_orders[3].total_cycles, Some(4_000_000));
    }

    #[tokio::test]
    #[traced_test]
    async fn test_insufficient_balance_committed_orders() {
        let mut ctx = setup_om_test_context().await;

        let balance = ctx.monitor.provider.get_balance(ctx.signer.address()).await.unwrap();
        let gas_price = ctx.monitor.provider.get_gas_price().await.unwrap();
        let gas_remaining: u64 = (balance / U256::from(gas_price)).try_into().unwrap();
        ctx.config.load_write().unwrap().market.fulfill_gas_estimate = gas_remaining / 2;
        ctx.config.load_write().unwrap().market.lockin_gas_estimate = gas_remaining / 3;

        let incoming_order =
            ctx.create_test_order(FulfillmentType::LockAndFulfill, now_timestamp(), 100, 200).await;

        let mut orders = vec![Arc::from(incoming_order)];

        // Should be able to have enough gas for 1 lock and fulfill
        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(
                orders.clone(),
                &OrderMonitorConfig::default(),
                &mut String::new(),
            )
            .await
            .unwrap();
        assert_eq!(filtered_orders.len(), 1);

        orders.push(Arc::from(
            ctx.create_test_order(FulfillmentType::LockAndFulfill, now_timestamp(), 100, 200).await,
        ));

        // Should still only be able to have enough gas for 1 lock and fulfill
        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(
                orders.clone(),
                &OrderMonitorConfig::default(),
                &mut String::new(),
            )
            .await
            .unwrap();
        assert_eq!(filtered_orders.len(), 1);

        for _ in 0..3 {
            let committed_order = ctx
                .create_test_order(FulfillmentType::LockAndFulfill, now_timestamp(), 100, 200)
                .await;

            let mut committed_order_obj = committed_order.to_proving_order(Default::default());
            committed_order_obj.status = OrderStatus::Proving;
            committed_order_obj.proving_started_at = Some(now_timestamp());
            ctx.db.add_order(&committed_order_obj).await.unwrap();
        }

        // Process the order - with insufficient balance for committed orders
        let filtered_orders = ctx
            .monitor
            .apply_capacity_limits(orders, &OrderMonitorConfig::default(), &mut String::new())
            .await
            .unwrap();

        assert!(filtered_orders.is_empty());
    }

    #[tokio::test]
    #[traced_test]
    async fn test_target_timestamp_prevents_early_locking() {
        let mut ctx = setup_om_test_context().await;
        let current_timestamp = now_timestamp();
        let future_timestamp = current_timestamp + 100; // 100 seconds in the future

        // Create orders of both types and set them to be picked up at a future timestamp.
        let mut lock_and_fulfill_order = ctx
            .create_test_order(FulfillmentType::LockAndFulfill, current_timestamp, 200, 300)
            .await;

        lock_and_fulfill_order.target_timestamp = Some(future_timestamp);
        let lock_and_fulfill_order_id = lock_and_fulfill_order.id();

        ctx.monitor
            .lock_and_prove_cache
            .insert(lock_and_fulfill_order.id(), Arc::from(lock_and_fulfill_order))
            .await;

        let mut fulfill_after_expire_order = ctx
            .create_test_order(
                FulfillmentType::FulfillAfterLockExpire,
                current_timestamp - 50,
                10,
                300,
            )
            .await;

        fulfill_after_expire_order.target_timestamp = Some(future_timestamp);
        let fulfill_after_expire_order_id = fulfill_after_expire_order.id();

        // Simulate that this order was locked by another prover but the lock has now expired
        ctx.db
            .set_request_locked(
                U256::from(fulfill_after_expire_order.request.id),
                &Address::ZERO.to_string(),
                current_timestamp - 50,
            )
            .await
            .unwrap();

        ctx.monitor
            .prove_cache
            .insert(fulfill_after_expire_order.id(), Arc::from(fulfill_after_expire_order))
            .await;

        // Call get_valid_orders with current timestamp - this should NOT return either order
        // because their target_timestamp is in the future
        let valid_orders = ctx.monitor.get_valid_orders(current_timestamp, 50).await.unwrap();

        assert!(
            valid_orders.is_empty(),
            "Orders with future target_timestamp should not be valid yet, got {} orders",
            valid_orders.len()
        );

        // Verify both orders are still in their respective caches and not skipped
        let cached_lock_order =
            ctx.monitor.lock_and_prove_cache.get(&lock_and_fulfill_order_id).await;
        assert!(cached_lock_order.is_some(), "LockAndFulfill order should still be in cache");

        let cached_prove_order = ctx.monitor.prove_cache.get(&fulfill_after_expire_order_id).await;
        assert!(
            cached_prove_order.is_some(),
            "FulfillAfterLockExpire order should still be in cache"
        );

        // Now test with future timestamp - both orders should be valid
        let valid_orders_in_future =
            ctx.monitor.get_valid_orders(future_timestamp + 1, 50).await.unwrap();

        assert_eq!(
            valid_orders_in_future.len(),
            2,
            "Both orders should be valid when current time >= target_timestamp"
        );

        assert!(valid_orders_in_future.iter().any(|order| order.id() == lock_and_fulfill_order_id));
        assert!(valid_orders_in_future
            .iter()
            .any(|order| order.id() == fulfill_after_expire_order_id));
    }
}
