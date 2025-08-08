// Parallel RPC Lock Transaction Implementation for Maximum Success Rate
use alloy::{
    network::Ethereum,
    primitives::{Address, Bytes, FixedBytes, U256},
    providers::{Provider, ProviderBuilder, WalletProvider},
    rpc::types::TransactionRequest,
};
use anyhow::Result;
use boundless_market::contracts::{ProofRequest, boundless_market::BoundlessMarketService};
use futures::future::join_all;
use std::sync::Arc;
use std::time::Instant;

/// 7개 RPC를 사용한 병렬 lock 트랜잭션 제출
/// 가장 빠른 RPC가 성공하면 즉시 반환
pub async fn submit_lock_transaction_parallel<P>(
    request: &ProofRequest,
    signature: alloy::primitives::Bytes,
    priority_gas: Option<u64>,
    market_addr: Address,
    provider: Arc<P>,
) -> Result<u64>
where
    P: Provider<Ethereum> + WalletProvider + Clone + 'static,
{
    let rpcs = vec![
        "https://mainnet-preconf.base.org", // Flashblocks - 최우선 (200ms 블록)
        "https://base-mainnet.g.alchemy.com/v2/LzOCLmaolNovcIhTvHpbNnBSnhGfSEFP",
        "https://base-mainnet.infura.io/v3/b287021875ef4eba8d11bc98c32d78a4",
        "https://cool-old-theorem.base-mainnet.quiknode.pro/90b7fb86615eee742b5c9bdcf46282072f690717/",
        "https://base-mainnet.core.chainstack.com/7169d70533af0103ae9984799c80cf76",
        "https://lb.drpc.org/base/ApklQs6JokXxjviH4hmf1eWmN3aZdDIR8IfXIgaNGuYu",
        "https://api.zan.top/node/v1/base/mainnet/da21214226664fc9bb3136d61034c99c",
    ];

    let request_id = request.id;
    tracing::info!("🚀 Parallel lock submission for request 0x{:x} using {} RPCs", request_id, rpcs.len());

    // 각 RPC에서 lock 트랜잭션 제출
    let handles: Vec<_> = rpcs.into_iter().enumerate().map(|(index, rpc_url)| {
        let req = request.clone();
        let sig = signature.clone();
        let gas = priority_gas;
        let url = rpc_url.to_string();
        let prov = provider.clone();
        let market = market_addr.clone();
        
        tokio::spawn(async move {
            let start = Instant::now();
            
            // URL 파싱 실패 시 None 반환
            let Ok(parsed_url) = url.parse() else {
                tracing::debug!("Failed to parse URL: {}", url);
                return None;
            };
            
            // 각 RPC용 BoundlessMarketService 생성
            #[allow(deprecated)]
            let rpc_provider = ProviderBuilder::new()
                .wallet(prov.wallet().clone())
                .on_http(parsed_url);
            
            let market_service = BoundlessMarketService::new(
                market,
                rpc_provider.clone(),
                rpc_provider.default_signer_address(),
            );
            
            // signature를 FixedBytes로 변환
            let fixed_sig = if sig.len() == 65 {
                FixedBytes::<65>::from_slice(&sig)
            } else {
                tracing::debug!("Invalid signature length: {}", sig.len());
                return None;
            };
            
            // Lock 트랜잭션 전송
            match market_service.lock_request(&req, fixed_sig, gas).await {
                Ok(block_num) => {
                    let latency = start.elapsed();
                    tracing::info!(
                        "✅ RPC[{}] {} locked 0x{:x} at block {} in {:?}",
                        index, url, req.id, block_num, latency
                    );
                    Some((index, url, block_num, latency))
                }
                Err(e) => {
                    let latency = start.elapsed();
                    
                    // AlreadyLocked는 다른 RPC가 이미 성공한 경우일 수 있음
                    if e.to_string().contains("RequestIsLocked") {
                        tracing::debug!(
                            "⚠️ RPC[{}] {} request already locked (another RPC won) in {:?}",
                            index, url, latency
                        );
                    } else {
                        tracing::debug!(
                            "❌ RPC[{}] {} failed: {} in {:?}",
                            index, url, e, latency
                        );
                    }
                    None
                }
            }
        })
    }).collect();

    // 모든 결과 수집
    let results = join_all(handles).await;
    
    // 성공한 결과 찾기 (가장 빠른 것)
    let mut fastest_result = None;
    let mut fastest_time = std::time::Duration::MAX;
    
    for result in results {
        if let Ok(Some((index, url, block_num, latency))) = result {
            if latency < fastest_time {
                fastest_time = latency;
                fastest_result = Some((index, url, block_num));
            }
        }
    }
    
    if let Some((index, url, block_num)) = fastest_result {
        tracing::info!(
            "🏆 LOCK SUCCESS! RPC[{}] {} won the race in {:?} - Block {}",
            index, url, fastest_time, block_num
        );
        Ok(block_num)
    } else {
        Err(anyhow::anyhow!("All {} RPCs failed to lock the request", 7))
    }
}

/// Lock 트랜잭션 직접 생성 (낮은 수준 제어용)
/// 현재 미사용 - 추후 필요시 구현
#[allow(dead_code)]
pub fn build_lock_transaction(
    _request: &ProofRequest,
    _signature: FixedBytes<65>,
    market_addr: Address,
    from: Address,
    nonce: U256,
    gas_price: U256,
) -> TransactionRequest {
    // lockRequest selector - 실제 구현 시 정확한 selector 필요
    let selector = [0x7b, 0x5e, 0x90, 0xe7];
    
    // ABI encode
    let mut data = Vec::new();
    data.extend_from_slice(&selector);
    
    // TODO: ProofRequest struct encoding
    // 실제 구현 시 복잡한 ABI encoding 로직 필요
    
    TransactionRequest::default()
        .from(from)
        .to(market_addr)
        .nonce(nonce.to::<u64>())
        .gas_limit(500000) // 적절한 gas limit
        .max_fee_per_gas(gas_price.to::<u128>())
        .max_priority_fee_per_gas(gas_price.to::<u128>() / 10)
        .input(Bytes::from(data).into())
}