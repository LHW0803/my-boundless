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

use alloy::{
    primitives::utils::parse_ether,
    providers::{fillers::ChainIdFiller, network::EthereumWallet, ProviderBuilder, WalletProvider},
    rpc::client::RpcClient,
    transports::layers::RetryBackoffLayer,
};
use anyhow::{Context, Result};
use boundless_market::{
    balance_alerts_layer::{BalanceAlertConfig, BalanceAlertLayer},
    contracts::boundless_market::BoundlessMarketService,
    dynamic_gas_filler::DynamicGasFiller,
    nonce_layer::NonceProvider,
};
use broker::{Args, Broker, Config, CustomRetryPolicy};
use clap::Parser;
use tracing_subscriber::fmt::format::FmtSpan;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::load(&args.config_file).await?;

    if args.log_json {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_span_events(FmtSpan::CLOSE)
            .json()
            .with_ansi(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_span_events(FmtSpan::CLOSE)
            .init();
    }

    let wallet = EthereumWallet::from(args.private_key.clone());

    // 병렬 RPC 활성화 확인 (환경변수 또는 기본값)
    let use_parallel_rpc = env::var("USE_PARALLEL_RPC")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()
        .unwrap_or(true);
    
    if use_parallel_rpc {
        tracing::info!("🚀 Parallel RPC enabled - using 7 RPCs for fastest response");
    }

    let retry_layer = RetryBackoffLayer::new_with_policy(
        args.rpc_retry_max,
        args.rpc_retry_backoff,
        args.rpc_retry_cu,
        CustomRetryPolicy,
    );
    
    // Primary RPC 설정 (병렬 RPC가 활성화되어도 fallback으로 사용)
    let client = RpcClient::builder().layer(retry_layer).http(args.rpc_url.clone());
    let balance_alerts_layer = BalanceAlertLayer::new(BalanceAlertConfig {
        watch_address: wallet.default_signer().address(),
        warn_threshold: config
            .market
            .balance_warn_threshold
            .map(|s| parse_ether(&s))
            .transpose()?,
        error_threshold: config
            .market
            .balance_error_threshold
            .map(|s| parse_ether(&s))
            .transpose()?,
    });

    let dynamic_gas_filler = DynamicGasFiller::new(
        0.2,  // 20% increase of gas limit
        0.05, // 5% increase of gas_price per pending transaction
        2.0,  // 2x max gas multiplier
        wallet.default_signer().address(),
    );

    let base_provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .filler(ChainIdFiller::default())
        .filler(dynamic_gas_filler)
        .layer(balance_alerts_layer)
        .connect_client(client);

    let provider = NonceProvider::new(base_provider, wallet.clone());
    let broker = Broker::new(args.clone(), provider.clone()).await?;

    // TODO: Move this code somewhere else / monitor our balanceOf and top it up as needed
    if let Some(deposit_amount) = args.deposit_amount.as_ref() {
        let boundless_market = BoundlessMarketService::new(
            broker.deployment().boundless_market_address,
            provider.clone(),
            provider.default_signer_address(),
        );

        tracing::info!("pre-depositing {deposit_amount} stake tokens into the market contract");
        boundless_market
            .deposit_stake_with_permit(*deposit_amount, &args.private_key)
            .await
            .context("Failed to deposit to market")?;
    }

    // Await broker shutdown before returning from main
    broker.start_service().await.context("Broker service failed")?;

    Ok(())
}
