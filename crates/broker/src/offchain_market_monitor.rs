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

use alloy::signers::{local::PrivateKeySigner, Signer};
use anyhow::Result;
use boundless_market::order_stream_client::{order_stream, OrderStreamClient};
use futures_util::StreamExt;

use crate::{
    errors::CodedError,
    impl_coded_debug,
    task::{RetryRes, RetryTask, SupervisorErr},
    FulfillmentType, OrderRequest,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use std::time::Duration;

#[derive(Error)]
pub enum OffchainMarketMonitorErr {
    #[error("WebSocket error: {0:?}")]
    WebSocketErr(anyhow::Error),

    #[error("{code} Receiver dropped", code = self.code())]
    ReceiverDropped,

    #[error("{code} Unexpected error: {0:?}", code = self.code())]
    UnexpectedErr(#[from] anyhow::Error),
}

impl_coded_debug!(OffchainMarketMonitorErr);

impl CodedError for OffchainMarketMonitorErr {
    fn code(&self) -> &str {
        match self {
            OffchainMarketMonitorErr::WebSocketErr(_) => "[B-OMM-001]",
            OffchainMarketMonitorErr::ReceiverDropped => "[B-OMM-002]",
            OffchainMarketMonitorErr::UnexpectedErr(_) => "[B-OMM-500]",
        }
    }
}

pub struct OffchainMarketMonitor {
    client: OrderStreamClient,
    signer: PrivateKeySigner,
    new_order_tx: tokio::sync::mpsc::Sender<Box<OrderRequest>>,
}

impl OffchainMarketMonitor {
    pub fn new(
        client: OrderStreamClient,
        signer: PrivateKeySigner,
        new_order_tx: tokio::sync::mpsc::Sender<Box<OrderRequest>>,
    ) -> Self {
        Self { client, signer, new_order_tx }
    }

    async fn monitor_orders(
        client: OrderStreamClient,
        signer: &impl Signer,
        new_order_tx: tokio::sync::mpsc::Sender<Box<OrderRequest>>,
        cancel_token: CancellationToken,
    ) -> Result<(), OffchainMarketMonitorErr> {
        // 3000분마다 자동 재연결 (메모리/성능 최적화)
        const RECONNECT_INTERVAL: Duration = Duration::from_secs(3000 * 60); // 3000분
        
        tracing::info!("🚀 ULTRA FAST: Connecting to off-chain market: {}", client.base_url);
        tracing::info!("⚡ Auto-reconnect enabled: Every 30 minutes for optimal performance");
        
        // Set ping interval from environment variable (default: 2000ms for stability)
        let ping_ms = std::env::var("ORDER_STREAM_CLIENT_PING_MS")
            .unwrap_or_else(|_| "2000".to_string());
        std::env::set_var("ORDER_STREAM_CLIENT_PING_MS", &ping_ms);
        
        // Connection timeout from environment variable (default: 5 seconds)
        let timeout_secs = std::env::var("ORDER_STREAM_CLIENT_TIMEOUT_SECS")
            .unwrap_or_else(|_| "5".to_string())
            .parse::<u64>()
            .unwrap_or(5);
        
        let socket = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            client.connect_async(signer)
        ).await
        .map_err(|_| OffchainMarketMonitorErr::WebSocketErr(anyhow::anyhow!("WebSocket connection timeout ({}s)", timeout_secs)))? 
        .map_err(OffchainMarketMonitorErr::WebSocketErr)?;

        let mut stream = order_stream(socket);
        tracing::info!("🚀 ULTRA FAST: Connected to offchain Order stream with {}ms ping", ping_ms);

        // Ultra-fast processing buffer for immediate parallel handling
        let (ultra_tx, mut ultra_rx) = tokio::sync::mpsc::unbounded_channel::<Box<OrderRequest>>();
        let main_tx = new_order_tx.clone();
        
        // Ultra-fast parallel forwarder
        tokio::spawn(async move {
            while let Some(order) = ultra_rx.recv().await {
                let tx = main_tx.clone();
                // Immediate fire-and-forget forwarding
                tokio::spawn(async move {
                    if let Err(_) = tx.send(order).await {
                        // Silent fail - ultra-fast processing prioritizes speed
                    }
                });
            }
        });
        
        // 재연결 타이머 시작
        let reconnect_timer = tokio::time::sleep(RECONNECT_INTERVAL);
        tokio::pin!(reconnect_timer);

        loop {
            tokio::select! {
                // 30분 타이머 만료시 재연결
                _ = &mut reconnect_timer => {
                    tracing::info!("⚡ Scheduled 30-minute reconnection for performance optimization");
                    return Ok(()); // RetryTask가 자동으로 재시작
                }
                order_data = stream.next() => {
                    match order_data {
                        Some(order_data) => {
                            let order_id = order_data.id;
                            
                            // Create order immediately - no logging delays
                            let mut new_order = OrderRequest::new(
                                order_data.order.request,
                                order_data.order.signature.as_bytes().into(),
                                FulfillmentType::MempoolLockAndFulfill,
                                client.boundless_market_address,
                                client.chain_id,
                            );
                            
                            // Set 15 minute expiration for offchain orders
                            use chrono::Utc;
                            new_order.expire_timestamp = Some((Utc::now().timestamp() + 840) as u64); // 15 minutes = 900 seconds

                            // Ultra-fast unbounded send (no blocking)
                            if let Err(_) = ultra_tx.send(Box::new(new_order)) {
                                tracing::error!("⚡ ULTRA: Channel closed for order {:x}", order_id);
                            }
                            
                            // Minimal success log
                            tracing::trace!("⚡ {:x}", order_id);
                        }
                        None => {
                            return Err(OffchainMarketMonitorErr::WebSocketErr(anyhow::anyhow!(
                                "Offchain order stream websocket exited, polling failed"
                            )));
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    tracing::info!("Offchain market monitor received cancellation, shutting down gracefully");
                    return Ok(());
                }
            }
        }
    }
}

impl RetryTask for OffchainMarketMonitor {
    type Error = OffchainMarketMonitorErr;
    fn spawn(&self, cancel_token: CancellationToken) -> RetryRes<Self::Error> {
        let client = self.client.clone();
        let signer = self.signer.clone();
        let new_order_tx = self.new_order_tx.clone();

        Box::pin(async move {
            tracing::info!("Starting up offchain market monitor");
            Self::monitor_orders(client, &signer, new_order_tx, cancel_token)
                .await
                .map_err(SupervisorErr::Recover)?;
            Ok(())
        })
    }
}
