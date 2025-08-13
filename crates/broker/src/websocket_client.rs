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

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info, warn};

type WebSocketSender = futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, Message>;
type WebSocketReceiver = futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>;

const WEBSOCKET_URL: &str = "wss://lb.drpc.org/base/ApklQs6JokXxjviH4hmf1eWmN3aZdDIR8IfXIgaNGuYu";
const RESPONSE_TIMEOUT_SECS: u64 = 5;
const RECONNECT_DELAY_MS: u64 = 1000;

#[derive(Debug)]
pub struct WebSocketClient {
    sender: Option<WebSocketSender>,
    request_id: AtomicU64,
    response_waiters: Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>,
    is_connected: bool,
}

impl WebSocketClient {
    pub async fn new() -> Result<Self> {
        let mut client = Self {
            sender: None,
            request_id: AtomicU64::new(1),
            response_waiters: Arc::new(Mutex::new(HashMap::new())),
            is_connected: false,
        };
        
        client.connect().await?;
        Ok(client)
    }

    pub fn new_disconnected() -> Self {
        Self {
            sender: None,
            request_id: AtomicU64::new(1),
            response_waiters: Arc::new(Mutex::new(HashMap::new())),
            is_connected: false,
        }
    }

    async fn connect(&mut self) -> Result<()> {
        debug!("🔌 WebSocket: Connecting to {}", WEBSOCKET_URL);
        
        // String을 직접 사용 (tokio-tungstenite는 &str을 받음)
        let (ws_stream, _) = connect_async(WEBSOCKET_URL).await
            .map_err(|e| anyhow!("WebSocket connection failed: {}", e))?;
        
        let (sender, receiver) = ws_stream.split();
        self.sender = Some(sender);
        self.is_connected = true;
        
        // 백그라운드에서 응답 처리
        let waiters = Arc::clone(&self.response_waiters);
        tokio::spawn(Self::handle_responses(receiver, waiters));
        
        info!("✅ WebSocket: Connected successfully to {}", WEBSOCKET_URL);
        Ok(())
    }

    async fn handle_responses(
        mut receiver: WebSocketReceiver,
        response_waiters: Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>,
    ) {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(response) = serde_json::from_str::<Value>(&text) {
                        if let Some(id) = response.get("id").and_then(|v| v.as_u64()) {
                            let mut waiters = response_waiters.lock().await;
                            if let Some(sender) = waiters.remove(&id) {
                                if let Err(_) = sender.send(response) {
                                    warn!("📞 WebSocket: Failed to send response to waiter for ID {}", id);
                                }
                            }
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    warn!("🔌 WebSocket: Connection closed by server");
                    break;
                }
                Err(e) => {
                    error!("⚠️ WebSocket: Error receiving message: {}", e);
                    break;
                }
                _ => {}
            }
        }
        
        // 연결이 끊어지면 대기 중인 모든 요청을 취소
        let mut waiters = response_waiters.lock().await;
        waiters.clear();
    }

    async fn ensure_connected(&mut self) -> Result<()> {
        if !self.is_connected || self.sender.is_none() {
            warn!("🔄 WebSocket: Reconnecting...");
            
            // 재연결 시도
            for attempt in 1..=3 {
                tokio::time::sleep(Duration::from_millis(RECONNECT_DELAY_MS)).await;
                
                match self.connect().await {
                    Ok(()) => {
                        info!("✅ WebSocket: Reconnected on attempt {}", attempt);
                        return Ok(());
                    }
                    Err(e) => {
                        warn!("❌ WebSocket: Reconnection attempt {} failed: {}", attempt, e);
                    }
                }
            }
            
            Err(anyhow!("WebSocket reconnection failed after 3 attempts"))
        } else {
            Ok(())
        }
    }

    pub async fn send_raw_transaction(&mut self, raw_tx: &str) -> Result<String> {
        // lazy loading으로 연결 시도
        if !self.is_connected {
            if let Err(e) = self.connect().await {
                return Err(anyhow!("Failed to establish WebSocket connection: {}", e));
            }
        } else {
            self.ensure_connected().await?;
        }
        
        let req_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        
        let request = json!({
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": [raw_tx],
            "id": req_id
        });

        debug!("🚀 WebSocket: Sending transaction with ID {}", req_id);
        
        // 응답 대기 채널 생성
        let (tx, rx) = oneshot::channel();
        self.response_waiters.lock().await.insert(req_id, tx);

        // WebSocket 메시지 전송
        if let Some(sender) = &mut self.sender {
            sender.send(Message::Text(request.to_string())).await
                .map_err(|e| {
                    self.is_connected = false;
                    anyhow!("WebSocket send failed: {}", e)
                })?;
        } else {
            return Err(anyhow!("WebSocket sender not available"));
        }

        // 응답 대기 (타임아웃 5초)
        let response = tokio::time::timeout(Duration::from_secs(RESPONSE_TIMEOUT_SECS), rx)
            .await
            .map_err(|_| anyhow!("WebSocket response timeout"))?
            .map_err(|_| anyhow!("WebSocket response channel closed"))?;

        // 에러 응답 처리
        if let Some(error) = response.get("error") {
            return Err(anyhow!("RPC error: {}", error));
        }

        // 트랜잭션 해시 추출
        let tx_hash = response.get("result")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Invalid response format: missing result field"))?;

        debug!("✅ WebSocket: Transaction sent successfully: {}", tx_hash);
        Ok(tx_hash.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_websocket_client_creation() {
        // 실제 연결 테스트는 네트워크 의존성 때문에 스킵
        // 로컬 테스트에서는 mock server를 사용해야 함
    }
}