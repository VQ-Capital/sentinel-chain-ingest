// ========== DOSYA: sentinel-chain-ingest/src/main.rs ==========
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use prost::Message;
use serde_json::Value;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};
use tracing::{error, info, warn};

pub mod sentinel_market {
    include!(concat!(env!("OUT_DIR"), "/sentinel.market.v1.rs"));
}
use sentinel_market::ChainUrgencyEvent;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("🔗 VQ-Chain-Ingest v0.1.1: On-Chain Mempool Radar Başlatılıyor...");

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    // ARCHITECTURAL FIX: Dinamik Sembol Okuma (Hardcoded kaldırıldı)
    let target_symbol = std::env::var("CHAIN_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_string());

    let nats_client = async_nats::connect(&nats_url).await.context("NATS Error")?;

    let mempool_ws = "wss://mempool.space/api/v1/ws";

    loop {
        info!(
            "📡 Mempool.space WebSocket'e bağlanılıyor... (Hedef Sembol: {})",
            target_symbol
        );
        match connect_async(mempool_ws).await {
            Ok((ws_stream, _)) => {
                info!("✅ [ON-CHAIN] Mempool Havuzuna Girildi! Zincir izleniyor.");
                let (mut write, mut read) = ws_stream.split();

                let init_msg =
                    serde_json::json!({"action": "want", "data": ["blocks", "mempool-blocks"]});
                let _ = write.send(WsMessage::Text(init_msg.to_string())).await;

                while let Some(message) = read.next().await {
                    if let Ok(WsMessage::Text(text)) = message {
                        if let Ok(json) = serde_json::from_str::<Value>(&text) {
                            if let Some(mempool) = json.get("mempoolInfo") {
                                let total_txs =
                                    mempool.get("size").and_then(|v| v.as_f64()).unwrap_or(0.0);

                                let mut urgency = total_txs / 100_000.0;
                                urgency = urgency.clamp(0.0, 1.0);

                                let event = ChainUrgencyEvent {
                                    symbol: target_symbol.clone(),
                                    urgency_score: urgency,
                                    network_fee: total_txs,
                                    timestamp: chrono::Utc::now().timestamp_millis(),
                                };

                                let mut buf = Vec::new();
                                if event.encode(&mut buf).is_ok() {
                                    let _ = nats_client
                                        .publish(
                                            format!("chain.urgency.{}", target_symbol),
                                            buf.into(),
                                        )
                                        .await;
                                }

                                if urgency > 0.7 {
                                    warn!("🔥 [CHAIN-ALERT] Ağ Tıkanıklığı Yüksek! Bekleyen TX: {} | Aciliyet: {:.2}", total_txs, urgency);
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => error!("❌ Mempool Bağlantısı Koptu: {:?}", e),
        }
        warn!("⚠️ 5 saniye sonra Mempool'a tekrar bağlanılacak...");
        sleep(Duration::from_secs(5)).await;
    }
}
