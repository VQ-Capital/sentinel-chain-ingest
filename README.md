# ⛓️ sentinel-onchain-ingest (Legacy: sentinel-chain-ingest)

**Domain:** Mempool & Pre-Trade Signal Ingestion
**Rol:** Sistemin Erken Uyarı Radarı

Bu servis, borsaların fiyatları daha tepki vermeden önce blockchain ağının bizzat kendi içindeki (Mempool) hareketliliği okur. Tıkanıklık ve işlem ücreti anomalilerini ölçerek Quant motoru için 4. Boyut olan "Chain Urgency" verisini üretir.

- **NATS Çıktısı:** `chain.urgency.*`
- **SLA Hedefi:** < 100ms