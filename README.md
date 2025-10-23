# Event Aggregator - Pub-Sub Log Aggregator

Sistem Event Aggregator dengan dukungan deduplication, idempotency, dan persistence untuk UTS Sistem Terdistribusi.

## 📋 Deskripsi

Event Aggregator adalah sistem yang menerima event dari publisher, melakukan deduplication berdasarkan `(topic, event_id)`, dan menyimpan event unik ke database persisten. Sistem ini dirancang dengan pola Producer-Consumer menggunakan async queue untuk processing yang efisien.

### Fitur Utama:
- **Deduplication:** Event duplikat otomatis ditolak
- **Idempotency:** Event yang sama tidak diproses lebih dari sekali
- **Persistence:** Data tersimpan di SQLite dengan Docker volume
- **Async Processing:** Background consumer worker dengan asyncio queue
- **At-Least-Once Delivery:** Simulasi event duplikat untuk testing resilience

---

## 🏗️ Arsitektur

Publisher → Aggregator API → Async Queue → Consumer Worker → SQLite DB
↓
Dedup Store

### Komponen:
1. **Aggregator Service (FastAPI):** API untuk menerima event dan menyediakan query
2. **Publisher Service:** Simulator untuk mengirim batch event (80% unik + 20% duplikat)
3. **Async Queue (in-memory):** Buffer untuk decoupling API dan processing
4. **Consumer Worker:** Background task untuk dedup dan save ke DB
5. **SQLite Database:** Persistent storage dengan volume Docker

---

## 🚀 Cara Build & Run

### Prerequisites:
- Docker & Docker Compose
- Python 3.11+ (untuk development lokal)

### 1. Build & Run Semua Service:
docker compose up --build

### 2. Run Aggregator Saja (untuk demo kondisi awal kosong):
docker compose up --build aggregator

### 3. Run Publisher Manual (setelah aggregator ready):
docker compose run publisher

### 4. Stop Service:
docker compose down

### 5. Restart Aggregator (untuk demo persistence):
docker compose restart aggregator

---

## 🧪 Testing

### Jalankan Unit Test:
Install dependencies
pip install -r requirements.txt

Jalankan pytest
pytest

### Expected Output:
========== 9 passed in ~15s ==========

---

## 📡 API Endpoints

### Base URL: `http://localhost:8080`

### 1. POST /publish
Menerima event (single atau batch) untuk diproses.

**Request Body (Single Event):**
{
"topic": "user.created",
"event_id": "evt-12345",
"timestamp": "2025-10-21T14:30:00Z",
"source": "auth-service",
"payload": {
"user_id": 123,
"email": "test@example.com"
}
}

**Request Body (Batch):**
{
"events": [
{
"topic": "order.placed",
"event_id": "evt-001",
"timestamp": "2025-10-21T14:30:00Z",
"source": "order-service",
"payload": {"order_id": 1}
},
{
"topic": "order.placed",
"event_id": "evt-002",
"timestamp": "2025-10-21T14:31:00Z",
"source": "order-service",
"payload": {"order_id": 2}
}
]
}


**Response:**
{
"status": "accepted",
"received": 2,
"queued": 2
}

**Example:**
curl -X POST http://localhost:8080/publish
-H "Content-Type: application/json"
-d '{"topic":"test","event_id":"evt-1","timestamp":"2025-10-21T14:30:00Z","source":"test","payload":{}}'

---

### 2. GET /events
Mengambil semua event yang sudah diproses.

**Query Parameters:**
- `topic` (optional): Filter event berdasarkan topic

**Response:**
{
"count": 100,
"events": [
{
"topic": "user.created",
"event_id": "evt-12345",
"timestamp": "2025-10-21T14:30:00Z",
"source": "auth-service",
"payload": {"user_id": 123},
"received_at": "2025-10-21T14:30:01.123456Z"
}
]
}

**Example:**
Get all events
curl http://localhost:8080/events

Get events by topic
curl http://localhost:8080/events?topic=user.created

---

### 3. GET /stats
Mengambil statistik processing event.

**Response:**
{
"received": 5000,
"unique_processed": 4000,
"duplicate_dropped": 1000,
"topics": ["user.created", "order.placed", "payment.processed"],
"uptime": 123.45
}

**Example:**
curl http://localhost:8080/stats

---

### 4. GET /health
Health check endpoint.

**Response:**
{
"status": "healthy"
}

**Example:**
curl http://localhost:8080/health

---

## 🔍 Asumsi & Design Decision

### Asumsi:
1. **Event ID Unik per Topic:** Kombinasi `(topic, event_id)` dijamin unik oleh publisher
2. **Timestamp Format:** ISO8601 dengan timezone (contoh: `2025-10-21T14:30:00Z`)
3. **Single Instance:** Aggregator berjalan sebagai single instance (tidak distributed)
4. **In-Memory Queue:** Queue menggunakan asyncio.Queue (in-memory), event bisa hilang jika container crash sebelum diproses
5. **Publisher Behavior:** Publisher mengirim 5000 event (4000 unik + 1000 duplikat) untuk simulasi at-least-once delivery

### Design Decisions:
- **SQLite:** Dipilih untuk simplicity, cocok untuk demo dan small-scale deployment
- **Dual Storage:** Data disimpan di memory (fast read) dan DB (persistence)
- **Async Queue:** Decoupling API response dari processing untuk throughput lebih baik
- **Single Consumer Worker:** Cukup untuk demo scale, bisa di-scale dengan multiple workers untuk production
- **Volume Mounting:** Database di-mount ke `./data` agar data persisten setelah restart

---

## 📂 Struktur Project

uts-aggregator/
├── src/
│ ├── init.py
│ ├── main.py # FastAPI app + queue + consumer
│ ├── models.py # Pydantic models
│ └── dedup_store.py # SQLite dedup & event storage
├── tests/
│ └── test_aggregator.py # Unit tests
├── data/ # SQLite database (volume-mounted)
│ └── dedup_store.db
├── publisher.py # Event publisher simulator
├── Dockerfile # Aggregator container
├── Dockerfile.publisher # Publisher container
├── docker-compose.yml # Service orchestration
├── requirements.txt # Python dependencies
├── pytest.ini # Pytest configuration
└── README.md # This file

---

## 📊 Demo Scenario

### 1. Kondisi Awal (Aggregator Kosong):
docker compose up aggregator

Browser: http://localhost:8080/docs
Check /events → count: 0
Check /stats → unique_processed: 0

### 2. Kirim Event Batch (5000 events):
docker compose run publisher

Wait ~10-15 detik untuk processing

### 3. Verifikasi Dedup & Stats:
Browser: http://localhost:8080/docs
Check /stats:
- received: 5000
- unique_processed: 4000
- duplicate_dropped: 1000

### 4. Restart Container (Demo Persistence):
docker compose restart aggregator

Check /stats → unique_processed: 4000 (data yang berhasil masuk sebelumnya akan tetap ada)

Run publisher lagi → semua event yang duplikat akan didrop


---

## 🛠️ Development

### Setup Local Environment:
Create virtual environment
python -m venv venv

Activate (Windows)
venv\Scripts\activate

Activate (Linux/Mac)
source venv/bin/activate

Install dependencies
pip install -r requirements.txt

Run aggregator locally
uvicorn src.main:app --reload --port 8080

---