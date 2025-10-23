import pytest
import asyncio
import os
from fastapi.testclient import TestClient
from src.main import app, state
from src.models import Event
import time
import logging


@pytest.fixture(scope="module", autouse=True)
def setup_database():
    state.dedup_store.db_path = "test_dedup.db"
    state.events_store = []
    state.stats = {
        "received": 0,
        "unique_processed": 0,
        "duplicate_dropped": 0,
    }
    yield
    if os.path.exists("test_dedup.db"):
        os.remove("test_dedup.db")


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def wait_for_processing(seconds=1):
    time.sleep(seconds)


def test_publish_single_event(client):
    event = {
        "topic": "test.event",
        "event_id": "test-001",
        "timestamp": "2025-10-21T14:00:00Z",
        "source": "test-source",
        "payload": {"key": "value"}
    }
    response = client.post("/publish", json=event)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "accepted"
    assert data["queued"] == 1
    
    wait_for_processing(0.5)
    
    events = client.get("/events").json()
    assert events["count"] >= 1


def test_deduplication(client):
    event = {
        "topic": "test.event",
        "event_id": "dup-001",
        "timestamp": "2025-10-21T14:00:00Z",
        "source": "test-source",
        "payload": {"key": "value"}
    }
    
    response1 = client.post("/publish", json=event)
    assert response1.json()["status"] == "accepted"
    wait_for_processing(0.5)
    
    stats_before = client.get("/stats").json()
    unique_before = stats_before["unique_processed"]
    
    response2 = client.post("/publish", json=event)
    assert response2.json()["status"] == "accepted"
    wait_for_processing(0.5)
    
    stats_after = client.get("/stats").json()
    assert stats_after["unique_processed"] == unique_before
    assert stats_after["duplicate_dropped"] > stats_before["duplicate_dropped"]


def test_batch_events(client):
    batch = {
        "events": [
            {
                "topic": "batch.test",
                "event_id": f"batch-{i}",
                "timestamp": "2025-10-21T14:00:00Z",
                "source": "batch-source",
                "payload": {"index": i}
            }
            for i in range(10)
        ]
    }
    response = client.post("/publish", json=batch)
    assert response.status_code == 200
    assert response.json()["queued"] == 10
    
    wait_for_processing(1)
    
    stats = client.get("/stats").json()
    assert stats["unique_processed"] >= 10


def test_invalid_timestamp(client):
    event = {
        "topic": "test.event",
        "event_id": "invalid-ts",
        "timestamp": "not-a-valid-timestamp",
        "source": "test-source",
        "payload": {}
    }
    response = client.post("/publish", json=event)
    assert response.status_code == 400


def test_get_events_filter_by_topic(client):
    events = [
        {
            "topic": "topic-a",
            "event_id": f"a-filter-{i}",
            "timestamp": "2025-10-21T14:00:00Z",
            "source": "test",
            "payload": {}
        }
        for i in range(3)
    ]
    for event in events:
        client.post("/publish", json=event)
    
    wait_for_processing(1)
    
    response = client.get("/events?topic=topic-a")
    assert response.status_code == 200
    assert response.json()["count"] >= 3


def test_stats_endpoint(client):
    event = {
        "topic": "stats.test",
        "event_id": "stats-unique-001",
        "timestamp": "2025-10-21T14:00:00Z",
        "source": "test",
        "payload": {}
    }
    client.post("/publish", json=event)
    
    wait_for_processing(0.5)
    
    response = client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["received"] >= 1
    assert data["unique_processed"] >= 1
    assert "uptime" in data
    assert "topics" in data


def test_dedup_persistence(client):
    event = {
        "topic": "persist.test",
        "event_id": "persist-unique-001",
        "timestamp": "2025-10-21T14:00:00Z",
        "source": "test",
        "payload": {}
    }
    
    client.post("/publish", json=event)
    wait_for_processing(0.5)
    
    stats_before = client.get("/stats").json()
    unique_before = stats_before["unique_processed"]
    
    asyncio.run(state.dedup_store.initialize())
    
    client.post("/publish", json=event)
    wait_for_processing(0.5)
    
    stats_after = client.get("/stats").json()
    assert stats_after["unique_processed"] == unique_before


def test_stress_batch(client):
    logging.disable(logging.WARNING)
    
    batch_size = 1000
    duplicate_ratio = 0.2
    events = []
    for i in range(batch_size):
        event_id = f"stress-unique-{i % int(batch_size * (1 - duplicate_ratio))}"
        events.append({
            "topic": "stress.test",
            "event_id": event_id,
            "timestamp": "2025-10-21T14:00:00Z",
            "source": "stress-test",
            "payload": {"index": i}
        })
    
    start = time.time()
    response = client.post("/publish", json={"events": events})
    duration = time.time() - start
    
    logging.disable(logging.NOTSET)
    
    assert response.status_code == 200
    assert duration < 20
    data = response.json()
    assert data["received"] == batch_size
    
    wait_for_processing(3)
    
    stats = client.get("/stats").json()
    assert stats["duplicate_dropped"] > 0


def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
