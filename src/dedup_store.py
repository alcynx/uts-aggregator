import aiosqlite
import asyncio
import logging
import json

logger = logging.getLogger(__name__)


class DedupStore:
    def __init__(self, db_path: str = "data/dedup_store.db"):
        self.db_path = db_path
        self.lock = asyncio.Lock()

    async def initialize(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS dedup_store (
                    topic TEXT,
                    event_id TEXT,
                    timestamp TEXT,
                    PRIMARY KEY (topic, event_id)
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    topic TEXT,
                    event_id TEXT,
                    timestamp TEXT,
                    source TEXT,
                    payload TEXT,
                    received_at TEXT,
                    PRIMARY KEY (topic, event_id)
                )
                """
            )
            await db.commit()
        logger.info(f"DedupStore initialized at {self.db_path}")

    async def is_duplicate(self, topic: str, event_id: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT 1 FROM dedup_store WHERE topic = ? AND event_id = ?",
                (topic, event_id)
            )
            result = await cursor.fetchone()
            return result is not None

    async def mark_processed(self, topic: str, event_id: str, timestamp: str):
        async with self.lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "INSERT OR IGNORE INTO dedup_store (topic, event_id, timestamp) VALUES (?, ?, ?)",
                    (topic, event_id, timestamp)
                )
                await db.commit()

    async def save_event(self, event: dict):
        async with self.lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """
                    INSERT OR REPLACE INTO events (topic, event_id, timestamp, source, payload, received_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event["topic"],
                        event["event_id"],
                        event["timestamp"],
                        event["source"],
                        json.dumps(event["payload"]),
                        event.get("received_at")
                    )
                )
                await db.commit()

    async def load_events(self) -> list:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT topic, event_id, timestamp, source, payload, received_at FROM events ORDER BY received_at")
            rows = await cursor.fetchall()
            events = []
            for row in rows:
                events.append({
                    "topic": row[0],
                    "event_id": row[1],
                    "timestamp": row[2],
                    "source": row[3],
                    "payload": json.loads(row[4]),
                    "received_at": row[5]
                })
            return events

    async def get_stats(self) -> dict:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM events")
            unique_count = (await cursor.fetchone())[0]
            
            cursor = await db.execute("SELECT COUNT(*) FROM dedup_store")
            total_processed = (await cursor.fetchone())[0]
            
            return {
                "unique_processed": unique_count,
                "total_processed": total_processed
            }
