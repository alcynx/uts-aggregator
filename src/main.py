from fastapi import FastAPI, HTTPException, Query, Body
from typing import List, Optional, Union
import asyncio
import logging
import time
from datetime import datetime, timezone
from pydantic import ValidationError
from contextlib import asynccontextmanager

from .models import Event, EventBatch, StatsResponse
from .dedup_store import DedupStore

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AppState:
    def __init__(self):
        self.dedup_store = DedupStore()
        self.events_store: List[Event] = []
        self.stats = {
            "received": 0,
            "unique_processed": 0,
            "duplicate_dropped": 0,
        }
        self.start_time = time.time()
        self.lock = asyncio.Lock()
        self.event_queue = asyncio.Queue()


state = AppState()


async def event_consumer():
    logger.info("Consumer worker started")
    while True:
        try:
            event = await state.event_queue.get()
            
            is_dup = await state.dedup_store.is_duplicate(event.topic, event.event_id)
            if is_dup:
                async with state.lock:
                    state.stats["duplicate_dropped"] += 1
                logger.warning(
                    f"DUPLICATE DETECTED: topic={event.topic}, "
                    f"event_id={event.event_id}, source={event.source}"
                )
            else:
                try:
                    await state.dedup_store.mark_processed(
                        event.topic,
                        event.event_id,
                        event.timestamp
                    )
                    event.received_at = datetime.now(timezone.utc).isoformat()
                    
                    await state.dedup_store.save_event(event.model_dump())
                    
                    async with state.lock:
                        state.events_store.append(event)
                        state.stats["unique_processed"] += 1
                    
                    logger.info(
                        f"PROCESSED: topic={event.topic}, "
                        f"event_id={event.event_id}, source={event.source}"
                    )
                except Exception as e:
                    logger.error(f"Error processing event {event.event_id}: {e}")
                    async with state.lock:
                        state.stats["duplicate_dropped"] += 1
            
            state.event_queue.task_done()
        except Exception as e:
            logger.error(f"Consumer error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer_task = asyncio.create_task(event_consumer())
    
    await state.dedup_store.initialize()
    
    loaded_events = await state.dedup_store.load_events()
    state.events_store = [Event(**e) for e in loaded_events]
    
    db_stats = await state.dedup_store.get_stats()
    state.stats["unique_processed"] = db_stats["unique_processed"]
    
    logger.info(f"Application started successfully, loaded {len(loaded_events)} events from database")
    
    yield
    
    await state.event_queue.join()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        logger.info("Consumer worker stopped")


app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description="Tugas UTS Sistem Terdistribusi Pub-Sub Log Aggregator",
    version="1.0.0",
    lifespan=lifespan
)


@app.post("/publish")
async def publish_events(body: Union[Event, EventBatch] = Body(...)):
    try:
        if isinstance(body, Event):
            events = [body]
        elif isinstance(body, EventBatch):
            events = body.events
        else:
            raise HTTPException(status_code=400, detail="Invalid body payload")
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    for event in events:
        try:
            datetime.fromisoformat(event.timestamp.replace('Z', '+00:00'))
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid timestamp format for event {event.event_id}"
            )
    
    async with state.lock:
        state.stats["received"] += len(events)
    
    for event in events:
        await state.event_queue.put(event)
    
    return {
        "status": "accepted",
        "received": len(events),
        "queued": len(events)
    }


@app.get("/events")
async def get_events(topic: Optional[str] = Query(None, description="Filter by topic")):
    def sort_key(e):
        return e.received_at or e.timestamp

    if topic:
        filtered = [e for e in state.events_store if e.topic == topic]
        filtered = sorted(filtered, key=sort_key)
        return {
            "topic": topic,
            "count": len(filtered),
            "events": filtered
        }
    sorted_events = sorted(state.events_store, key=sort_key)
    return {
        "count": len(sorted_events),
        "events": sorted_events
    }


@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    topics = list(set([e.topic for e in state.events_store]))
    uptime = time.time() - state.start_time
    return StatsResponse(
        received=state.stats["received"],
        unique_processed=state.stats["unique_processed"],
        duplicate_dropped=state.stats["duplicate_dropped"],
        topics=topics,
        uptime=uptime
    )


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
