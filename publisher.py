import httpx
import asyncio
import random
import time
from datetime import datetime, timezone


AGGREGATOR_URL = "http://aggregator:8080" 


async def wait_for_aggregator():
    async with httpx.AsyncClient() as client:
        for _ in range(30): 
            try:
                resp = await client.get(f"{AGGREGATOR_URL}/health")
                if resp.status_code == 200:
                    print("Aggregator is ready!")
                    return
            except Exception:
                pass
            print("Waiting for aggregator...")
            time.sleep(1)
        raise RuntimeError("Aggregator not available after waiting.")
    
async def publish_events():
    async with httpx.AsyncClient() as client:
        total_events = 5000
        unique_count = int(total_events * 0.8)
        duplicate_count = total_events - unique_count 

        unique_events = []
        for i in range(unique_count):
            event = {
                "topic": random.choice(["user.created", "order.placed", "payment.processed"]),
                "event_id": f"evt-{i}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "publisher-service",
                "payload": {"data": f"event-{i}"}
            }
            unique_events.append(event)

        dupe_sources = random.sample(unique_events, k=duplicate_count)
        duplicate_events = []
        for e in dupe_sources:
            dupe = dict(e)
            dupe["timestamp"] = datetime.now(timezone.utc).isoformat()
            duplicate_events.append(dupe)

        all_events = unique_events + duplicate_events
        random.shuffle(all_events)

        for event in all_events:
            await client.post(f"{AGGREGATOR_URL}/publish", json=event)
            await asyncio.sleep(0.001)


if __name__ == "__main__":
    async def main():
        await wait_for_aggregator()
        await publish_events()
    asyncio.run(main())
