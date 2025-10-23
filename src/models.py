from pydantic import BaseModel, Field, ConfigDict
from typing import Dict, Any, List, Optional

class Event(BaseModel):
    topic: str = Field(..., description="Event topic")
    event_id: str = Field(..., description="Unique event identifier")
    timestamp: str = Field(..., description="ISO8601 timestamp")
    source: str = Field(..., description="Event source")
    payload: Dict[str, Any] = Field(default_factory=dict)
    received_at: Optional[str] = Field(None, description="ISO8601 received time")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "topic": "user.created",
                "event_id": "evt-12345",
                "timestamp": "2025-10-21T14:30:00Z",
                "source": "auth-service",
                "payload": {"user_id": 123, "email": "test@example.com"}
            }
        }
    )

class EventBatch(BaseModel):
    events: List[Event]

class StatsResponse(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: List[str]
    uptime: float
