from datetime import datetime, timezone
from typing import Literal
from uuid import UUID
from pydantic import BaseModel, Field

class PlaybackPulseEvent(BaseModel):
    session_id: UUID
    user_id: str = Field(..., min_length=3, pattern=r"^[a-zA-Z0-9]+$")
    content_id: str
    playback_position_sec: int = Field(..., ge=0)
    bitrate_kbps: int = Field(..., gt=0)
    buffer_duration_sec: float = Field(..., ge=0.0)
    event_type: Literal["heartbeat", "buffering_start", "buffering_end", "bitrate_change"]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
