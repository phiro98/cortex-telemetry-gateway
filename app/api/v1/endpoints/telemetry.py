from fastapi import APIRouter, status
from pydantic import BaseModel
from uuid import UUID
from app.schemas.telemetry import PlaybackPulseEvent
from app.services.producer import publish_pulse_event

router = APIRouter()

class PulseResponse(BaseModel):
    status: str
    session_id: UUID

@router.post("/pulse", status_code=status.HTTP_202_ACCEPTED, response_model=PulseResponse)
async def ingest_pulse(event: PlaybackPulseEvent):
    await publish_pulse_event(event)
    return PulseResponse(status="queued", session_id=event.session_id)
