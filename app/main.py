from fastapi import FastAPI
from app.api.v1.endpoints import telemetry
from app.core.config import settings

app = FastAPI(title=settings.PROJECT_NAME)

app.include_router(telemetry.router, prefix=f"{settings.API_V1_STR}/telemetry", tags=["telemetry"])

@app.get("/health")
async def health_check():
    return {"status": "ok"}
