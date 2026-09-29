import pytest
import pytest_asyncio
import asyncio
import fakeredis.aioredis
from typing import AsyncGenerator
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.main import app
from app.api.deps import get_db
from app.models.metrics import Base

test_engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
TestSessionLocal = sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)

async def override_get_db() -> AsyncGenerator:
    async with TestSessionLocal() as session:
        yield session

app.dependency_overrides[get_db] = override_get_db



@pytest_asyncio.fixture(autouse=True)
async def setup_db():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest.fixture
def fake_redis():
    server = fakeredis.FakeServer()
    return fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)

@pytest.fixture(autouse=True)
def patch_redis_client(monkeypatch, fake_redis):
    def mock_get_redis_client():
        return fake_redis
    monkeypatch.setattr("app.services.producer.get_redis_client", mock_get_redis_client)
    monkeypatch.setattr("app.services.consumer.get_redis_client", mock_get_redis_client)

@pytest_asyncio.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac

@pytest.fixture
def db_session_maker():
    return TestSessionLocal
