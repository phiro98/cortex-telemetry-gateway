from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, BigInteger, DateTime, UniqueConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class ContentHourlyMetric(Base):
    __tablename__ = "content_hourly_metrics"

    id = Column(Integer, primary_key=True, index=True)
    content_id = Column(String, index=True, nullable=False)
    hour_bucket = Column(DateTime, nullable=False)
    total_play_time_sec = Column(BigInteger, default=0, nullable=False)
    total_buffer_duration_sec = Column(Float, default=0.0, nullable=False)
    pulse_count = Column(Integer, default=0, nullable=False)

    __table_args__ = (
        UniqueConstraint('content_id', 'hour_bucket', name='uq_content_id_hour_bucket'),
    )
