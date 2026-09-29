# Project 2: High-Throughput Telemetry Ingestion Gateway

## Overview
A high-performance asynchronous telemetry ingestion gateway designed to handle `POST /v1/telemetry/pulse` events with sub-15ms response times. The architecture utilizes FastAPI, Redis Streams, and PostgreSQL.

## Running the Application
```bash
docker-compose up --build
```
API runs on `http://localhost:8000`

## Running Tests
```bash
pip install -r requirements.txt
pytest -v
```
