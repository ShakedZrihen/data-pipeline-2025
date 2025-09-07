# Data Pipeline 2025 – Final Project

## Overview
This project implements a simplified **data ingestion and processing pipeline** for supermarket price lists.  
The pipeline is containerized with Docker Compose and runs locally with LocalStack (S3/SQS simulation).  

Main stages:
1. **Extractor (Crawlers)** – downloads raw files (PDF/Excel) from provider websites.  
2. **Enricher (Stub)** – placeholder service to keep the pipeline consistent.  
3. **API (FastAPI)** – exposes endpoints for querying supermarket/product data.  

Providers currently supported:
- **Super-Pharm**
- **Yohananof**

---

## Requirements
- Docker Desktop (Linux containers mode)  
- Docker Compose v2+  
  

---

## Setup & Run

1. **Clone the repo**:
   ```bash
   git clone <your-fork-url>
   cd data-pipeline-2025/salim

2. **Start the pipeline:**
docker compose build
docker compose up

This will start all services: LocalStack, Extractor, Enricher (stub), API, and Postgres (if configured).

Check services:
LocalStack UI: http://localhost:8080
FastAPI docs: http://localhost:8000/docs

**Project Structure**
salim/
  ├── crawler/             # Crawlers per provider (Yohananof, SuperPharm, etc.)
  ├── enricher/            # Stub enricher (keeps pipeline consistent)
  ├── api/                 # FastAPI service
  ├── extractor/ 
  ├── uploader/            # upload to S3 logic
  ├── docker-compose.yml   # Orchestrates all services
  ├── init-s3.sh
  ├── requirments.txt
  ├── Dockerfile           # Base image config
  └── README.md            # This file

**Notes & Assumptions**

Some stages (like the Enricher) are provided as stubs to keep the pipeline running end-to-end.
Providers differ in crawling logic:
Yohananof → Cerberus-based file manager (dynamic pages, Selenium).
Super-Pharm → Direct file links.
API not tested

**How to Verify**

Run docker compose up.
Ensure crawler logs show files being downloaded into salim/downloads/<provider>.
Visit http://localhost:8000/docs

