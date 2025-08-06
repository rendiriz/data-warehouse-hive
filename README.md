# Data Warehouse Hive Project

## Overview

This project is a modern data warehouse solution that enables users to upload CSV files via a web interface, process and validate them, and create external tables in Apache Hive backed by S3-compatible object storage (e.g., MinIO). The system is built as a monorepo with a TypeScript/Next.js frontend, a Python/Sanic backend, and a Docker-based orchestration for Hive, Postgres, and supporting services.

---

## Architecture

- **Frontend**: Next.js (TypeScript), TailwindCSS, shadcn/ui, file upload via tus protocol to S3/MinIO, communicates with backend for CSV processing and Hive table creation.
- **Backend**: Python (Sanic), processes CSVs from S3, infers schema, validates with Pandera, manages Hive tables via PyHive, exposes REST API for frontend.
- **Data Layer**: Apache Hive (metastore + server), Postgres (metastore DB), S3-compatible storage (MinIO).
- **Orchestration**: Docker Compose for local development and service management.

---

## Features

- Upload CSV files via web UI (with resumable uploads)
- Automatic schema inference and validation
- External Hive table creation pointing to S3/MinIO
- Table management (list, info, drop)
- Health checks and robust error handling

---

## Project Structure

```
├── backend/    # Python Sanic API for CSV processing and Hive management
├── frontend/   # Next.js web app (file upload, table management UI)
├── docker/     # Docker Compose, Hive, Postgres, MinIO configs
```

---

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Node.js (v18+ recommended)
- pnpm (for frontend)
- Python 3.10+ (for backend, if running outside Docker)

### 1. Clone the repository
```bash
git clone <repo-url>
cd data-warehouse-hive
```

### 2. Environment Variables
- Copy and configure `.env` files for backend, frontend, and Docker as needed (see `backend/config.py` and Docker Compose for required variables: AWS/MinIO, Postgres, etc).

### 3. Start All Services (Recommended)
```bash
cd docker
docker-compose up --build
```
- This will start Hive, Postgres, MinIO, and other dependencies.

### 4. Start Backend (API)
```bash
cd backend
uv run sanic app:app --host=0.0.0.0 --port=8000 --debug --auto-reload
```

### 5. Start Frontend (Web UI)
```bash
cd frontend
pnpm install
pnpm dev
```
- Open [http://localhost:3001](http://localhost:3001) in your browser.

---

## Usage

1. **Upload CSV**: Use the web UI to upload a CSV file. The file is stored in S3/MinIO.
2. **Processing**: The backend loads the CSV, infers schema, and creates an external Hive table pointing to the uploaded data.
3. **Table Management**: View, inspect, or drop tables via the UI or backend API.

---

## API Endpoints (Backend)
- `POST /process-csv` — Create Hive table from uploaded CSV
- `GET /tables` — List Hive tables
- `GET /table/<table_name>/info` — Get table info
- `DELETE /table/<table_name>` — Drop table
- `GET /schema/<s3_key>` — Preview inferred schema
- `GET /health` — Health check

---

## Technologies Used
- **Frontend**: Next.js, TypeScript, TailwindCSS, shadcn/ui, tus-js-client
- **Backend**: Python, Sanic, Pandera, Polars, PyHive, Boto3
- **Data**: Apache Hive, Postgres, MinIO (S3-compatible)
- **DevOps**: Docker Compose, Turborepo, Biome, Husky

---

## License
MIT (or specify your license)

---

## Acknowledgements
- [Better-T-Stack](https://github.com/AmanVarshney01/create-better-t-stack) for frontend scaffolding
- Apache Hive, MinIO, Pandera, Polars, and the open-source community