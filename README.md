# NodeSpark

NodeSpark is a local **data engineering playground** for testing transformation pipelines on tabular data.

## What it does

- Upload datasets in `csv` or `json`
- Apply transformation steps (deduplicate, null handling, normalize, convert format)
- Save every step as a **new dataset version** with lineage/history
- Preview transformed output from the frontend analytics views

## Project structure

- `backend/`: FastAPI service and storage/index logic
- `frontend/`: Angular application
- `sample-data/`: ready-to-upload sample files

## Requirements

- Python `3.14+`
- Node.js `18+` (or current LTS) and npm

## Local setup

You run two processes during development: backend and frontend.

### 1) Backend (FastAPI)

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
./run.sh
```

`./run.sh` automatically:

- sets `PYTHONPATH` for the backend package
- loads environment variables from `backend/.env` (if the file exists)
- starts Uvicorn on port `8000`

You do **not** need to manually `export` env vars when using `./run.sh`.

Backend URL: `http://127.0.0.1:8000`

Health check:

```bash
curl http://127.0.0.1:8000/health
```

### 2) Frontend (Angular)

```bash
cd frontend
npm install
npm start
```

Frontend URL: `http://localhost:4200`

## Typical workflow

1. Open the app and navigate to the **Data Engineering** page
2. Upload a `csv` or `json` dataset
3. Select a transformation and provide parameters
4. Apply pipeline steps (each step creates a new saved version)
5. Review version history and continue from any previous version

## Sample data

Use `sample-data/nodespark_sample.csv` to quickly test:

- duplicates
- null values
- inconsistent casing
- whitespace cleanup

## Backend API (high level)

- `GET /health`: service liveness
- `GET /transformations`: available transforms + parameter definitions
- `POST /datasets`: create dataset + initial version
- `GET /datasets`: list datasets
- `GET /datasets/{dataset_id}/versions`: list dataset versions
- `POST /pipelines/apply`: apply one or more transformation steps
- `GET /versions/{version_id}`: fetch version metadata
- `GET /versions/{version_id}/download`: download a stored version
- `GET /versions/{version_id}/records`: JSON records preview for analytics
