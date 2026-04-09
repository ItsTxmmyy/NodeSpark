# NodeSpark

NodeSpark is a simple **data engineering playground**:

- Upload a **CSV** or **JSON** dataset
- Apply transformations (deduplicate, null handling, normalization, format conversion)
- Every transformation creates a **new dataset version** with timestamps + lineage

This repo currently contains:

- **FastAPI backend** in `backend/`
- **Angular frontend** in `frontend/`

MongoDB-backed persistence (datasets, versions, transformation logs, users/roles) is planned, but the current dev build uses a lightweight local storage approach in the backend.

## Requirements

- **Python 3.14+**
- **Node.js + npm** (for Angular)

## Quick start (dev)

You run **two processes** in development: backend + frontend.

### 1) Backend (FastAPI)

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./run.sh
```

Backend runs at `http://127.0.0.1:8000`.

Quick health check:

```bash
curl http://127.0.0.1:8000/health
```

### 2) Frontend (Angular)

```bash
cd frontend
npm install
npm start
```

Frontend runs at `http://localhost:4200`.

## Using the app

Open the **Data Engineering** page in the browser and follow this workflow:

1. **Upload dataset**
   - Select format: `csv` or `json`
   - Choose file
   - Click **Upload**
2. **Transform**
   - Pick a transformation
   - Paste transformation parameters as JSON
   - Click **Apply** (creates a new version)
3. **Version history**
   - See versions with timestamps and the transformation applied
   - “Revert” by selecting an older version, then applying transformations from there

## Sample data

There is a sample dataset you can upload from the UI:

- `sample-data/nodespark_sample.csv`

It includes duplicates, missing values, inconsistent casing, and extra whitespace so you can test transformations.

## Backend API (high level)

- `POST /datasets`: upload a dataset (creates dataset + v1)
- `GET /transformations`: list available transformations + parameter schema
- `POST /pipelines/apply`: apply one or more steps (each step creates its own new version)
- `GET /datasets/{datasetId}/versions`: list version history
- `GET /versions/{versionId}/download`: download dataset bytes for a version
