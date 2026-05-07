from __future__ import annotations

"""FastAPI entrypoint for dataset upload, transforms, and version history."""

import io
import json
import math
from typing import Any, List

from fastapi import FastAPI, File, HTTPException, UploadFile, Depends
from fastapi.responses import StreamingResponse
from fastapi.security import OAuth2PasswordRequestForm
from pymongo.errors import DuplicateKeyError

from .auth import create_access_token, get_current_user_id
from . import user_store
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from pydantic import BaseModel, ValidationError
from uuid import uuid4

try:
    from openai import APIError as OpenAIAPIError
except Exception:
    OpenAIAPIError = Exception  # type: ignore[misc,assignment]

from .models import (
    AiSuggestRequest,
    ApplyPipelineRequest,
    ApplyPipelineResponse,
    CreateDatasetResponse,
    Dataset,
    DatasetVersion,
    RenameVersionRequest,
    TransformationRecord,
    TransformationType,
    now_iso,
)
from .storage import (
    add_dataset_and_version,
    add_version,
    get_dataset,
    get_version,
    load_index,
    next_version_number,
    read_version_file,
    save_index,
    update_version,
    write_version_file,
)
from .transforms import apply_single_step
from .transforms import infer_columns_and_sample_rows
from . import ai_assistant

app = FastAPI(title="NodeSpark Backend", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class AuthRequest(BaseModel):
    username: str
    password: str

# Capabilities payload shared by /transformations and AI assistant context.
TRANSFORMATION_CAPABILITIES = [
    {
        "type": TransformationType.deduplicate.value,
        "parameters": {
            "subset": {"type": "array", "items": "string", "optional": True},
            "keep": {"type": "string|false", "allowed": ["first", "last", False], "default": "first"},
        },
    },
    {
        "type": TransformationType.null_handling.value,
        "parameters": {
            "strategy": {"type": "string", "allowed": ["remove", "fill"], "default": "remove"},
            "columns": {"type": "array", "items": "string", "optional": True},
            "value": {"type": "any", "optional": True, "used_when": {"strategy": "fill"}},
        },
    },
    {
        "type": TransformationType.normalize.value,
        "parameters": {
            "columns": {"type": "array", "items": "string", "optional": True},
            "trim": {"type": "boolean", "default": True},
            "case": {"type": "string|null", "allowed": ["lower", "upper", "title", None], "optional": True},
        },
    },
    {
        "type": TransformationType.filter_rows.value,
        "parameters": {
            "column": {"type": "string", "required": True},
            "operator": {
                "type": "string",
                "allowed": [
                    "equals",
                    "not_equals",
                    "contains",
                    "not_contains",
                    "starts_with",
                    "ends_with",
                    "is_empty",
                    "is_not_empty",
                    "gt",
                    "gte",
                    "lt",
                    "lte",
                ],
                "default": "equals",
            },
            "value": {"type": "any", "optional": True},
            "mode": {"type": "string", "allowed": ["include", "exclude"], "default": "include"},
        },
    },
    {
        "type": TransformationType.convert_format.value,
        "parameters": {},
    },
]

# --- AUTHENTICATION ROUTES ---

@app.post("/signup")
async def signup(user: AuthRequest):
    """Register a new user in MongoDB (`users` collection, database from MONGODB_DB)."""
    try:
        user_store.create_user(user.username, user.password)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except DuplicateKeyError:
        raise HTTPException(status_code=400, detail="Username already taken") from None

    return {"message": "User created successfully"}


@app.post("/login")
async def login(user: AuthRequest):
    """Login against MongoDB users; JWT subject is the stored username (casing as registered)."""
    doc = user_store.authenticate(user.username, user.password)
    if not doc:
        raise HTTPException(status_code=401, detail="Incorrect username or password")

    access_token = create_access_token(data={"sub": doc["username"]})
    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/token")
async def token_login(form_data: OAuth2PasswordRequestForm = Depends()):
    """OAuth2 password flow; same credentials as /login (MongoDB-backed)."""
    doc = user_store.authenticate(form_data.username, form_data.password)
    if not doc:
        raise HTTPException(status_code=401, detail="Incorrect username or password")

    access_token = create_access_token(data={"sub": doc["username"]})
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/health")
def health():
    """Simple liveness endpoint for local/dev checks."""
    return {"ok": True}


@app.get("/transformations")
def list_transformations(_user: str = Depends(get_current_user_id)):
    """
    Lightweight "capabilities" endpoint for the UI.
    Frontend can use this to render a transformation picker + parameter forms.
    """
    return {
        "transformations": TRANSFORMATION_CAPABILITIES,
        "formats": ["csv", "json"],
    }


@app.post("/ai/suggest")
def ai_suggest(req: AiSuggestRequest, current_user: str = Depends(get_current_user_id)):
    """
    Use the AI assistant to suggest a transformation pipeline for a given dataset version.
    Requires auth (same scope as dataset APIs).
    """
    index = load_index(current_user)
    try:
        version = get_version(index, req.inputVersionId)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    raw = read_version_file(version)
    columns, sample_rows = infer_columns_and_sample_rows(
        raw, version.format, sample_size=max(1, min(int(req.sampleSize), 100))
    )

    try:
        return ai_assistant.suggest_transformations(
            prompt=req.prompt,
            transformations=TRANSFORMATION_CAPABILITIES,
            columns=columns,
            sample_rows=sample_rows,
        )
    except ValueError as e:
        # e.g. OPENAI_API_KEY missing
        raise HTTPException(status_code=500, detail=str(e)) from e
    except ValidationError as e:
        raise HTTPException(status_code=502, detail=f"Invalid AI JSON response: {e}") from e
    except OpenAIAPIError as e:
        raise HTTPException(status_code=502, detail=f"OpenAI error: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI assistant request failed: {e!s}") from e


@app.get("/datasets", response_model=List[Dataset])
def list_datasets(current_user: str = Depends(get_current_user_id)):
    """List datasets owned by the authenticated user."""
    index = load_index(current_user)
    return sorted(index.datasets, key=lambda d: d.createdAt)


@app.post("/datasets", response_model=CreateDatasetResponse)
async def create_dataset(
    name: str,
    format: str,
    file: UploadFile = File(...),
    current_user: str = Depends(get_current_user_id),
):
    """Create a new dataset and its initial version from uploaded file bytes."""
    fmt = format.lower()
    if fmt not in ("csv", "json"):
        raise HTTPException(status_code=400, detail="format must be csv or json")

    content = await file.read()

    dataset_id = str(uuid4())  # Stable container id for all future versions.
    version_id = str(uuid4())  # Immutable id of this specific snapshot.

    index = load_index(current_user)
    version_number = next_version_number(index, dataset_id)
    created_at = now_iso()

    file_path = write_version_file(dataset_id, version_id, fmt, content)

    dataset = Dataset(id=dataset_id, name=name, createdAt=created_at, ownerId=current_user)
    version = DatasetVersion(
        id=version_id,
        datasetId=dataset_id,
        versionNumber=version_number,
        format=fmt,  # type: ignore[arg-type]
        createdAt=created_at,
        filePath=file_path,
    )

    add_dataset_and_version(index, dataset, version)
    save_index(index)
    return CreateDatasetResponse(dataset=dataset, version=version)


@app.get("/datasets/{dataset_id}/versions", response_model=List[DatasetVersion])
def list_versions(dataset_id: str, current_user: str = Depends(get_current_user_id)):
    """List all versions for a dataset, ordered by version number."""
    index = load_index(current_user)
    if not any(d.id == dataset_id for d in index.datasets):
        raise HTTPException(status_code=404, detail="dataset not found")
    return sorted([v for v in index.versions if v.datasetId == dataset_id], key=lambda v: v.versionNumber)


@app.get("/versions/{version_id}", response_model=DatasetVersion)
def get_version_meta(version_id: str, current_user: str = Depends(get_current_user_id)):
    """Fetch metadata for one concrete version id."""
    index = load_index(current_user)
    try:
        return get_version(index, version_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/versions/{version_id}/download")
def download_version(version_id: str, current_user: str = Depends(get_current_user_id)):
    """Download a stored version as a file (CSV/JSON) with proper headers."""
    index = load_index(current_user)
    try:
        v = get_version(index, version_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    dataset = None
    try:
        dataset = get_dataset(index, v.datasetId)
    except KeyError:
        dataset = None

    raw = read_version_file(v)
    ext = "json" if v.format == "json" else "csv"
    media_type = "application/json" if v.format == "json" else "text/csv; charset=utf-8"
    base = (dataset.name if dataset else v.datasetId).strip().replace("/", "-")
    label = (v.versionName or f"v{v.versionNumber}").strip().replace("/", "-")
    filename = f"{base}_{label}.{ext}"

    return StreamingResponse(
        io.BytesIO(raw),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/versions/{version_id}/records")
def get_version_records(version_id: str, current_user: str = Depends(get_current_user_id)):
    """Return version data as JSON records for in-app analytics preview."""
    index = load_index(current_user)
    try:
        version = get_version(index, version_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    raw = read_version_file(version)

    def sanitize_jsonish(value: Any) -> Any:
        """
        FastAPI/Starlette uses stdlib `json` which can't serialize NaN/Infinity.
        Convert any non-finite numeric values to `None` recursively.
        """
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (str, int)):
            return value
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None
            return value
        if isinstance(value, dict):
            return {k: sanitize_jsonish(v) for k, v in value.items()}
        if isinstance(value, list):
            return [sanitize_jsonish(v) for v in value]

        # Covers numpy scalar floats and other "number-like" values.
        try:
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except Exception:
            return value
    if version.format == "json":
        parsed = json.loads(raw.decode("utf-8"))
        if isinstance(parsed, dict) and "records" in parsed:
            parsed = parsed["records"]
        if not isinstance(parsed, list):
            raise HTTPException(status_code=400, detail="stored JSON must be a list of records")
        return sanitize_jsonish(parsed)

    if version.format == "csv":
        df = pd.read_csv(io.BytesIO(raw))
        records = df.to_dict(orient="records")
        return sanitize_jsonish(records)

    raise HTTPException(status_code=400, detail=f"unsupported format: {version.format}")


@app.post("/pipelines/apply", response_model=ApplyPipelineResponse)
def apply_transformation_pipeline(req: ApplyPipelineRequest, current_user: str = Depends(get_current_user_id)):
    """
    Apply a step-by-step pipeline and create a new DatasetVersion per step.
    The resulting versions form a parent/child lineage chain.
    """
    index = load_index(current_user)
    try:
        input_version = get_version(index, req.inputVersionId)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    current_version = input_version
    current_bytes = read_version_file(current_version)
    created_ids: List[str] = []
    applied_records: List[TransformationRecord] = []

    for i, step in enumerate(req.steps):
        created_at = now_iso()

        # Output format only applies to the final step (so intermediate versions keep current format)
        final_output_format = req.outputFormat if i == (len(req.steps) - 1) else None

        try:
            out_bytes, out_fmt, record_count = apply_single_step(
                input_bytes=current_bytes,
                input_format=current_version.format,  # type: ignore[arg-type]
                step_type=step.type.value,
                parameters=step.parameters,
                output_format=final_output_format,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        # Persist every step as a materialized version so history is inspectable/revertable.
        new_version_id = str(uuid4())
        version_number = next_version_number(index, current_version.datasetId)
        out_path = write_version_file(current_version.datasetId, new_version_id, out_fmt, out_bytes)

        record = TransformationRecord(
            id=str(uuid4()),
            type=step.type,
            parameters=step.parameters,
            timestamp=created_at,
        )

        new_version = DatasetVersion(
            id=new_version_id,
            datasetId=current_version.datasetId,
            versionNumber=version_number,
            format=out_fmt,  # type: ignore[arg-type]
            createdAt=created_at,
            createdFromVersionId=current_version.id,
            transformation=record,
            recordCount=record_count,
            filePath=out_path,
        )

        add_version(index, new_version)
        created_ids.append(new_version_id)
        applied_records.append(record)

        # Advance pipeline cursor so next step uses fresh output from this step.
        current_version = new_version
        current_bytes = out_bytes

    save_index(index)

    output_version_id = created_ids[-1] if created_ids else input_version.id
    return ApplyPipelineResponse(
        datasetId=input_version.datasetId,
        inputVersionId=input_version.id,
        outputVersionId=output_version_id,
        createdVersionIds=created_ids,
        stepsApplied=applied_records,
    )


@app.post("/versions/{version_id}/revert", response_model=DatasetVersion)
def revert_to_version(version_id: str, current_user: str = Depends(get_current_user_id)):
    """
    Create a new latest version by cloning bytes from an existing version.
    This preserves history like GitHub: old versions stay unchanged, and revert
    becomes a new snapshot at the end of the timeline.
    """
    index = load_index(current_user)
    try:
        source_version = get_version(index, version_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    source_bytes = read_version_file(source_version)
    created_at = now_iso()
    new_version_id = str(uuid4())
    new_version_number = next_version_number(index, source_version.datasetId)
    out_path = write_version_file(source_version.datasetId, new_version_id, source_version.format, source_bytes)

    new_version = DatasetVersion(
        id=new_version_id,
        datasetId=source_version.datasetId,
        versionNumber=new_version_number,
        format=source_version.format,  # type: ignore[arg-type]
        createdAt=created_at,
        createdFromVersionId=source_version.id,
        recordCount=source_version.recordCount,
        filePath=out_path,
    )

    add_version(index, new_version)
    save_index(index)
    return new_version


@app.patch("/versions/{version_id}/name", response_model=DatasetVersion)
def rename_version(version_id: str, req: RenameVersionRequest, current_user: str = Depends(get_current_user_id)):
    """Assign or clear a human-friendly name for a version."""
    index = load_index(current_user)
    try:
        version = get_version(index, version_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    name = req.versionName.strip()
    version.versionName = name if name else None
    update_version(index, version)
    save_index(index)
    return version

