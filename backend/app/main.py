from __future__ import annotations

"""FastAPI entrypoint for dataset upload, transforms, and version history."""

import io
import json
from typing import List

import os

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from uuid import uuid4

from .models import (
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
    get_version,
    load_index,
    next_version_number,
    read_version_file,
    save_index,
    update_version,
    write_version_file,
)
from .transforms import apply_single_step
from .powerbi_auth import get_report_embed_config

app = FastAPI(title="NodeSpark Backend", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    """Simple liveness endpoint for local/dev checks."""
    return {"ok": True}


@app.get("/transformations")
def list_transformations():
    """
    Lightweight "capabilities" endpoint for the UI.
    Frontend can use this to render a transformation picker + parameter forms.
    """
    return {
        "transformations": [
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
                "type": TransformationType.convert_format.value,
                "parameters": {},
            },
        ],
        "formats": ["csv", "json"],
    }


@app.get("/datasets", response_model=List[Dataset])
def list_datasets():
    """List all datasets for selector UIs."""
    index = load_index()
    return sorted(index.datasets, key=lambda d: d.createdAt)


@app.post("/datasets", response_model=CreateDatasetResponse)
async def create_dataset(
    name: str,
    format: str,
    file: UploadFile = File(...),
):
    """Create a new dataset and its initial version from uploaded file bytes."""
    fmt = format.lower()
    if fmt not in ("csv", "json"):
        raise HTTPException(status_code=400, detail="format must be csv or json")

    content = await file.read()

    dataset_id = str(uuid4())  # Stable container id for all future versions.
    version_id = str(uuid4())  # Immutable id of this specific snapshot.

    index = load_index()
    version_number = next_version_number(index, dataset_id)
    created_at = now_iso()

    file_path = write_version_file(dataset_id, version_id, fmt, content)

    dataset = Dataset(id=dataset_id, name=name, createdAt=created_at)
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
def list_versions(dataset_id: str):
    """List all versions for a dataset, ordered by version number."""
    index = load_index()
    return sorted([v for v in index.versions if v.datasetId == dataset_id], key=lambda v: v.versionNumber)


@app.get("/versions/{version_id}", response_model=DatasetVersion)
def get_version_meta(version_id: str):
    """Fetch metadata for one concrete version id."""
    index = load_index()
    try:
        return get_version(index, version_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/versions/{version_id}/download")
def download_version(version_id: str):
    """Download raw bytes of a stored version (CSV/JSON)."""
    index = load_index()
    try:
        v = get_version(index, version_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    # Return raw bytes; frontend can parse based on v.format
    return read_version_file(v)


@app.get("/powerbi/{version_id}")
def powerbi_preview(version_id: str):
    """Return version data as JSON records for analytics/PowerBI preview."""
    index = load_index()
    try:
        version = get_version(index, version_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    raw = read_version_file(version)
    if version.format == "json":
        parsed = json.loads(raw.decode("utf-8"))
        if isinstance(parsed, dict) and "records" in parsed:
            parsed = parsed["records"]
        if not isinstance(parsed, list):
            raise HTTPException(status_code=400, detail="stored JSON must be a list of records")
        return parsed

    if version.format == "csv":
        df = pd.read_csv(io.BytesIO(raw))
        return df.to_dict(orient="records")

    raise HTTPException(status_code=400, detail=f"unsupported format: {version.format}")


@app.get("/powerbi/embed/{version_id}")
def powerbi_embed_config(version_id: str):
    """
    Return Power BI embed configuration using Publish to Web URL.
    
    Set this environment variable:
    - POWERBI_EMBED_URL: Your "Publish to Web" embed URL from Power BI Service
    
    To get the URL:
    1. Go to Power BI Service (app.powerbi.com)
    2. Open your report
    3. File → Publish to Web
    4. Copy the embed HTML or iframe src URL
    """
    embed_url = os.getenv("POWERBI_EMBED_URL")
    
    if not embed_url:
        raise HTTPException(
            status_code=500,
            detail="Missing POWERBI_EMBED_URL environment variable. "
                   "Go to Power BI Service → Your Report → File → Publish to Web"
        )
    
    return {
        "type": "report",
        "embedUrl": embed_url,
        "tokenType": 1,
        "settings": {
            "filterPaneEnabled": False,
            "navContentPaneEnabled": True
        }
    }


@app.post("/pipelines/apply", response_model=ApplyPipelineResponse)
def apply_transformation_pipeline(req: ApplyPipelineRequest):
    """
    Apply a step-by-step pipeline and create a new DatasetVersion per step.
    The resulting versions form a parent/child lineage chain.
    """
    index = load_index()
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
def revert_to_version(version_id: str):
    """
    Create a new latest version by cloning bytes from an existing version.
    This preserves history like GitHub: old versions stay unchanged, and revert
    becomes a new snapshot at the end of the timeline.
    """
    index = load_index()
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
def rename_version(version_id: str, req: RenameVersionRequest):
    """Assign or clear a human-friendly name for a version."""
    index = load_index()
    try:
        version = get_version(index, version_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    name = req.versionName.strip()
    version.versionName = name if name else None
    update_version(index, version)
    save_index(index)
    return version

