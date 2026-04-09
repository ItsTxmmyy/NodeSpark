from __future__ import annotations

from typing import List

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from uuid import uuid4

from .models import (
    ApplyPipelineRequest,
    ApplyPipelineResponse,
    CreateDatasetResponse,
    Dataset,
    DatasetVersion,
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
    save_index,
    write_version_file,
)
from .transforms import apply_single_step

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


@app.post("/datasets", response_model=CreateDatasetResponse)
async def create_dataset(
    name: str,
    format: str,
    file: UploadFile = File(...),
):
    fmt = format.lower()
    if fmt not in ("csv", "json"):
        raise HTTPException(status_code=400, detail="format must be csv or json")

    content = await file.read()

    dataset_id = str(uuid4())
    version_id = str(uuid4())

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
    index = load_index()
    return sorted([v for v in index.versions if v.datasetId == dataset_id], key=lambda v: v.versionNumber)


@app.get("/versions/{version_id}", response_model=DatasetVersion)
def get_version_meta(version_id: str):
    index = load_index()
    try:
        return get_version(index, version_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/versions/{version_id}/download")
def download_version(version_id: str):
    index = load_index()
    try:
        v = get_version(index, version_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    # Return raw bytes; frontend can parse based on v.format
    return open(v.filePath, "rb").read()


@app.post("/pipelines/apply", response_model=ApplyPipelineResponse)
def apply_transformation_pipeline(req: ApplyPipelineRequest):
    index = load_index()
    try:
        input_version = get_version(index, req.inputVersionId)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    current_version = input_version
    current_bytes = open(current_version.filePath, "rb").read()
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

