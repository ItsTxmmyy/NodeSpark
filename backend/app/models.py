from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


DatasetFormat = Literal["csv", "json"]


class TransformationType(str, Enum):
    deduplicate = "deduplicate"
    null_handling = "null_handling"
    normalize = "normalize"
    convert_format = "convert_format"


class TransformationRecord(BaseModel):
    id: str
    type: TransformationType
    parameters: Dict[str, Any] = Field(default_factory=dict)
    timestamp: str


class Dataset(BaseModel):
    id: str
    name: str
    createdAt: str


class DatasetVersion(BaseModel):
    id: str
    datasetId: str
    versionNumber: int
    format: DatasetFormat
    createdAt: str
    createdFromVersionId: Optional[str] = None
    transformation: Optional[TransformationRecord] = None
    recordCount: Optional[int] = None
    filePath: str


class StorageIndex(BaseModel):
    datasets: List[Dataset] = Field(default_factory=list)
    versions: List[DatasetVersion] = Field(default_factory=list)


class CreateDatasetResponse(BaseModel):
    dataset: Dataset
    version: DatasetVersion


class TransformationStep(BaseModel):
    type: TransformationType
    parameters: Dict[str, Any] = Field(default_factory=dict)


class ApplyPipelineRequest(BaseModel):
    inputVersionId: str
    steps: List[TransformationStep]
    outputFormat: Optional[DatasetFormat] = None


class ApplyPipelineResponse(BaseModel):
    datasetId: str
    inputVersionId: str
    outputVersionId: str
    createdVersionIds: List[str]
    stepsApplied: List[TransformationRecord]

