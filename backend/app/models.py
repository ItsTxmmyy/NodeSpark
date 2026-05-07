from __future__ import annotations

"""Shared API/data models for datasets, versions, and transformation pipelines."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


def now_iso() -> str:
    """Return current UTC time in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


DatasetFormat = Literal["csv", "json"]


class TransformationType(str, Enum):
    """Supported transformation step types."""
    deduplicate = "deduplicate"
    null_handling = "null_handling"
    normalize = "normalize"
    convert_format = "convert_format"
    filter_rows = "filter_rows"


class TransformationRecord(BaseModel):
    """Audit entry describing one transformation applied to a version."""
    id: str
    type: TransformationType
    parameters: Dict[str, Any] = Field(default_factory=dict)
    timestamp: str


class Dataset(BaseModel):
    """Top-level dataset container (business object)."""
    id: str
    name: str
    createdAt: str
    # JWT `sub` (username) of the account that owns this dataset; scoped reads/writes.
    ownerId: Optional[str] = None


class DatasetVersion(BaseModel):
    """Immutable snapshot of dataset contents after each step."""
    id: str
    datasetId: str
    versionNumber: int
    format: DatasetFormat
    createdAt: str
    versionName: Optional[str] = None
    # Parent pointer used to build version lineage (root version has None).
    createdFromVersionId: Optional[str] = None
    transformation: Optional[TransformationRecord] = None
    recordCount: Optional[int] = None
    # Absolute path of serialized dataset bytes in local storage.
    filePath: str


class StorageIndex(BaseModel):
    """In-memory representation of storage/index.json."""
    datasets: List[Dataset] = Field(default_factory=list)
    versions: List[DatasetVersion] = Field(default_factory=list)


class CreateDatasetResponse(BaseModel):
    """Response payload for initial upload endpoint."""
    dataset: Dataset
    version: DatasetVersion


class TransformationStep(BaseModel):
    """One requested pipeline step from the client."""
    type: TransformationType
    parameters: Dict[str, Any] = Field(default_factory=dict)


class ApplyPipelineRequest(BaseModel):
    """Request payload for applying one or many transformation steps."""
    inputVersionId: str
    steps: List[TransformationStep]
    outputFormat: Optional[DatasetFormat] = None


class ApplyPipelineResponse(BaseModel):
    """Response payload that returns lineage and step metadata."""
    datasetId: str
    inputVersionId: str
    outputVersionId: str
    createdVersionIds: List[str]
    stepsApplied: List[TransformationRecord]


class RenameVersionRequest(BaseModel):
    """Request payload for assigning a human-friendly version name."""
    versionName: str


# --- AI assistant models ---


class AiSuggestStep(BaseModel):
    """One suggested pipeline step."""

    type: str
    parameters: Dict[str, Any] = Field(default_factory=dict)


class AiSuggestResponse(BaseModel):
    """Assistant output contract used by ai_assistant.suggest_transformations."""

    steps: List[AiSuggestStep] = Field(default_factory=list)
    explanation: str = ""
    assumptions: List[str] = Field(default_factory=list)
    needsClarification: bool = False
    clarificationQuestion: Optional[str] = None


class AiSuggestRequest(BaseModel):
    """Request payload for AI pipeline suggestions."""

    inputVersionId: str
    prompt: str
    sampleSize: int = 20

