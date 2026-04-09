from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional, Tuple

from pydantic import TypeAdapter

from .models import Dataset, DatasetVersion, StorageIndex


def storage_root() -> Path:
    return Path(os.environ.get("NODESPARK_STORAGE_DIR", Path(__file__).resolve().parent.parent / "storage")).resolve()


def index_path() -> Path:
    return storage_root() / "index.json"


def dataset_dir(dataset_id: str) -> Path:
    return storage_root() / "datasets" / dataset_id


def versions_dir(dataset_id: str) -> Path:
    return dataset_dir(dataset_id) / "versions"


def ensure_storage_dirs() -> None:
    (storage_root() / "datasets").mkdir(parents=True, exist_ok=True)


_index_adapter = TypeAdapter(StorageIndex)


def load_index() -> StorageIndex:
    ensure_storage_dirs()
    p = index_path()
    if not p.exists():
        return StorageIndex()
    return _index_adapter.validate_python(json.loads(p.read_text(encoding="utf-8")))


def save_index(index: StorageIndex) -> None:
    ensure_storage_dirs()
    index_path().write_text(index.model_dump_json(indent=2), encoding="utf-8")


def next_version_number(index: StorageIndex, dataset_id: str) -> int:
    nums = [v.versionNumber for v in index.versions if v.datasetId == dataset_id]
    return (max(nums) + 1) if nums else 1


def get_version(index: StorageIndex, version_id: str) -> DatasetVersion:
    for v in index.versions:
        if v.id == version_id:
            return v
    raise KeyError(f"version not found: {version_id}")


def get_dataset(index: StorageIndex, dataset_id: str) -> Dataset:
    for d in index.datasets:
        if d.id == dataset_id:
            return d
    raise KeyError(f"dataset not found: {dataset_id}")


def version_file_path(dataset_id: str, version_id: str, fmt: str) -> Path:
    ext = "csv" if fmt == "csv" else "json"
    return versions_dir(dataset_id) / f"{version_id}.{ext}"


def write_version_file(dataset_id: str, version_id: str, fmt: str, data_bytes: bytes) -> str:
    versions_dir(dataset_id).mkdir(parents=True, exist_ok=True)
    p = version_file_path(dataset_id, version_id, fmt)
    p.write_bytes(data_bytes)
    return str(p)


def read_version_file(version: DatasetVersion) -> bytes:
    return Path(version.filePath).read_bytes()


def add_dataset_and_version(index: StorageIndex, dataset: Dataset, version: DatasetVersion) -> StorageIndex:
    index.datasets.append(dataset)
    index.versions.append(version)
    return index


def add_version(index: StorageIndex, version: DatasetVersion) -> StorageIndex:
    index.versions.append(version)
    return index


def latest_version_for_dataset(index: StorageIndex, dataset_id: str) -> Optional[DatasetVersion]:
    versions = [v for v in index.versions if v.datasetId == dataset_id]
    versions.sort(key=lambda v: v.versionNumber)
    return versions[-1] if versions else None


def dataset_versions(index: StorageIndex, dataset_id: str) -> Tuple[DatasetVersion, ...]:
    versions = tuple(v for v in index.versions if v.datasetId == dataset_id)
    return tuple(sorted(versions, key=lambda v: v.versionNumber))

