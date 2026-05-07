from __future__ import annotations

import os
from typing import Optional, Tuple

from bson import Binary
from pymongo import MongoClient

from .models import Dataset, DatasetVersion, StorageIndex


_mongo_client: Optional[MongoClient] = None


def mongodb_uri() -> str:
    return os.environ.get("MONGODB_URI", "mongodb://127.0.0.1:27017")


def mongodb_db_name() -> str:
    return os.environ.get("MONGODB_DB", "nodespark")


def mongo_client() -> MongoClient:
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(mongodb_uri())
    return _mongo_client


def mongo_db():
    return mongo_client()[mongodb_db_name()]


def ensure_mongo_indexes() -> None:
    db = mongo_db()
    db.datasets.create_index("id", unique=True)
    db.datasets.create_index("ownerId")
    db.versions.create_index("id", unique=True)
    db.versions.create_index([("datasetId", 1), ("versionNumber", 1)], unique=True)
    db.dataset_versions_data.create_index("versionId", unique=True)
    # Fifth collection: lightweight activity log per account.
    db.logs.create_index([("ownerId", 1), ("timestamp", -1)])


def load_index(owner_id: Optional[str]) -> StorageIndex:
    """
    Load datasets and versions visible to this account.

    - If owner_id is a string: scope to that ownerId (normal user behavior).
    - If owner_id is None: load ALL datasets and ALL versions (admin behavior).
    """
    ensure_mongo_indexes()
    db = mongo_db()
    dataset_query = {} if owner_id is None else {"ownerId": owner_id}
    datasets_raw = list(db.datasets.find(dataset_query, {"_id": 0}))
    dataset_ids = [d["id"] for d in datasets_raw]
    if not dataset_ids:
        return StorageIndex(datasets=[], versions=[])
    versions_raw = list(db.versions.find({"datasetId": {"$in": dataset_ids}}, {"_id": 0}))
    return StorageIndex(
        datasets=[Dataset.model_validate(d) for d in datasets_raw],
        versions=[DatasetVersion.model_validate(v) for v in versions_raw],
    )


def save_index(index: StorageIndex) -> None:
    # In Mongo-only mode, writes happen at mutation time; index persistence is a no-op.
    ensure_mongo_indexes()


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


def write_version_file(dataset_id: str, version_id: str, fmt: str, data_bytes: bytes) -> str:
    ensure_mongo_indexes()
    db = mongo_db()
    db.dataset_versions_data.replace_one(
        {"versionId": version_id},
        {
            "versionId": version_id,
            "datasetId": dataset_id,
            "format": fmt,
            "data": Binary(data_bytes),
        },
        upsert=True,
    )
    return f"mongo://{version_id}"


def read_version_file(version: DatasetVersion) -> bytes:
    db = mongo_db()
    doc = db.dataset_versions_data.find_one({"versionId": version.id})
    if not doc:
        raise KeyError(f"version bytes not found: {version.id}")
    return bytes(doc["data"])


def add_dataset_and_version(index: StorageIndex, dataset: Dataset, version: DatasetVersion) -> StorageIndex:
    ensure_mongo_indexes()
    db = mongo_db()
    db.datasets.replace_one({"id": dataset.id}, dataset.model_dump(), upsert=True)
    db.versions.replace_one({"id": version.id}, version.model_dump(), upsert=True)

    index.datasets.append(dataset)
    index.versions.append(version)
    return index


def add_version(index: StorageIndex, version: DatasetVersion) -> StorageIndex:
    ensure_mongo_indexes()
    db = mongo_db()
    db.versions.replace_one({"id": version.id}, version.model_dump(), upsert=True)

    index.versions.append(version)
    return index


def update_version(index: StorageIndex, version: DatasetVersion) -> StorageIndex:
    ensure_mongo_indexes()
    db = mongo_db()
    db.versions.replace_one({"id": version.id}, version.model_dump(), upsert=True)

    for i, existing in enumerate(index.versions):
        if existing.id == version.id:
            index.versions[i] = version
            return index

    index.versions.append(version)
    return index


def latest_version_for_dataset(index: StorageIndex, dataset_id: str) -> Optional[DatasetVersion]:
    versions = [v for v in index.versions if v.datasetId == dataset_id]
    versions.sort(key=lambda v: v.versionNumber)
    return versions[-1] if versions else None


def dataset_versions(index: StorageIndex, dataset_id: str) -> Tuple[DatasetVersion, ...]:
    versions = tuple(v for v in index.versions if v.datasetId == dataset_id)
    return tuple(sorted(versions, key=lambda v: v.versionNumber))

