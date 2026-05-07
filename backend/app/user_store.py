from __future__ import annotations

from typing import Any, Dict, Optional

from pymongo.errors import DuplicateKeyError

from .auth import get_password_hash, verify_password
from .models import now_iso
from .storage import ensure_mongo_indexes, mongo_db


def _users_collection():
    return mongo_db()["users"]


def ensure_users_collection() -> None:
    """Create users indexes and seed default admin when the collection is empty."""
    ensure_mongo_indexes()
    coll = _users_collection()
    coll.create_index("username_lower", unique=True)

    # Always ensure a default admin exists and has role=admin.
    # If the admin user already existed from older versions, this "migrates" it by setting role.
    try:
        coll.update_one(
            {"username_lower": "admin"},
            {
                "$set": {"role": "admin"},
                "$setOnInsert": {
                    "username": "admin",
                    "username_lower": "admin",
                    "password_hash": get_password_hash("password123"),
                    "createdAt": now_iso(),
                },
            },
            upsert=True,
        )
    except DuplicateKeyError:
        # Extremely unlikely with the query above, but keep behavior safe.
        pass


def create_user(username: str, plain_password: str) -> Dict[str, Any]:
    """Insert a new user. Raises DuplicateKeyError if username is taken."""
    ensure_users_collection()
    name = username.strip()
    if not name:
        raise ValueError("username is required")
    if not plain_password:
        raise ValueError("password is required")

    doc = {
        "username": name,
        "username_lower": name.lower(),
        "password_hash": get_password_hash(plain_password),
        "role": "user",
        "createdAt": now_iso(),
    }
    _users_collection().insert_one(doc)
    return doc


def create_user_with_role(username: str, plain_password: str, role: str) -> Dict[str, Any]:
    """Insert a new user with explicit role. Raises DuplicateKeyError if username is taken."""
    ensure_users_collection()
    name = username.strip()
    if not name:
        raise ValueError("username is required")
    if not plain_password:
        raise ValueError("password is required")
    role_norm = role.strip().lower()
    if role_norm not in ("admin", "user"):
        raise ValueError("role must be 'admin' or 'user'")

    doc = {
        "username": name,
        "username_lower": name.lower(),
        "password_hash": get_password_hash(plain_password),
        "role": role_norm,
        "createdAt": now_iso(),
    }
    _users_collection().insert_one(doc)
    return doc


def set_user_role(username: str, role: str) -> Optional[Dict[str, Any]]:
    """
    Set the user's role ("admin" or "user"). Returns updated document or None if not found.
    """
    ensure_users_collection()
    key = username.strip().lower()
    if not key:
        return None
    role_norm = role.strip().lower()
    if role_norm not in ("admin", "user"):
        raise ValueError("role must be 'admin' or 'user'")
    coll = _users_collection()
    coll.update_one({"username_lower": key}, {"$set": {"role": role_norm}})
    return coll.find_one({"username_lower": key}, {"password_hash": 0})


def list_users() -> list[Dict[str, Any]]:
    """List users (excluding password_hash). Admin-only endpoint should call this."""
    ensure_users_collection()
    return list(_users_collection().find({}, {"_id": 0, "password_hash": 0}))


def find_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    """Return user document including password_hash, or None."""
    ensure_users_collection()
    key = username.strip().lower()
    if not key:
        return None
    return _users_collection().find_one({"username_lower": key})


def authenticate(username: str, plain_password: str) -> Optional[Dict[str, Any]]:
    """Return the user document if credentials match, else None."""
    doc = find_user_by_username(username)
    if not doc:
        return None
    if not verify_password(plain_password, doc["password_hash"]):
        return None
    return doc
