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

    if coll.count_documents({}) > 0:
        return

    try:
        coll.insert_one(
            {
                "username": "admin",
                "username_lower": "admin",
                "password_hash": get_password_hash("password123"),
                "createdAt": now_iso(),
            }
        )
    except DuplicateKeyError:
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
        "createdAt": now_iso(),
    }
    _users_collection().insert_one(doc)
    return doc


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
