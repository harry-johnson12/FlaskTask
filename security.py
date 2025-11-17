"""Helper utilities for hashing and verifying user passwords."""

from __future__ import annotations

import bcrypt
from werkzeug.security import check_password_hash

BCRYPT_ROUNDS = 12


def hash_password(password: str) -> str:
    """Hash the provided password using bcrypt with a per-password salt."""

    if not isinstance(password, str):
        raise TypeError("Password must be a string.")
    salt = bcrypt.gensalt(rounds=BCRYPT_ROUNDS)
    hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
    return hashed.decode("utf-8")


def verify_password(password: str, stored_hash: str | bytes | None) -> bool:
    """Validate a plaintext password against a stored bcrypt hash."""

    if not password or not stored_hash:
        return False

    stored_hash_str = stored_hash.decode("utf-8") if isinstance(stored_hash, bytes) else str(stored_hash)

    if stored_hash_str.startswith("scrypt:") or stored_hash_str.startswith("pbkdf2:"):
        return check_password_hash(stored_hash_str, password)

    try:
        return bcrypt.checkpw(password.encode("utf-8"), stored_hash_str.encode("utf-8"))
    except (ValueError, TypeError):
        return False
