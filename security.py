"""Helper utilities for hashing, verifying, and encrypting user data."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import bcrypt
from cryptography.fernet import Fernet, InvalidToken
from werkzeug.security import check_password_hash

BCRYPT_ROUNDS = 12
SENSITIVE_KEY_ENV = "SENSITIVE_DATA_KEY"
SENSITIVE_KEY_FILE = Path(__file__).with_name("sensitive_key.txt")

_sensitive_key_cache: Optional[bytes] = None
_sensitive_cipher: Optional[Fernet] = None


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


def _load_sensitive_key() -> bytes:
    """Fetch or lazily generate the symmetric key used for sensitive columns."""

    global _sensitive_key_cache
    if _sensitive_key_cache:
        return _sensitive_key_cache

    env_key = os.getenv(SENSITIVE_KEY_ENV)
    if env_key:
        key_bytes = env_key.strip().encode("utf-8")
    elif SENSITIVE_KEY_FILE.exists():
        key_bytes = SENSITIVE_KEY_FILE.read_bytes().strip()
    else:
        key_bytes = Fernet.generate_key()
        SENSITIVE_KEY_FILE.write_bytes(key_bytes)

    _sensitive_key_cache = key_bytes
    return key_bytes


def _get_sensitive_cipher() -> Fernet:
    global _sensitive_cipher
    if _sensitive_cipher is None:
        key_bytes = _load_sensitive_key()
        _sensitive_cipher = Fernet(key_bytes)
    return _sensitive_cipher


def encrypt_sensitive_value(value: Optional[str]) -> str:
    """Encrypt a sensitive string using the shared symmetric key."""

    if value is None:
        value = ""
    cipher = _get_sensitive_cipher()
    token = cipher.encrypt(value.encode("utf-8"))
    return token.decode("utf-8")


def decrypt_sensitive_value(value: Optional[str]) -> str:
    """Decrypt a stored sensitive value, returning the plain text."""

    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    if not value:
        return ""

    cipher = _get_sensitive_cipher()
    try:
        return cipher.decrypt(value.encode("utf-8")).decode("utf-8")
    except (InvalidToken, ValueError, TypeError):
        # Legacy rows may still exist in plaintext; surface them as-is.
        return value
