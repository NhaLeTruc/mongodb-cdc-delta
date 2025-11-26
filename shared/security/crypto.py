"""Cryptographic utilities for data encryption and hashing.

Provides secure encryption, decryption, and hashing functions.
"""

import hashlib
import hmac
import os
from base64 import b64decode, b64encode
from typing import Union

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2


def generate_key(password: Union[str, bytes], salt: bytes) -> bytes:
    """Generate encryption key from password using PBKDF2.

    Args:
        password: Password string or bytes
        salt: Salt for key derivation

    Returns:
        Derived encryption key
    """
    if isinstance(password, str):
        password = password.encode()

    kdf = PBKDF2(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    return b64encode(kdf.derive(password))


def encrypt_data(
    data: Union[str, bytes], key: Union[str, bytes]
) -> str:
    """Encrypt data using Fernet symmetric encryption.

    Args:
        data: Data to encrypt (string or bytes)
        key: Encryption key

    Returns:
        Base64-encoded encrypted data
    """
    if isinstance(data, str):
        data = data.encode()

    if isinstance(key, str):
        key = key.encode()

    fernet = Fernet(key)
    encrypted = fernet.encrypt(data)
    return b64encode(encrypted).decode()


def decrypt_data(
    encrypted_data: Union[str, bytes], key: Union[str, bytes]
) -> str:
    """Decrypt data using Fernet symmetric encryption.

    Args:
        encrypted_data: Encrypted data (base64 string or bytes)
        key: Decryption key

    Returns:
        Decrypted data as string
    """
    if isinstance(encrypted_data, str):
        encrypted_data = b64decode(encrypted_data.encode())

    if isinstance(key, str):
        key = key.encode()

    fernet = Fernet(key)
    decrypted = fernet.decrypt(encrypted_data)
    return decrypted.decode()


def hash_data(data: Union[str, bytes], algorithm: str = "sha256") -> str:
    """Hash data using specified algorithm.

    Args:
        data: Data to hash
        algorithm: Hash algorithm (sha256, sha512, md5)

    Returns:
        Hexadecimal hash string
    """
    if isinstance(data, str):
        data = data.encode()

    if algorithm == "sha256":
        hasher = hashlib.sha256()
    elif algorithm == "sha512":
        hasher = hashlib.sha512()
    elif algorithm == "md5":
        hasher = hashlib.md5()
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    hasher.update(data)
    return hasher.hexdigest()


def generate_hmac(
    data: Union[str, bytes], key: Union[str, bytes], algorithm: str = "sha256"
) -> str:
    """Generate HMAC for data integrity verification.

    Args:
        data: Data to create HMAC for
        key: Secret key
        algorithm: HMAC algorithm (sha256, sha512)

    Returns:
        Hexadecimal HMAC string
    """
    if isinstance(data, str):
        data = data.encode()

    if isinstance(key, str):
        key = key.encode()

    if algorithm == "sha256":
        hasher = hmac.new(key, data, hashlib.sha256)
    elif algorithm == "sha512":
        hasher = hmac.new(key, data, hashlib.sha512)
    else:
        raise ValueError(f"Unsupported HMAC algorithm: {algorithm}")

    return hasher.hexdigest()


def verify_hmac(
    data: Union[str, bytes],
    key: Union[str, bytes],
    expected_hmac: str,
    algorithm: str = "sha256",
) -> bool:
    """Verify HMAC for data integrity.

    Args:
        data: Data to verify
        key: Secret key
        expected_hmac: Expected HMAC value
        algorithm: HMAC algorithm (sha256, sha512)

    Returns:
        True if HMAC matches, False otherwise
    """
    computed_hmac = generate_hmac(data, key, algorithm)
    return hmac.compare_digest(computed_hmac, expected_hmac)


def generate_random_key() -> str:
    """Generate a random Fernet encryption key.

    Returns:
        Base64-encoded encryption key
    """
    return Fernet.generate_key().decode()


def generate_salt(size: int = 16) -> bytes:
    """Generate random salt for key derivation.

    Args:
        size: Salt size in bytes (default: 16)

    Returns:
        Random salt bytes
    """
    return os.urandom(size)
