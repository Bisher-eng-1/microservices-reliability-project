"""
Centralized Configuration Loader.
Reads environment variables with typed defaults and validation.
"""
import os


def get_env_str(name: str, default: str) -> str:
    """Retrieve string environment variable or default."""
    return os.environ.get(name, default).strip()


def get_env_int(name: str, default: int) -> int:
    """Retrieve integer environment variable or default."""
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return int(val.strip())
    except ValueError:
        return default


def get_env_float(name: str, default: float) -> float:
    """Retrieve float environment variable or default."""
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return float(val.strip())
    except ValueError:
        return default


def get_env_bool(name: str, default: bool) -> bool:
    """Retrieve boolean environment variable or default."""
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in ("true", "1", "yes", "on")
