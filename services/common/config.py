import os


def get_env_str(name: str, default: str) -> str:
    return os.environ.get(name, default).strip()


def get_env_int(name: str, default: int) -> int:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return int(val.strip())
    except ValueError:
        return default


def get_env_float(name: str, default: float) -> float:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return float(val.strip())
    except ValueError:
        return default


def get_env_bool(name: str, default: bool) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in ("true", "1", "yes", "on")
