import yaml
from pathlib import Path

_CONF_DIR = Path(__file__).parent.parent / "conf"


def load_config(filename: str) -> dict:
    with open(_CONF_DIR / filename, "r") as f:
        return yaml.safe_load(f)


def get_exchanges() -> dict:
    return load_config("exchanges.yml")["exchanges"]


def get_instruments() -> list:
    return load_config("instruments.yml")["instruments"]


def get_collection_config() -> dict:
    return load_config("collection.yml")
