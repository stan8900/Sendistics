import logging
import os
import time
from pathlib import Path
from typing import Optional

from .storage import Storage


logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent.parent


def resolve_storage_paths() -> tuple[Path, Path]:
    storage_path_env = os.getenv("STORAGE_PATH")
    if storage_path_env:
        storage_path = Path(storage_path_env)
        if not storage_path.is_absolute():
            storage_path = (BASE_DIR / storage_path).resolve()
    else:
        storage_path = (BASE_DIR / "data" / "storage.db").resolve()

    if storage_path.suffix == ".json":
        legacy_storage_path = storage_path
        storage_path = storage_path.with_suffix(".db")
    else:
        legacy_storage_path = storage_path.with_suffix(".json")
    return storage_path, legacy_storage_path


def create_storage_from_env() -> Storage:
    database_url: Optional[str] = os.getenv("DATABASE_URL") or None
    database_required = os.getenv("DATABASE_URL_REQUIRED", "false").lower() in {"1", "true", "yes"}
    storage_path, legacy_storage_path = resolve_storage_paths()

    if database_url:
        attempts = 5
        for attempt in range(1, attempts + 1):
            try:
                logger.info("Используем PostgreSQL хранилище (попытка %s/%s).", attempt, attempts)
                return Storage(storage_path, legacy_json_path=legacy_storage_path, database_url=database_url)
            except Exception:
                logger.exception("Не удалось подключиться к PostgreSQL (попытка %s).", attempt)
                if attempt == attempts:
                    if database_required:
                        raise
                    logger.warning("Переходим на локальную SQLite-базу по пути %s.", storage_path)
                    break
                wait_for = min(5, attempt)
                logger.info("Повторяем подключение через %s c.", wait_for)
                time.sleep(wait_for)
    return Storage(storage_path, legacy_json_path=legacy_storage_path)
