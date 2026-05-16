import logging
import os
import time
from pathlib import Path
from typing import Optional

try:
    from .storage import AnalyticsStorage
except ImportError:  # pragma: no cover - allows `python web_admin/main.py`
    from storage import AnalyticsStorage


logger = logging.getLogger(__name__)
WEB_DIR = Path(__file__).resolve().parent
BASE_DIR = WEB_DIR.parent


def resolve_storage_path() -> Path:
    storage_path_env = os.getenv("STORAGE_PATH")
    if storage_path_env:
        storage_path = Path(storage_path_env)
        if not storage_path.is_absolute():
            storage_path = (BASE_DIR / storage_path).resolve()
    else:
        storage_path = (BASE_DIR / "data" / "storage.db").resolve()
    if storage_path.suffix == ".json":
        storage_path = storage_path.with_suffix(".db")
    return storage_path


def create_storage_from_env() -> AnalyticsStorage:
    database_url: Optional[str] = os.getenv("DATABASE_URL") or None
    database_required = os.getenv("DATABASE_URL_REQUIRED", "false").lower() in {"1", "true", "yes"}
    storage_path = resolve_storage_path()

    if database_url:
        attempts = 5
        for attempt in range(1, attempts + 1):
            try:
                logger.info("Веб-админка читает статистику из PostgreSQL (попытка %s/%s).", attempt, attempts)
                return AnalyticsStorage(storage_path, database_url=database_url)
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
    logger.info("Веб-админка читает статистику из SQLite: %s.", storage_path)
    return AnalyticsStorage(storage_path)
