import logging
from typing import Dict, Optional
logger = logging.getLogger(__name__)

TEXT_SPLITTERS = {}


def register_text_splitter(type: str) -> None:
    def decorator(func):
        TEXT_SPLITTERS[type] = func
        logger.debug(
            f"[register_text_splitter] Registered text splitter: {type} -> {func.__name__}"
        )
        return func

    return decorator

