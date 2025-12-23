import logging
from typing import Dict, Optional
logger = logging.getLogger(__name__)

DOCUMENT_LOADERS = {}


def register_document_loader(type: str) -> None:
    def decorator(func):
        DOCUMENT_LOADERS[type] = func
        logger.debug(
            f"[register_document_loader] Registered document loader: {type} -> {func.__name__}"
        )
        return func

    return decorator