import logging
logger = logging.getLogger(__name__)

EMBEDDERS = {}


def register_embedder(type: str) -> None:
    def decorator(func):
        EMBEDDERS[type] = func
        logger.debug(
            f"[register_embedder] Registered embedder: {type} -> {func.__name__}"
        )
        return func

    return decorator

