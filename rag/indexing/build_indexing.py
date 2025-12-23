import logging
logger = logging.getLogger(__name__)

VECTOR_STORES = {}


def register_vector_store(type: str) -> None:
    def decorator(func):
        VECTOR_STORES[type] = func
        logger.debug(
            f"[register_vector_store] Registered vector store: {type} -> {func.__name__}"
        )
        return func

    return decorator

