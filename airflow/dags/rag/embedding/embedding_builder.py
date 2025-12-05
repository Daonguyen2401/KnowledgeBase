import logging
from rag.embedding.build_embedder import EMBEDDERS
from rag.common.utils import filter_kwargs

logger = logging.getLogger(__name__)


def get_embedder(type: str):
    """
    Get an embedder by type from the registry.

    Args:
        type: Embedder type key registered via ``register_embedder``.

    Returns:
        The embedder function/class registered for the given type.

    Raises:
        ValueError: If the embedder type is not found in the registry.
    """
    embedder = EMBEDDERS.get(type)
    if not embedder:
        raise ValueError(f"Embedder not found: {type}")
    return embedder


def create_embedder(
    type: str,
    model: str,
    api_key: str,
    **kwargs,
):
    """
    High-level helper to load/create an embedder instance with parameters.

    Args:
        type: Embedder type key registered via ``register_embedder``.
        **kwargs: Extra params passed to the concrete embedder (e.g., model, api_key).

    Returns:
        Embedder instance ready to use.
    """
    embedder_creator = get_embedder(type)
    kwargs = filter_kwargs(embedder_creator.__init__, kwargs)
    embedder = embedder_creator(model=model, api_key=api_key, **kwargs)
    
    logger.debug(
        f"[load_embedder] Loaded embedder type: {type} with model: {model}"
    )
    return embedder

