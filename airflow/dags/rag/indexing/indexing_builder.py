import logging
from build_indexing import VECTOR_STORES

logger = logging.getLogger(__name__)


def get_indexer(type: str):
    """
    Get a vector store indexer by type from the registry.

    Args:
        type: Vector store type key registered via ``register_vector_store``.

    Returns:
        The vector store creator function registered for the given type.

    Raises:
        ValueError: If the vector store type is not found in the registry.
    """
    indexer = VECTOR_STORES.get(type)
    if not indexer:
        raise ValueError(f"Vector store not found: {type}")
    return indexer


def index_documents(
    type: str,
    documents,
    embeddings,
    collection_name: str,
    connection_url: str,
    **kwargs,
):
    """
    High-level helper to load/create a vector store instance with parameters.

    Args:
        type: Vector store type key registered via ``register_vector_store``.
        embeddings: Embeddings instance to use for vectorization.
        collection_name: Name of the collection/table in the vector store.
        connection: Connection string for the database/storage.

    Returns:
        Vector store instance ready to use.
    """
    indexer_creator = get_indexer(type)
    vector_store = indexer_creator(
        embeddings=embeddings,
        collection_name=collection_name,
        connection_url=connection_url,
        **kwargs
    )
    result = vector_store.add_documents(documents)
    
    logger.debug(
        f"[load_indexer] Loaded vector store type: {type} with collection: {collection_name}"
    )
    return result



