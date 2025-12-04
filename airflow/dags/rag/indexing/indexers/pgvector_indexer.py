from build_indexing import register_vector_store
from langchain_postgres import PGVector
import logging

logger = logging.getLogger(__name__)


@register_vector_store("pgvector")
def create_pgvector_indexer(
    embedding_model,
    collection_name: str,
    connection_url: str,
    **kwargs
):
    """
    Create a PGVector vector store instance.

    Args:
        embeddings: Embeddings instance to use for vectorization.
        collection_name: Name of the collection/table in the vector store.
        connection: PostgreSQL connection string (e.g., "postgresql+psycopg://user:pass@host:port/dbname").
        use_jsonb: Whether to use JSONB for metadata storage. Default: True.
        **kwargs: Additional arguments passed to PGVector.

    Returns:
        PGVector instance ready to use.
    """
    vector_store = PGVector(
        embeddings=embedding_model,
        collection_name=collection_name,
        connection=connection_url,
        **kwargs
    )
    
    logger.debug(
        f"[create_pgvector_indexer] Created PGVector store with collection: {collection_name}, "

    )
    
    return vector_store

