import logging
from typing import Union, List, Dict

from langchain_core.documents import Document
from langchain_core.load import loads
from langchain_core.embeddings import Embeddings
from rag.indexing.build_indexing import VECTOR_STORES
from rag.common.utils import filter_kwargs
from rag.embedding.embedding_builder import create_embedder

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
    documents: Union[List[Document], str],
    embeddings,
    collection_name: str,
    connection_url: str,
    **kwargs,
):
    """
    High-level helper to load/create a vector store instance with parameters.

    Args:
        type: Vector store type key registered via ``register_vector_store``.
        documents: List of Document objects or serialized string from XCom.
        embeddings: Embeddings instance to use for vectorization.
        collection_name: Name of the collection/table in the vector store.
        connection_url: Connection string for the database/storage.
        **kwargs: Additional arguments passed to the vector store.

    Returns:
        Result from vector_store.add_documents().
    """
    # Deserialize if documents is a serialized string (from XCom)
    if isinstance(documents, str):
        documents = loads(documents)
        logger.debug(f"[index_documents] Deserialized documents from XCom")
    
    indexer_creator = get_indexer(type)
    # Filter kwargs based on the creator function signature
    # indexer_creator is a function, not a class, so we filter based on the function itself
    kwargs = filter_kwargs(indexer_creator, kwargs)
    vector_store = indexer_creator(
        embedding_model=embeddings,
        collection_name=collection_name,
        connection_url=connection_url,
        **kwargs
    )
    result = vector_store.add_documents(documents)
    
    logger.debug(
        f"[index_documents] Indexed documents to vector store type: {type} with collection: {collection_name}"
    )
    return result

def index_documents_with_embeddings_config(
    type: str,
    documents: Union[List[Document], str],
    embeddings_config: dict, # {type: str, model: str, api_key: str, **kwargs}
    collection_name: str,
    connection_url: str,
    **kwargs,
):
    embeddings = create_embedder(**embeddings_config)
    return index_documents(type, documents, embeddings, collection_name, connection_url, **kwargs)


def delete_documents_by_metadata(
    type: str,
    metadata_filter: Dict,
    collection_name: str,
    embeddings: Embeddings,
    connection_url: str,
    **kwargs,
):
    indexer_creator = get_indexer(type)
    # Filter kwargs based on the creator function signature
    # indexer_creator is a function, not a class, so we filter based on the function itself
    kwargs = filter_kwargs(indexer_creator, kwargs)
    indexer = indexer_creator(
        embedding_model=embeddings,
        collection_name=collection_name,
        connection_url=connection_url,
        **kwargs
    )
    result = indexer.delete_by_metadata(metadata_filter=metadata_filter)
    logger.debug(
        f"[delete_documents_by_metadata] Deleted documents from vector store type: {type} with collection: {collection_name}"
    )
    return result