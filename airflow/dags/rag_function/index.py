"""
Function to index documents with embeddings.
Combines getting pgvector connection and indexing documents into one function.
"""
from typing import Union, List, Dict
from langchain_core.documents import Document
from pinwheel.database_utils import parse_pgvector_connection_url
from pinwheel.helper import make_serializable
from rag.indexing.indexing_builder import index_documents_with_embeddings_config, get_indexer


def get_indexing_config(
    conn_id: str,
    collection_name: str,
    type: str = "pgvector",
    **kwargs
) -> Dict:
    kwargs = make_serializable(kwargs)
    connection_url = parse_pgvector_connection_url(conn_id)
    return {
        "type": type,
        "connection_url": connection_url,
        "collection_name": collection_name,
        **kwargs
    }


def index_documents_task(
    conn_id: str,
    documents: Union[List[Document], str],
    type: str,
    embeddings_config: Dict,
    collection_name: str,
    **kwargs
):
    """
    Index documents to pgvector using Airflow connection.
    
    This function combines:
    1. Getting pgvector connection URL from Airflow connection
    2. Indexing documents with embeddings
    
    Args:
        conn_id: Airflow connection ID for pgvector (e.g., 'pgvector_conn')
        documents: List of Document objects or serialized string from XCom
        type: Vector store type (e.g., 'pgvector')
        embeddings_config: Dict containing embeddings configuration, e.g.:
            {
                'type': 'openai',
                'model': 'text-embedding-3-small',
                'api_key': 'your-api-key',
                **kwargs
            }
        collection_name: Name of the collection/table in the vector store
        **kwargs: Additional arguments passed to the vector store
    
    Returns:
        Result from vector_store.add_documents()
    """
    # Get pgvector connection URL from Airflow connection
    connection_url = parse_pgvector_connection_url(conn_id)
    
    # Index documents with embeddings
    result = index_documents_with_embeddings_config(
        type=type,
        documents=documents,
        embeddings_config=embeddings_config,
        collection_name=collection_name,
        connection_url=connection_url,
        **kwargs
    )
    
    return result

