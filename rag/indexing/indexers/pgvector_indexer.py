from typing import List, Any
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_postgres import PGVector
from typing import Dict, Any
import logging

from .base import BaseIndexer
from rag.indexing.build_indexing import register_vector_store

logger = logging.getLogger(__name__)


class PGVectorIndexer(BaseIndexer):
    """PGVector indexer implementation with unified interface"""
    
    def __init__(
        self,
        embedding_model: Embeddings,
        collection_name: str,
        connection_url: str,
        **kwargs
    ):
        """Initialize PGVector indexer
        
        Args:
            embedding_model: Embeddings instance to use for vectorization
            collection_name: Name of the collection/table in the vector store
            connection_url: PostgreSQL connection string (e.g., "postgresql+psycopg://user:pass@host:port/dbname")
            **kwargs: Additional arguments passed to PGVector (e.g., use_jsonb=True)
        """
        self._store = PGVector(
            embeddings=embedding_model,
            collection_name=collection_name,
            connection=connection_url,
            **kwargs
        )
        logger.debug(
            f"[PGVectorIndexer] Created PGVector store with collection: {collection_name}"
        )
    
    def add_documents(self, documents: List[Document]) -> List[str]:
        """Add documents to PGVector"""
        return self._store.add_documents(documents)
    
    def delete_by_ids(self, ids: List[str]) -> None:
        """Delete documents from PGVector"""
        self._store.delete(ids)
    
    def delete_by_metadata(self, metadata_filter: Dict[str, Any], k: int = 100000) -> None:
        """
        Delete documents from PGVector by metadata.
        
        Args:
            metadata_filter: Dict chứa metadata filters, ví dụ: {"source": "document.pdf"}
            k: Số lượng documents tối đa để tìm và xóa
        """
        # Use similarity_search_with_score to get documents with IDs
        # Note: similarity_search may not return IDs, so we use similarity_search_with_score
        # and extract IDs from the results
        docs_with_scores = self._store.similarity_search_with_score(
            query="*",
            k=k,
            filter=metadata_filter,
        )
        
        # Extract document IDs
        # similarity_search_with_score returns List[tuple[Document, float]]
        ids = []
        for doc, score in docs_with_scores:
            # Document ID is typically stored in metadata or as an attribute
            if hasattr(doc, 'id'):
                ids.append(doc.id)
            elif hasattr(doc, 'metadata') and 'id' in doc.metadata:
                ids.append(doc.metadata['id'])
            elif hasattr(doc, 'metadata') and 'uuid' in doc.metadata:
                ids.append(doc.metadata['uuid'])
        
        if ids:
            logger.info(f"[PGVectorIndexer] Found {len(ids)} documents to delete by metadata filter: {metadata_filter}")
            self.delete_by_ids(ids=ids)
        else:
            logger.warning(f"[PGVectorIndexer] No documents found matching metadata filter: {metadata_filter}")
    
    def as_retriever(self, **kwargs: Any):
        """Return a retriever interface"""
        return self._store.as_retriever(**kwargs)
    
    def similarity_search(self, query: str, k: int = 4, **kwargs: Any) -> List[Document]:
        """Search for similar documents in PGVector"""
        return self._store.similarity_search(query, k=k, **kwargs)
    
    def similarity_search_with_score(
        self, query: str, k: int = 4, **kwargs: Any
    ) -> List[tuple[Document, float]]:
        """Search for similar documents in PGVector with score"""
        return self._store.similarity_search_with_score(query, k=k, **kwargs)
    
    def delete_collection(self) -> None:
        """Delete the entire collection"""
        # PGVector doesn't have a direct delete_collection method
        # This would need to be implemented via direct SQL if needed
        raise NotImplementedError("delete_collection not implemented for PGVector")


@register_vector_store("pgvector")
def create_pgvector_indexer(
    embedding_model: Embeddings,
    collection_name: str,
    connection_url: str,
    **kwargs
) -> PGVectorIndexer:
    """
    Factory function to create a PGVector indexer instance.
    
    This function is registered with the vector store registry and provides
    a unified interface for creating PGVector indexers.

    Args:
        embedding_model: Embeddings instance to use for vectorization
        collection_name: Name of the collection/table in the vector store
        connection_url: PostgreSQL connection string
        **kwargs: Additional arguments passed to PGVector

    Returns:
        PGVectorIndexer instance with unified interface
    """
    return PGVectorIndexer(
        embedding_model=embedding_model,
        collection_name=collection_name,
        connection_url=connection_url,
        **kwargs
    )

