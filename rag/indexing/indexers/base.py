from abc import ABC, abstractmethod
from typing import List, Any
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from typing import Dict, Any

class BaseIndexer(ABC):
    """Abstract base class for vector store indexer implementations
    
    This class provides a unified interface for all vector store implementations,
    hiding the differences in initialization and usage between different vector stores.
    """
    
    @abstractmethod
    def __init__(
        self,
        embedding_model: Embeddings,
        collection_name: str,
        connection_url: str,
        **kwargs
    ):
        """Initialize the vector store indexer
        
        Args:
            embedding_model: Embeddings instance to use for vectorization
            collection_name: Name of the collection/table in the vector store
            connection_url: Connection string/URL for the database/storage
            **kwargs: Additional arguments for specific vector store implementations
        """
        raise NotImplementedError("Subclasses must implement __init__")
    
    @abstractmethod
    def add_documents(self, documents: List[Document]) -> List[str]:
        """Add documents to the vector store
        
        Args:
            documents: List of Document objects to add
            
        Returns:
            List of document IDs that were added
        """
        raise NotImplementedError("Subclasses must implement add_documents")
    
    @abstractmethod
    def delete_by_ids(self, ids: List[str]) -> None:
        """Delete documents from the vector store
        
        Args:
            ids: List of document IDs to delete
        """
        raise NotImplementedError("Subclasses must implement delete_by_ids")
    
    @abstractmethod
    def delete_by_metadata(self, metadata_filter: Dict[str, Any], k: int = 10000) -> None:
        """Delete documents from the vector store by metadata
        
        Args:
            metadata_filter: Dictionary of metadata to filter by
            k: Number of documents to return
        """
        raise NotImplementedError("Subclasses must implement delete_by_metadata")
    
    @abstractmethod
    def as_retriever(self, **kwargs: Any):
        """Return a retriever interface for the vector store
        
        Args:
            **kwargs: Additional arguments for the retriever
            
        Returns:
            A retriever instance
        """
        raise NotImplementedError("Subclasses must implement as_retriever")
    
    @abstractmethod
    def similarity_search(self, query: str, k: int = 4, **kwargs: Any) -> List[Document]:
        """Search for similar documents
        
        Args:
            query: Query string
            k: Number of documents to return
            **kwargs: Additional search parameters
            
        Returns:
            List of similar documents
        """
        raise NotImplementedError("Subclasses must implement similarity_search")
    
    @abstractmethod
    def similarity_search_with_score(
        self, query: str, k: int = 4, **kwargs: Any
    ) -> List[tuple[Document, float]]:
        """Search for similar documents with similarity scores
        
        Args:
            query: Query string
            k: Number of documents to return
            **kwargs: Additional search parameters
            
        Returns:
            List of tuples containing (document, score)
        """
        raise NotImplementedError("Subclasses must implement similarity_search_with_score")
    
    @abstractmethod
    def delete_collection(self) -> None:
        """Delete the entire collection/table"""
        raise NotImplementedError("Subclasses must implement delete_collection")

