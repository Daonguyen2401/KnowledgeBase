from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class EmbedderConfig(BaseModel):
    """Configuration for the embedding model."""
    type: str = "openai"
    model: str = "text-embedding-3-small"
    api_key: Optional[str] = ""
    extra: Dict[str, Any] = {}


class SimilaritySearchRequest(BaseModel):
    query: str
    k: int = 4

    # Vector store config
    vector_store_type: str = "pgvector"
    collection_name: str
    connection_url: str

    # Embedder config (nested)
    embedder: EmbedderConfig = EmbedderConfig()

    # Extra kwargs forwarded to the vector store constructor
    vector_store_extra: Dict[str, Any] = {}


class DocumentResponse(BaseModel):
    page_content: str
    metadata: Dict[str, Any] = {}


class DocumentWithScoreResponse(BaseModel):
    document: DocumentResponse
    score: float


class SimilaritySearchResponse(BaseModel):
    results: List[DocumentResponse]


class SimilaritySearchWithScoreResponse(BaseModel):
    results: List[DocumentWithScoreResponse]


