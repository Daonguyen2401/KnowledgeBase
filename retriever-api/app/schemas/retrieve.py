from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class SimilaritySearchRequest(BaseModel):
    query: str
    k: int = 4

    # Vector store config
    vector_store_type: str = "pgvector"
    collection_name: str
    connection_url: str

    # Embedder config
    embedder_type: str = "local-gamma"
    embedder_model: str = "local-sentence-transformer"
    embedder_api_key: Optional[str] = ""
    embedder_extra: Dict[str, Any] = {}

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


