from fastapi import APIRouter, HTTPException

from app.schemas.retrieve_schema import (
    DocumentResponse,
    DocumentWithScoreResponse,
    SimilaritySearchRequest,
    SimilaritySearchResponse,
    SimilaritySearchWithScoreResponse,
)

from rag.embedding.embedding_builder import create_embedder as load_embedder
from rag.indexing.indexing_builder import get_indexer


router = APIRouter(prefix="/retrieve", tags=["retrieve"])


def _get_vector_store(req: SimilaritySearchRequest):
    """
    Helper to construct a vector store instance using the shared
    embedder/indexer builders defined in `rag/`.
    """
    try:
        embedder = load_embedder(
            type=req.embedder.type,
            model=req.embedder.model,
            api_key=req.embedder.api_key or "",
            **(req.embedder.extra or {}),
        )

        indexer_creator = get_indexer(req.vector_store_type)
        vector_store = indexer_creator(
            embedding_model=embedder,
            collection_name=req.collection_name,
            connection_url=req.connection_url,
            **(req.vector_store_extra or {}),
        )
        return vector_store
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to initialize vector store: {e}")


@router.post(
    "/similarity_search",
    response_model=SimilaritySearchResponse,
)
async def similarity_search(payload: SimilaritySearchRequest) -> SimilaritySearchResponse:
    """
    Retrieve similar documents from the configured vector store.
    """
    try:
        vector_store = _get_vector_store(payload)
        docs = vector_store.similarity_search(payload.query, k=payload.k)

        results = [
            DocumentResponse(page_content=doc.page_content, metadata=doc.metadata or {})
            for doc in docs
        ]
        return SimilaritySearchResponse(results=results)
    except HTTPException:
        # Re-raise HTTPExceptions such as initialization errors
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"similarity_search failed: {e}")


@router.post(
    "/similarity_search_with_score",
    response_model=SimilaritySearchWithScoreResponse,
)
async def similarity_search_with_score(
    payload: SimilaritySearchRequest,
) -> SimilaritySearchWithScoreResponse:
    """
    Retrieve similar documents along with their similarity scores.
    """
    try:
        vector_store = _get_vector_store(payload)
        docs_and_scores = vector_store.similarity_search_with_score(
            payload.query, k=payload.k
        )

        results = [
            DocumentWithScoreResponse(
                document=DocumentResponse(
                    page_content=doc.page_content,
                    metadata=doc.metadata or {},
                ),
                score=score,
            )
            for doc, score in docs_and_scores
        ]
        return SimilaritySearchWithScoreResponse(results=results)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"similarity_search_with_score failed: {e}",
        )

