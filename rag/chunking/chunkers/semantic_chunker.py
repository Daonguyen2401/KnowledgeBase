import logging
import os
from typing import List, Optional

from langchain_core.documents import Document
from langchain_experimental.text_splitter import SemanticChunker
from rag.chunking.build_chunking import register_text_splitter
from rag.embedding.embedding_builder import create_embedder

logger = logging.getLogger(__name__)


@register_text_splitter("semantic")
def create_semantic_splitter(
    documents: List[Document],
    # Cấu hình embedder: ưu tiên dùng registry để tái sử dụng
    embeddings_config: dict = {'type': 'local-gamma', 'model': 'local-sentence-transformer', 'api_key': ''}, # {type: str, model: str, api_key: str, **kwargs}
    # Ngưỡng chia đoạn theo semantic
    breakpoint_threshold_type: str = "percentile",  # "percentile" | "standard_deviation" | "interquartile"
    breakpoint_threshold_amount: int = 95,
    **kwargs,
) -> List[Document]:
    """
    Semantic chunking sử dụng LangChain SemanticChunker.

    Args:
        documents: Danh sách LangChain Document.
        embeddings_config: cấu hình embedder lấy từ registry (vd: "openai").
        breakpoint_threshold_type/amount: tham số ngưỡng cho SemanticChunker.
        **kwargs: Tham số bổ sung truyền vào SemanticChunker.

    Returns:
        List[Document] sau khi tách theo ngữ nghĩa.
    """
    embeddings = create_embedder(**embeddings_config)

    semantic_splitter = SemanticChunker(
        embeddings=embeddings,
        breakpoint_threshold_type=breakpoint_threshold_type,
        breakpoint_threshold_amount=breakpoint_threshold_amount,
        **kwargs,
    )

    chunks = semantic_splitter.split_documents(documents)

    logger.debug(
        "[create_semantic_splitter] Split %d documents into %d semantic chunks "
        "with threshold_type=%s, threshold_amount=%s",
        len(documents),
        len(chunks),
        breakpoint_threshold_type,
        breakpoint_threshold_amount,
    )

    return chunks