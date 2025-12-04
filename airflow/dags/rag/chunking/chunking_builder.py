import logging
from typing import List, Optional

from langchain_core.documents import Document
from build_chunking import TEXT_SPLITTERS

logger = logging.getLogger(__name__)


def get_text_splitter(type: str):
    text_splitter = TEXT_SPLITTERS.get(type)
    if not text_splitter:
        raise ValueError(f"Text splitter not found: {type}")
    return text_splitter


def chunk_documents(
    documents: List[Document],
    type: str,
    **kwargs,
) -> List[Document]:
    """
    High-level helper to chunk/split documents using a registered text splitter.

    Args:
        documents: List of Document objects to be chunked.
        type: Splitter type key registered via ``register_text_splitter``.
        **kwargs: Extra params passed to the concrete splitter (e.g., chunk_size, chunk_overlap).

    Returns:
        List of Document objects after chunking.
    """
    text_splitter = get_text_splitter(type)
    chunks = text_splitter(documents, **kwargs)
    
    logger.debug(
        f"[chunk_documents] Split {len(documents)} documents into {len(chunks)} chunks"
    )
    return chunks

