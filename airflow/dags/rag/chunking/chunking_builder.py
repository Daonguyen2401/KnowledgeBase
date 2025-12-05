import logging
from typing import List, Optional, Union

from langchain_core.documents import Document
from langchain_core.load import dumps, loads
from rag.chunking.build_chunking import TEXT_SPLITTERS
from rag.common.utils import filter_kwargs

logger = logging.getLogger(__name__)


def get_text_splitter(type: str):
    text_splitter = TEXT_SPLITTERS.get(type)
    if not text_splitter:
        raise ValueError(f"Text splitter not found: {type}")
    return text_splitter


def chunk_documents(
    documents: Union[List[Document], str],
    type: str,
    **kwargs,
) -> str:
    """
    High-level helper to chunk/split documents using a registered text splitter.

    Args:
        documents: List of Document objects or serialized string from XCom.
        type: Splitter type key registered via ``register_text_splitter``.
        **kwargs: Extra params passed to the concrete splitter (e.g., chunk_size, chunk_overlap).

    Returns:
        Serialized string of Document objects after chunking (for XCom transmission).
    """
    # Deserialize if documents is a serialized string (from XCom)
    if isinstance(documents, str):
        documents = loads(documents)
        logger.debug(f"[chunk_documents] Deserialized documents from XCom")
    
    text_splitter = get_text_splitter(type)
    kwargs = filter_kwargs(text_splitter.__init__, kwargs)
    chunks = text_splitter(documents, **kwargs)
    
    logger.debug(
        f"[chunk_documents] Split {len(documents)} documents into {len(chunks)} chunks"
    )
    
    # Serialize Document objects for XCom transmission
    serialized_chunks = dumps(chunks)
    logger.debug(f"[chunk_documents] Serialized {len(chunks)} chunks for XCom")
    
    return serialized_chunks

