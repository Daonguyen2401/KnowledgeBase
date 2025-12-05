from rag.chunking.build_chunking import register_text_splitter
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from typing import List
import logging

logger = logging.getLogger(__name__)


@register_text_splitter("recursive_character")
def create_recursive_character_splitter(
    documents: List[Document],
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
    add_start_index: bool = True,
    **kwargs
) -> List[Document]:
    """
    Split documents using RecursiveCharacterTextSplitter.

    Args:
        documents: List of Document objects to split.
        chunk_size: Maximum size of chunks (in characters). Can be passed via kwargs.
        chunk_overlap: Number of characters to overlap between chunks. Can be passed via kwargs.
        add_start_index: Whether to add start index metadata to chunks. Can be passed via kwargs.
        **kwargs: Additional arguments passed to RecursiveCharacterTextSplitter.
                  Can include chunk_size, chunk_overlap, add_start_index to override defaults.

    Returns:
        List of Document objects after splitting.
    """
    # Ưu tiên lấy từ kwargs nếu có, nếu không thì dùng giá trị mặc định từ signature
    final_chunk_size = kwargs.pop('chunk_size', chunk_size)
    final_chunk_overlap = kwargs.pop('chunk_overlap', chunk_overlap)
    final_add_start_index = kwargs.pop('add_start_index', add_start_index)
    
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=final_chunk_size,
        chunk_overlap=final_chunk_overlap,
        add_start_index=final_add_start_index,
        **kwargs
    )
    
    all_splits = text_splitter.split_documents(documents)
    
    logger.debug(
        f"[create_recursive_character_splitter] Split {len(documents)} documents "
        f"into {len(all_splits)} sub-documents with chunk_size={final_chunk_size}, "
        f"chunk_overlap={final_chunk_overlap}"
    )
    
    return all_splits

