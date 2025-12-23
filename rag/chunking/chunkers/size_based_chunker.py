from rag.chunking.build_chunking import register_text_splitter
from langchain_text_splitters import CharacterTextSplitter
from langchain_text_splitters import TokenTextSplitter
from langchain_core.documents import Document
from typing import List
import logging

logger = logging.getLogger(__name__)

@register_text_splitter("size_based_character")
def create_size_based_character_splitter(
    documents: List[Document],
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
    separator: str = "\n",
    **kwargs
) -> List[Document]:
    """
    Split documents using CharacterTextSplitter.

    Args:
        documents: List of Document objects to split.
        chunk_size: Maximum size of chunks (in characters). Can be passed via kwargs.
        chunk_overlap: Number of characters to overlap between chunks. Can be passed via kwargs.
        separator: Separator to use for splitting. Can be passed via kwargs.
        **kwargs: Additional arguments passed to CharacterTextSplitter.
                  Can include chunk_size, chunk_overlap, separator to override defaults.

    Returns:
        List of Document objects after splitting.
    """
    # Ưu tiên lấy từ kwargs nếu có, nếu không thì dùng giá trị mặc định từ signature
    final_chunk_size = kwargs.pop('chunk_size', chunk_size)
    final_chunk_overlap = kwargs.pop('chunk_overlap', chunk_overlap)
    final_separator = kwargs.pop('separator', separator)    
    logger.info(f"final_chunk_size: {final_chunk_size}, final_chunk_overlap: {final_chunk_overlap}, final_separator: {final_separator}")
    
    text_splitter = CharacterTextSplitter(
        chunk_size=final_chunk_size,
        chunk_overlap=final_chunk_overlap,
        separator=final_separator,
        **kwargs
    )
    
    all_splits = text_splitter.split_documents(documents)
    
    logger.debug(
        f"[create_size_based_character_splitter] Split {len(documents)} documents "
        f"into {len(all_splits)} sub-documents with chunk_size={final_chunk_size}, "
        f"chunk_overlap={final_chunk_overlap}, separator={final_separator}"
    )
    
    return all_splits

# @register_text_splitter("size_based_token")
# def create_size_based_token_splitter(
#     documents: List[Document],
#     chunk_size: int = 512,
#     chunk_overlap: int = 50,
#     **kwargs
# ) -> List[Document]:
#     """
#     Split documents using TokenTextSplitter.
#     """
#     # Ưu tiên lấy từ kwargs nếu có, nếu không thì dùng giá trị mặc định từ signature
#     final_chunk_size = kwargs.pop('chunk_size', chunk_size)
#     final_chunk_overlap = kwargs.pop('chunk_overlap', chunk_overlap)
    
#     text_splitter = TokenTextSplitter(
#         chunk_size=final_chunk_size,
#         chunk_overlap=final_chunk_overlap,
#         **kwargs
#     )
    
#     all_splits = text_splitter.split_documents(documents)

#     logger.info(all_splits[0])
#     logger.debug(
#         f"[create_size_based_token_splitter] Split {len(documents)} documents "
#         f"into {len(all_splits)} sub-documents with chunk_size={final_chunk_size}, "
#         f"chunk_overlap={final_chunk_overlap}"
#     )

#     return all_splits