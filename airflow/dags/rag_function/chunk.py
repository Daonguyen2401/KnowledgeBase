"""
Function to chunk documents.
"""
from typing import Union, List, Dict
from langchain_core.documents import Document
from pinwheel.helper import make_serializable
from rag.chunking.chunking_builder import chunk_documents


def get_chunking_config(
    type: str,
    **kwargs
) -> Dict:
    kwargs = make_serializable(kwargs)
    return {
        "type": type,
        **kwargs
    }


def chunk_documents_task(
    documents: Union[List[Document], str],
    type: str,
    **kwargs
) -> str:
    """
    Chunk/split documents using a registered text splitter.
    
    Args:
        documents: List of Document objects or serialized string from XCom
        type: Splitter type key (e.g., 'recursive_character')
        **kwargs: Extra params passed to the text splitter (e.g., chunk_size, chunk_overlap)
    
    Returns:
        Serialized string of Document objects after chunking (for XCom transmission)
    """
    return chunk_documents(documents=documents, type=type, **kwargs)

