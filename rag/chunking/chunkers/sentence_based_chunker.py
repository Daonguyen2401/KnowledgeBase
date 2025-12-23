import logging
from typing import List

from langchain_core.documents import Document
from rag.chunking.build_chunking import register_text_splitter
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core import Document as LlamaDocument

logger = logging.getLogger(__name__)


@register_text_splitter("sentence_based")
def create_sentence_based_splitter(
    documents: List[Document],
    chunk_size: int = 1024,
    chunk_overlap: int = 20,
    **kwargs,
) -> List[Document]:
    """
    Split documents by sentences using LlamaIndex SentenceSplitter, but
    input/output dùng LangChain Document để thống nhất với pipeline.

    Args:
        documents: List[Document] (LangChain) to split.
        chunk_size: Max chars per chunk.
        chunk_overlap: Overlap chars between chunks.
        **kwargs: Extra args passed to SentenceSplitter (if any).

    Returns:
        List[Document] (LangChain) after sentence-based splitting.
    """
    splitter = SentenceSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        **kwargs,
    )

    all_nodes = []
    for doc in documents:
        llama_doc = LlamaDocument(text=doc.page_content, metadata=doc.metadata or {})
        nodes = splitter.get_nodes_from_documents([llama_doc])
        all_nodes.extend(nodes)

    # Convert back to LangChain Document
    lc_chunks = [
        Document(page_content=node.text, metadata=node.metadata) for node in all_nodes
    ]

    logger.debug(
        "[create_sentence_based_splitter] Split %d documents into %d chunks "
        "with chunk_size=%d, chunk_overlap=%d",
        len(documents),
        len(lc_chunks),
        chunk_size,
        chunk_overlap,
    )

    return lc_chunks