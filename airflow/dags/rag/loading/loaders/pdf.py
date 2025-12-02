from loading_builder import register_document_loader
from langchain_docling import DoclingLoader
from langchain_community.document_loaders import PyPDFLoader
from langchain_core.documents import Document
from typing import List
import logging

logger = logging.getLogger(__name__)

@register_document_loader("pdf_docling")
def create_loader_docling_pdf(file_path: str,  **kwargs)-> List[Document]:
    loader = DoclingLoader(file_path, **kwargs)
    documents = loader.load()
    logger.debug(f"[create_loader_docling_pdf] Loaded {len(documents)} documents from {file_path}")
    return documents


@register_document_loader("pdf_pypdf")
def create_loader_pypdf_pdf(file_path: str,  **kwargs)-> List[Document]:
    loader = PyPDFLoader(file_path, **kwargs)
    documents = loader.load()
    logger.debug(f"[create_loader_pypdf_pdf] Loaded {len(documents)} documents from {file_path}")
    return documents



