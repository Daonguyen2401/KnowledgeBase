from rag.loading.build_loading import register_document_loader
# from langchain_docling import DoclingLoader
from langchain_community.document_loaders import PyPDFLoader
from langchain_core.documents import Document
from typing import List
import logging

logger = logging.getLogger(__name__)

# @register_document_loader("pdf_docling")
# def create_loader_docling_pdf(file_path: str,  **kwargs)-> List[Document]:
#     loader = DoclingLoader(file_path, **kwargs)
#     documents = loader.load()
#     logger.debug(f"[create_loader_docling_pdf] Loaded {len(documents)} documents from {file_path}")
#     return documents


@register_document_loader("pdf_pypdf")
def create_loader_pypdf_pdf(file_path: str,  **kwargs)-> List[Document]:
    loader = PyPDFLoader(file_path, **kwargs)
    documents = loader.load()
    logger.debug(f"[create_loader_pypdf_pdf] Loaded {len(documents)} documents from {file_path}")
    return documents


if __name__ == "__main__":
    """
    Quick manual test for PDF loaders.
    
    Edit the sample dict below with your PDF file path, then run:
        python pdf_loader.py
    """
    import sys
    from pathlib import Path
    
    # Sample configuration dict
    sample_config = {
        "file_path": "C:/Users/nguyentdd2/Desktop/MSB/Lab/KnowledgeBaseStack/experiment/BankRelationships.pdf",  # Relative path to sample PDF
        # Alternative: use absolute path
        # "file_path": "/path/to/your/file.pdf",
    }
    
    # Resolve relative path to absolute path
    script_dir = Path(__file__).parent
    pdf_path = (script_dir / sample_config["file_path"]).resolve()
    
    # Check if file exists
    if not pdf_path.exists():
        print(f"ERROR: PDF file not found at: {pdf_path}")
        print(f"Please update 'file_path' in sample_config to point to a valid PDF file.")
        sys.exit(1)
    
    print(f"Testing PDF loaders with file: {pdf_path}")
    print("=" * 60)
    
    # Test pdf_pypdf loader
    print("/n[1] Testing pdf_pypdf loader...")
    try:
        documents_pypdf = create_loader_pypdf_pdf(str(pdf_path))
        print(f"✓ Successfully loaded {len(documents_pypdf)} documents using PyPDFLoader")
        if documents_pypdf:
            print(f"  First document preview (first 200 chars):")
            print(f"  {documents_pypdf[0].page_content[:200]}...")
    except Exception as e:
        print(f"✗ Error loading with PyPDFLoader: {e}")
    
    # # Test pdf_docling loader
    # print("/n[2] Testing pdf_docling loader...")
    # try:
    #     documents_docling = create_loader_docling_pdf(str(pdf_path))
    #     print(f"✓ Successfully loaded {len(documents_docling)} documents using DoclingLoader")
    #     if documents_docling:
    #         print(f"  First document preview (first 200 chars):")
    #         print(f"  {documents_docling[0].page_content[:200]}...")
    # except Exception as e:
    #     print(f"✗ Error loading with DoclingLoader: {e}")
    #     print(f"  Note: DoclingLoader may require additional dependencies")
    
    # print("/n" + "=" * 60)
    # print("Test completed!")
