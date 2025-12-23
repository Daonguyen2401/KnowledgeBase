
import logging
from typing import Dict, Optional, List, Union

from langchain_core.documents import Document
from langchain_core.load import dumps, loads
from rag.loading.build_loading import DOCUMENT_LOADERS
from rag.common.utils import filter_kwargs, get_file_from_s3
logger = logging.getLogger(__name__)

def get_document_loader(type: str, **kwargs):
    document_loader = DOCUMENT_LOADERS.get(type)
    if not document_loader:
        raise ValueError(f"Document loader not found: {type}")
    return document_loader


def load_documents(
    s3_config: Dict[str, Optional[str]],
    s3_bucket: str,
    s3_file_path: str,
    type: str,
    **kwargs,
):
    """
    High-level helper to:
      1) Download a file from S3 (or S3-compatible, e.g. MinIO), then
      2) Load it using a registered document loader.
      3) Replace source metadata from local_file_path to s3_file_path.

    Args:
        s3_config: Dict with connection info, e.g.:
            {
                "aws_access_key_id": "YOUR_ACCESS_KEY",
                "aws_secret_access_key": "YOUR_SECRET_KEY",
                "endpoint_url": "http://localhost:9000",
            }
        s3_bucket: Name of the S3 bucket.
        s3_file_path: Object key inside ``s3_bucket``.
        type: Loader type key registered via ``register_document_loader``.
        **kwargs: Extra params passed to the concrete loader.
    """
    filepath = get_file_from_s3(s3_config, s3_bucket, s3_file_path)

    document_loader = get_document_loader(type)
    # Lọc kwargs theo chữ ký của loader (function/class), tránh mất các tham số custom
    kwargs = filter_kwargs(document_loader, kwargs)
    documents = document_loader(filepath, **kwargs)
    
    # Thay thế source trong metadata từ local_file_path thành s3_file_path
    # Overwrite source metadata với s3_file_path cho tất cả documents
    for doc in documents:
        if doc.metadata is None:
            doc.metadata = {}
        doc.metadata["source"] = s3_file_path
    
    logger.debug(
        f"[load_documents] Updated source metadata for {len(documents)} documents "
        f"to S3 path: {s3_file_path}"
    )
    
    # Serialize Document objects for XCom transmission
    serialized_documents = dumps(documents)
    logger.debug(f"[load_documents] Serialized {len(documents)} documents for XCom")
    
    return serialized_documents
