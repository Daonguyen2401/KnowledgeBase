
import logging
from typing import Dict, Optional

from pinwheel.s3_utils import get_file_from_s3
from rag.loading.build_loading import DOCUMENT_LOADERS
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
    return document_loader(filepath, **kwargs)
