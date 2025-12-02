
import logging
from typing import Dict, Optional

from pinwheel.s3_utils import get_file_from_s3

logger = logging.getLogger(__name__)

DOCUMENT_LOADERS = {}


def register_document_loader(type: str) -> None:
    def decorator(func):
        DOCUMENT_LOADERS[type] = func
        logger.debug(
            f"[register_document_loader] Registered document loader: {type} -> {func.__name__}"
        )
        return func

    return decorator


def get_document_loader(type: str, **kwargs):
    document_loader = DOCUMENT_LOADERS.get(type)
    if not document_loader:
        raise ValueError(f"Document loader not found: {type}")
    return document_loader


def load_documents(
    s3_config: Dict[str, Optional[str]],
    bucket: str,
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
        bucket: Name of the S3 bucket.
        s3_file_path: Object key inside ``bucket``.
        type: Loader type key registered via ``register_document_loader``.
        **kwargs: Extra params passed to the concrete loader.
    """
    filepath = get_file_from_s3(s3_config, bucket, s3_file_path)

    document_loader = get_document_loader(type)
    return document_loader(filepath, **kwargs)
