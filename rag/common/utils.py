import inspect
from pathlib import Path
from typing import Dict, Optional

import boto3
from botocore.exceptions import ClientError


def filter_kwargs(func, kwargs):
    sig = inspect.signature(func)
    allowed_params = sig.parameters
    return {
        k: v for k, v in kwargs.items()
        if k in allowed_params
    }


def get_file_from_s3(
    s3_config: Dict[str, Optional[str]],
    s3_bucket: str,
    s3_file_name: str,
    *,
    local_filename: Optional[str] = None,
) -> str:
    """
    Download a file from S3 (or S3-compatible storage) and save it to a local path.

    Args:
        s3_config: Dict containing ONLY S3 connection info, for example:
            - aws_access_key_id
            - aws_secret_access_key
            - endpoint_url
          (You can optionally still add keys like ``aws_session_token`` or
          ``region_name`` if you need them.)
        s3_bucket: Name of the S3 bucket.
        s3_file_name: Object key inside the bucket (e.g. ``path/to/file.pdf``).
        local_filename: Optional explicit local filename; if None, the object's
                        basename is used in the current working directory.

    Returns:
        Absolute path to the downloaded file.
    """
    config = dict(s3_config or {})

    key = s3_file_name.lstrip("/")

    # Build session from config
    session_kwargs = {
        k: config.get(k)
        for k in (
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_session_token",
            "region_name",
        )
        if config.get(k)
    }
    session = boto3.session.Session(**session_kwargs)

    s3_client = session.client("s3", endpoint_url=config.get("endpoint_url"))

    # Determine local path: explicit path or just basename in current dir
    local_path = Path(local_filename) if local_filename else Path(Path(key).name)
    local_path = local_path.expanduser().resolve()

    # Ensure parent directory exists if a directory is specified
    if local_path.parent and str(local_path.parent) not in (".", ""):
        local_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        s3_client.download_file(s3_bucket, key, str(local_path))
    except ClientError as exc:
        raise RuntimeError(
            f"Failed to download 's3://{s3_bucket}/{key}' -> '{local_path}': {exc}"
        ) from exc

    return str(local_path)