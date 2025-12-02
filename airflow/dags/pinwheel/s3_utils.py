"""
Utility helpers for downloading objects from S3 (or S3-compatible services)
and saving them to a local file path.

Expected structure of the ``s3_config`` argument (passed directly as a dict):

    {
        "aws_access_key_id": "YOUR_ACCESS_KEY",
        "aws_secret_access_key": "YOUR_SECRET_KEY",
        "endpoint_url": "http://localhost:9000"
    }

You now pass the **bucket name separately** to ``get_file_from_s3`` instead
of putting it inside ``s3_config``.
"""

from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError


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


if __name__ == "__main__":
    """
    Quick manual test without CLI arguments.

    Edit the sample dict and key below, then run:

        python s3_utils.py
    """

    sample_s3_config: Dict[str, Optional[str]] = {
        # TODO: fill in your real values or read from environment variables.
        # Only connection details are kept in this dict.
        "aws_access_key_id": "YOUR_ACCESS_KEY",
        "aws_secret_access_key": "YOUR_SECRET_KEY",
        "endpoint_url": "http://localhost:9000",
    }

    sample_bucket = "my-bucket-name"

    # Example object key inside the bucket
    sample_key = "path/to/file.pdf"

    downloaded_path = get_file_from_s3(
        s3_config=sample_s3_config,
        s3_bucket=sample_bucket,
        s3_file_name=sample_key,
        local_filename=None,  # or set to a specific local path
    )

    print(f"Downloaded to {downloaded_path}")
