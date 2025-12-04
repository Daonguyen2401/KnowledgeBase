from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

try:
    from airflow.hooks.base import BaseHook
except ImportError:
    # For local testing without Airflow
    BaseHook = None


def get_s3_config_from_airflow_connection(conn_id: str) -> Dict[str, Optional[str]]:
    """
    Lấy thông tin AWS (ACCESS_KEY, SECRET_KEY, endpoint_url) từ Airflow Connection.

    Args:
        conn_id: Connection ID được định nghĩa trong Airflow Connections.

    Returns:
        Dict chứa thông tin S3 config với các keys:
            - aws_access_key_id
            - aws_secret_access_key
            - endpoint_url (nếu có trong extra)
            - region_name (nếu có trong extra)

    Raises:
        ImportError: Nếu không thể import BaseHook (khi chạy ngoài Airflow context).
        ValueError: Nếu connection không tồn tại hoặc thiếu thông tin cần thiết.

    Example:
        Trong Airflow Connection UI, tạo connection với:
        - Connection Id: 's3_connection'
        - Connection Type: 'Generic'
        - Login: 'your_access_key'
        - Password: 'your_secret_key'
        - Extra (JSON): {"endpoint_url": "http://localhost:9000", "region_name": "us-east-1"}

        Sau đó sử dụng:
        >>> s3_config = get_s3_config_from_airflow_connection('s3_connection')
        >>> s3_config
        {
            'aws_access_key_id': 'your_access_key',
            'aws_secret_access_key': 'your_secret_key',
            'endpoint_url': 'http://localhost:9000',
            'region_name': 'us-east-1'
        }
    """
    if BaseHook is None:
        raise ImportError(
            "BaseHook không thể import. Hàm này chỉ hoạt động trong Airflow context."
        )

    # Lấy connection từ Airflow
    connection = BaseHook.get_connection(conn_id)

    if not connection:
        raise ValueError(f"Connection '{conn_id}' không tồn tại.")

    # Parse extra nếu có (thường là JSON string)
    extra = {}
    if connection.extra:
        try:
            import json
            if isinstance(connection.extra, str):
                extra = json.loads(connection.extra)
            elif isinstance(connection.extra, dict):
                extra = connection.extra
        except (json.JSONDecodeError, TypeError):
            # Nếu không parse được JSON, giữ nguyên
            pass

    # Xây dựng config dict
    s3_config: Dict[str, Optional[str]] = {
        "aws_access_key_id": connection.login or extra.get("aws_access_key_id"),
        "aws_secret_access_key": connection.password or extra.get("aws_secret_access_key"),
    }

    # Thêm endpoint_url từ extra nếu có
    if "endpoint_url" in extra:
        s3_config["endpoint_url"] = extra["endpoint_url"]
    elif connection.host:
        # Nếu có host trong connection, có thể dùng làm endpoint_url
        scheme = connection.schema or "http"
        port = f":{connection.port}" if connection.port else ""
        s3_config["endpoint_url"] = f"{scheme}://{connection.host}{port}"

    # Thêm region_name từ extra nếu có
    if "region_name" in extra:
        s3_config["region_name"] = extra["region_name"]

    # Kiểm tra các thông tin bắt buộc
    if not s3_config.get("aws_access_key_id") or not s3_config.get("aws_secret_access_key"):
        raise ValueError(
            f"Connection '{conn_id}' thiếu thông tin ACCESS_KEY hoặc SECRET_KEY. "
            "Vui lòng kiểm tra Login và Password trong Airflow Connection."
        )

    return s3_config


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
        "aws_access_key_id": "demouseraccesskey",
        "aws_secret_access_key": "jV9vt2uREobOcNSWFaqZ3dWFmAc2nFPhu1mVlrvO",
        "endpoint_url": "http://localhost:9000",
    }

    sample_bucket = "demo-rag"

    sample_key = "Bankdocument/BankRelationships.pdf"

    downloaded_path = get_file_from_s3(
        s3_config=sample_s3_config,
        s3_bucket=sample_bucket,
        s3_file_name=sample_key,
        local_filename='./hehe.pdf',  # or set to a specific local path
    )

    print(f"Downloaded to {downloaded_path}")
