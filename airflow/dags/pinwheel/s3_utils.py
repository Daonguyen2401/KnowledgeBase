from typing import Dict, Optional

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


if __name__ == "__main__":
    """
    Quick manual test without CLI arguments.

    Edit the sample connection ID below, then run:

        python s3_utils.py
    """
    # Test get_s3_config_from_airflow_connection
    try:
        s3_config = get_s3_config_from_airflow_connection('minio_conn')
        print(f"S3 Config: {s3_config}")
    except Exception as e:
        print(f"Error: {e}")
