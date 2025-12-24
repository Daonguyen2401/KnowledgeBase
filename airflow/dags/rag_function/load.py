"""
Function to load documents from S3 using Airflow connection.
Combines getting S3 config and loading documents into one function.
"""
from typing import Dict, Optional
from pinwheel.s3_utils import get_s3_config_from_airflow_connection
from pinwheel.helper import make_serializable
from rag.loading.loading_builder import load_documents


def get_loading_config(
    conn_id: str,
    s3_bucket: str,
    type: str,
    **kwargs
) -> Dict:
    s3_config = get_s3_config_from_airflow_connection(conn_id)
    kwargs = make_serializable(kwargs)
    return {
        "s3_config": s3_config,
        "s3_bucket": s3_bucket,
        "type": type,
        **kwargs
    }


def load_documents_task(
    conn_id: str,
    s3_bucket: str,
    s3_file_path: str,
    type: str,
    **kwargs
):
    """
    Load documents from S3 using Airflow connection.
    
    This function combines:
    1. Getting S3 config from Airflow connection
    2. Loading documents from S3
    
    Args:
        conn_id: Airflow connection ID for S3 (e.g., 'minio_conn')
        s3_bucket: Name of the S3 bucket
        s3_file_path: Object key inside the bucket (e.g., 'BankDocument/BankRelationships.pdf')
        type: Document loader type (e.g., 'pdf_pypdf')
        **kwargs: Extra params passed to the document loader
    
    Returns:
        Serialized string of Document objects (for XCom transmission)
    """
    # Get S3 config from Airflow connection
    s3_config = get_s3_config_from_airflow_connection(conn_id)
    
    # Load documents from S3
    serialized_documents = load_documents(
        s3_config=s3_config,
        s3_bucket=s3_bucket,
        s3_file_path=s3_file_path,
        type=type,
        **kwargs
    )
    
    return serialized_documents

