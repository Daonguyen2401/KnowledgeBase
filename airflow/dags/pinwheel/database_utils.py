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
    

def get_postgres_conn_info(conn_id: str):
    """
    Get Postgres connection info from Airflow Connection ID.
    
    Args:
        conn_id (str): Airflow connection ID (Postgres type)
    
    Returns:
        dict: {
            "host": ...,
            "port": ...,
            "user": ...,
            "password": ...,
            "database": ...
        }
    """
    conn = BaseHook.get_connection(conn_id)
    return {
        "host": conn.host,
        "port": conn.port,
        "user": conn.login,
        "password": conn.password,
        "database": conn.schema,
    }

def parse_pgvector_connection_url(conn_id: str) -> str:
    """
    Parse Postgres connection info into a PostgreSQL connection URL.
    
    Args:
        conn_info (dict): Connection info containing host, port, user, password, and database.
    
    Returns:
        str: PostgreSQL connection URL in the format:
            postgresql+psycopg://user:pass@host:port/dbname
    """
    conn_info = get_postgres_conn_info(conn_id)
    return f"postgresql+psycopg://{conn_info['user']}:{conn_info['password']}@{conn_info['host']}:{conn_info['port']}/{conn_info['database']}"

def parse_qdrant_connection_url(conn_id: str) -> str:
    """
    Parse Qdrant connection info into a Qdrant connection URL.
    
    Args:
        conn_id (str): Airflow connection ID (Qdrant type)
    
    Returns:
        str: Qdrant connection URL in the format:
            host:port
    """
    conn_info = get_postgres_conn_info(conn_id)
    return f"{conn_info['host']}:{conn_info['port']}"