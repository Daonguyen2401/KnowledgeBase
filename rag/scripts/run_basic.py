"""
Script để chạy Indexing Pipeline với các config và danh sách file paths.
Tham khảo các hàm trong demo_dag.py nhưng sử dụng trực tiếp các hàm RAG cơ bản.
"""
import logging
from typing import Dict, List, Optional

from rag.loading.loading_builder import load_documents
from rag.chunking.chunking_builder import chunk_documents
from rag.indexing.indexing_builder import index_documents_with_embeddings_config

logger = logging.getLogger(__name__)


def run_indexing_pipeline(
    loading_config: Dict,
    chunking_config: Dict,
    embedding_config: Dict,
    indexing_config: Dict,
    file_paths: List[str],
):
    """
    Chạy Indexing Pipeline cho danh sách các file paths.
    
    Pipeline thực hiện: Load -> Chunk -> Index cho mỗi file.
    
    Args:
        loading_config: Dict chứa config cho loading, ví dụ:
            {
                "s3_config": {
                    "aws_access_key_id": "YOUR_ACCESS_KEY",
                    "aws_secret_access_key": "YOUR_SECRET_KEY",
                    "endpoint_url": "http://localhost:9000",  # optional
                    "region_name": "us-east-1",  # optional
                },
                "s3_bucket": "demo-rag",
                "type": "pdf_pypdf",
                # Các tham số khác cho document loader
            }
        chunking_config: Dict chứa config cho chunking, ví dụ:
            {
                "type": "recursive_character",
                "chunk_size": 1000,  # optional
                "chunk_overlap": 200,  # optional
                # Các tham số khác cho text splitter
            }
        embedding_config: Dict chứa config cho embeddings, ví dụ:
            {
                "type": "openai",
                "model": "text-embedding-3-small",
                "api_key": "your-api-key",
                # Các tham số khác cho embedder
            }
        indexing_config: Dict chứa config cho indexing, ví dụ:
            {
                "type": "pgvector",
                "connection_url": "postgresql://user:password@localhost:5432/dbname",
                "collection_name": "demo_rag",
                # Các tham số khác cho vector store
            }
        file_paths: List[str] - Danh sách các S3 file paths (object keys) cần index
    
    Returns:
        Dict[str, Any] - Kết quả cho mỗi file với key là file_path và value là kết quả indexing
    """
    results = {}
    
    # Validate required config fields
    required_loading_fields = ["s3_config", "s3_bucket", "type"]
    for field in required_loading_fields:
        if field not in loading_config:
            raise ValueError(f"loading_config thiếu field bắt buộc: {field}")
    
    if "type" not in chunking_config:
        raise ValueError("chunking_config thiếu field bắt buộc: type")
    
    required_embedding_fields = ["type"]
    for field in required_embedding_fields:
        if field not in embedding_config:
            raise ValueError(f"embedding_config thiếu field bắt buộc: {field}")
    
    required_indexing_fields = ["type", "connection_url", "collection_name"]
    for field in required_indexing_fields:
        if field not in indexing_config:
            raise ValueError(f"indexing_config thiếu field bắt buộc: {field}")
    
    # Xử lý từng file
    for file_path in file_paths:
        logger.info(f"[run_indexing_pipeline] Bắt đầu xử lý file: {file_path}")
        
        try:
            # Bước 1: Load documents
            logger.info(f"[run_indexing_pipeline] Loading documents từ {file_path}")
            s3_config = loading_config["s3_config"]
            s3_bucket = loading_config["s3_bucket"]
            loader_type = loading_config["type"]
            loader_kwargs = {k: v for k, v in loading_config.items() 
                           if k not in ["s3_config", "s3_bucket", "type"]}
            
            serialized_documents = load_documents(
                s3_config=s3_config,
                s3_bucket=s3_bucket,
                s3_file_path=file_path,
                type=loader_type,
                **loader_kwargs
            )
            logger.info(f"[run_indexing_pipeline] Đã load documents từ {file_path}")
            
            # Bước 2: Chunk documents
            logger.info(f"[run_indexing_pipeline] Chunking documents từ {file_path}")
            chunker_type = chunking_config["type"]
            chunker_kwargs = {k: v for k, v in chunking_config.items() 
                            if k != "type"}
            
            serialized_chunks = chunk_documents(
                documents=serialized_documents,
                type=chunker_type,
                **chunker_kwargs
            )
            logger.info(f"[run_indexing_pipeline] Đã chunk documents từ {file_path}")
            
            # Bước 3: Index documents
            logger.info(f"[run_indexing_pipeline] Indexing documents từ {file_path}")
            indexer_type = indexing_config["type"]
            connection_url = indexing_config["connection_url"]
            collection_name = indexing_config["collection_name"]
            indexer_kwargs = {k: v for k, v in indexing_config.items() 
                            if k not in ["type", "connection_url", "collection_name"]}
            
            indexing_result = index_documents_with_embeddings_config(
                type=indexer_type,
                documents=serialized_chunks,
                embeddings_config=embedding_config,
                collection_name=collection_name,
                connection_url=connection_url,
                **indexer_kwargs
            )
            logger.info(f"[run_indexing_pipeline] Đã index documents từ {file_path}")
            
            results[file_path] = {
                "status": "success",
                "result": indexing_result
            }
            
        except Exception as e:
            logger.error(f"[run_indexing_pipeline] Lỗi khi xử lý file {file_path}: {str(e)}")
            results[file_path] = {
                "status": "error",
                "error": str(e)
            }
    
    logger.info(f"[run_indexing_pipeline] Hoàn thành xử lý {len(file_paths)} file(s)")
    return results

