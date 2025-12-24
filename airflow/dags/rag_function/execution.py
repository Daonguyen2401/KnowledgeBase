from rag.indexing.indexing_builder import delete_documents_by_metadata
from rag.scripts.run_basic import run_indexing_pipeline as _run_indexing_pipeline
from pinwheel.database_utils import parse_pgvector_connection_url
from typing import Dict, List, Any
from rag.embedding.embedding_builder import create_embedder
from pinwheel.helper import parse_xcom_value

def execute_delete_documents_by_metadata(
    conn_id: str,
    type: str,
    embeddings_config: Any,
    query_result: Any,
    collection_name: str,
    **kwargs
):
    """
    Xóa documents từ PGVector dựa trên kết quả từ query_new_documents.
    
    Args:
        conn_id: Airflow connection ID for pgvector
        type: Vector store type (e.g., 'pgvector')
        embeddings_config: Dict hoặc string (JSON hoặc Python dict representation) chứa config cho embedding model
        query_result: Dict hoặc string (JSON hoặc Python dict representation) từ query_new_documents với format:
            {
                'delete': [{'bucket_name': '...', 'file_path': '...'}, ...],
                'update': [{'bucket_name': '...', 'file_path': '...'}, ...],
                'create': [...],
                'keep': [...]
            }
        collection_name: Name of the collection/table in the vector store
        **kwargs: Additional arguments passed to delete_documents_by_metadata
    """
    # Parse query_result từ XCom
    query_result = parse_xcom_value(query_result, param_name="query_result")
    print(f"[execute_delete_documents_by_metadata] query_result sau khi xử lý: {query_result}")
    
    # Parse embeddings_config từ XCom
    embeddings_config = parse_xcom_value(embeddings_config, param_name="embeddings_config")
    print(f"[execute_delete_documents_by_metadata] embeddings_config sau khi xử lý: {embeddings_config}")
    
    # Lấy danh sách files từ 'delete' và 'update'
    delete_files = query_result.get('delete', [])
    update_files = query_result.get('update', [])
    
    print(f"[execute_delete_documents_by_metadata] delete_files: {delete_files}")
    print(f"[execute_delete_documents_by_metadata] update_files: {update_files}")
    
    # Tổng hợp tất cả file_path cần xóa
    file_paths = []
    for file_info in delete_files + update_files:
        file_path = file_info.get('file_path')
        if file_path:
            file_paths.append(file_path)
    
    if not file_paths:
        print("[execute_delete_documents_by_metadata] Không có file nào cần xóa")
        return {"status": "skipped", "deleted_files": []}
    
    print(f"[execute_delete_documents_by_metadata] Tìm thấy {len(file_paths)} file(s) cần xóa")
    
    connection_url = parse_pgvector_connection_url(conn_id)
    deleted_files = []
    failed_files = []
    
    # Xóa từng file_path
    for file_path in file_paths:
        try:
            metadata_filter = {"source": file_path}
            print(f"[execute_delete_documents_by_metadata] Đang xóa documents với source: {file_path}")
            
            # Tạo embedder từ config
            embeddings = create_embedder(**embeddings_config)
            delete_documents_by_metadata(
                type=type,
                metadata_filter=metadata_filter,
                embeddings=embeddings,
                collection_name=collection_name,
                connection_url=connection_url,
                **kwargs
            )
            
            print(f"[execute_delete_documents_by_metadata] Đã xóa thành công documents với source: {file_path}")
            deleted_files.append(file_path)
        except Exception as e:
            print(f"[execute_delete_documents_by_metadata] Lỗi khi xóa documents với source '{file_path}': {e}")
            failed_files.append({"file_path": file_path, "error": str(e)})
    
    return {
        "status": "success" if not failed_files else "partial_success",
        "deleted_files": deleted_files,
        "failed_files": failed_files,
        "total_files": len(file_paths)
    }


def run_indexing_pipeline(
    loading_config: Any,
    chunking_config: Any,
    embedding_config: Any,
    indexing_config: Any,
    query_result: Any,
):
    """
    Chạy Indexing Pipeline cho danh sách các file paths từ query_result.
    
    Wrapper function để parse XCom values và gọi hàm từ rag.scripts.run_basic.
    Pipeline thực hiện: Load -> Chunk -> Index cho mỗi file.
    
    Args:
        loading_config: Dict hoặc string (JSON/Python dict) chứa config cho loading
        chunking_config: Dict hoặc string (JSON/Python dict) chứa config cho chunking
        embedding_config: Dict hoặc string (JSON/Python dict) chứa config cho embeddings
        indexing_config: Dict hoặc string (JSON/Python dict) chứa config cho indexing
        query_result: Dict hoặc string (JSON/Python dict) từ query_new_documents với format:
            {
                'delete': [{'bucket_name': '...', 'file_path': '...'}, ...],
                'update': [{'bucket_name': '...', 'file_path': '...'}, ...],
                'create': [{'bucket_name': '...', 'file_path': '...'}, ...],
                'keep': [...]
            }
    
    Returns:
        Dict[str, Any] - Kết quả cho mỗi file với key là file_path và value là kết quả indexing
    """
    # Parse các config từ XCom nếu cần
    loading_config = parse_xcom_value(loading_config, param_name="loading_config")
    chunking_config = parse_xcom_value(chunking_config, param_name="chunking_config")
    embedding_config = parse_xcom_value(embedding_config, param_name="embedding_config")
    indexing_config = parse_xcom_value(indexing_config, param_name="indexing_config")
    query_result = parse_xcom_value(query_result, param_name="query_result")
    
    print(f"[run_indexing_pipeline] query_result sau khi xử lý: {query_result}")
    
    # Lấy danh sách files từ 'update' và 'create'
    update_files = query_result.get('update', [])
    create_files = query_result.get('create', [])
    
    print(f"[run_indexing_pipeline] update_files: {update_files}")
    print(f"[run_indexing_pipeline] create_files: {create_files}")
    
    # Tổng hợp tất cả file_path cần index
    file_paths = []
    for file_info in update_files + create_files:
        file_path = file_info.get('file_path')
        if file_path:
            file_paths.append(file_path)
    
    if not file_paths:
        print("[run_indexing_pipeline] Không có file nào cần index")
        return {"status": "skipped", "indexed_files": []}
    
    print(f"[run_indexing_pipeline] Tìm thấy {len(file_paths)} file(s) cần index")
    
    # Gọi hàm từ run_basic.py
    return _run_indexing_pipeline(
        loading_config=loading_config,
        chunking_config=chunking_config,
        embedding_config=embedding_config,
        indexing_config=indexing_config,
        file_paths=file_paths,
    )

