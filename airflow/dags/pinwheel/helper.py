from typing import Any, Dict
import json
import ast

def _make_serializable(value: Any) -> Any:
    """
    Chuyển đổi giá trị thành dạng có thể serialize qua XCom (JSON).
    Chỉ giữ lại: str, int, float, bool, None, dict, list.
    """
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {k: _make_serializable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_make_serializable(v) for v in value]
    # Nếu không thể serialize, chuyển thành string
    return str(value)


def make_serializable(value: Any) -> Any:
    return _make_serializable(value)


def parse_xcom_value(value: Any, param_name: str = "value") -> Dict:
    """
    Parse giá trị từ XCom (có thể là string hoặc dict) thành dict.
    
    Khi dùng template string "{{ ti.xcom_pull(...) }}" trong Airflow, giá trị có thể được serialize thành:
    - JSON string (double quotes): '{"key": "value"}'
    - Python dict representation (single quotes): "{'key': 'value'}"
    - Dict object trực tiếp
    
    Args:
        value: Giá trị từ XCom (có thể là string hoặc dict)
        param_name: Tên parameter để hiển thị trong error message
    
    Returns:
        Dict sau khi parse
    
    Raises:
        ValueError: Nếu value là None hoặc không thể parse
        TypeError: Nếu value không phải dict sau khi parse
    """
    if value is None:
        raise ValueError(f"{param_name} không được None")
    
    # Nếu đã là dict, return luôn
    if isinstance(value, dict):
        return value
    
    # Nếu là string, cần parse
    if isinstance(value, str):
        try:
            # Thử parse JSON trước (double quotes)
            parsed = json.loads(value)
        except json.JSONDecodeError:
            # Nếu không phải JSON, thử parse Python literal (single quotes)
            try:
                parsed = ast.literal_eval(value)
            except (ValueError, SyntaxError) as e:
                raise ValueError(
                    f"Không thể parse {param_name} từ string. "
                    f"Không phải JSON cũng không phải Python dict. "
                    f"Lỗi: {e}. String value: {value[:500]}"
                ) from e
        
        # Kiểm tra kết quả parse có phải dict không
        if not isinstance(parsed, dict):
            raise TypeError(
                f"{param_name} phải là dict sau khi parse, "
                f"nhận được: {type(parsed).__name__}, value: {str(parsed)[:200]}"
            )
        
        return parsed
    
    # Nếu không phải string cũng không phải dict
    raise TypeError(
        f"{param_name} phải là dict hoặc string (JSON/Python dict), "
        f"nhận được: {type(value).__name__}, value: {str(value)[:200]}"
    )