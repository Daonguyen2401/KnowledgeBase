from pathlib import Path
import os
import yaml
import tempfile
from typing import Dict, Any, List

# The following import is here so Airflow parses this file
# from airflow import DAG
from dagfactory.dagfactory import _DagFactory

# Thư mục chứa các template YAML cho rag-workflows
# Sử dụng đường dẫn tương đối từ file hiện tại để đảm bảo đúng trong mọi môi trường
CURRENT_DIR = Path(__file__).parent
CONFIG_ROOT_TEST_DIR = CURRENT_DIR / "test_templates"

# Đường dẫn đến file defaults.yml (hỗ trợ cả .yml và .yaml)
DEFAULTS_CONFIG_PATH = None
DEFAULTS_CONFIG = None
for ext in [".yml", ".yaml"]:
    defaults_path = CONFIG_ROOT_TEST_DIR / f"defaults{ext}"
    if defaults_path.exists():
        DEFAULTS_CONFIG_PATH = defaults_path
        # Load defaults config để merge tasks
        with open(defaults_path, "r", encoding="utf-8") as f:
            DEFAULTS_CONFIG = yaml.safe_load(f) or {}
        break


def merge_defaults_with_dag_config(
    dag_config: Dict[str, Any],
    defaults_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Merge defaults config vào DAG config, bao gồm cả tasks.
    
    Theo documentation dag-factory, defaults_config_path chỉ merge default_args và 
    DAG-level configs, KHÔNG merge tasks. Hàm này tự implement logic merge tasks.
    
    Args:
        dag_config: Config của DAG từ file YAML
        defaults_config: Config từ defaults.yml
    
    Returns:
        Merged config với tasks từ defaults được thêm vào
    """
    merged_config = dag_config.copy()
    
    # Lấy default section từ defaults_config
    default_section = defaults_config.get("default", {})
    
    # Merge default_args (nếu chưa có trong dag_config)
    if "default_args" not in merged_config:
        merged_config["default_args"] = default_section.get("default_args", {})
    else:
        # Deep merge default_args
        dag_default_args = merged_config.get("default_args", {})
        defaults_default_args = default_section.get("default_args", {})
        merged_config["default_args"] = {**defaults_default_args, **dag_default_args}
    
    # Merge các DAG-level configs (schedule, catchup, tags, etc.)
    for key in ["schedule", "catchup", "tags", "description", "params"]:
        if key not in merged_config and key in default_section:
            merged_config[key] = default_section[key]
    
    # Merge tasks từ defaults vào DAG config
    # Tasks từ defaults sẽ được thêm vào TRƯỚC tasks của DAG
    default_tasks = default_section.get("tasks", [])
    dag_tasks = merged_config.get("tasks", [])
    
    if default_tasks:
        # Merge tasks: defaults tasks trước, sau đó là DAG tasks
        # Tạo dict để track task_ids đã có (tránh duplicate)
        existing_task_ids = {task.get("task_id") for task in dag_tasks if isinstance(task, dict)}
        
        # Thêm tasks từ defaults (chỉ thêm nếu chưa có task_id trùng)
        merged_tasks = []
        for task in default_tasks:
            if isinstance(task, dict):
                task_id = task.get("task_id")
                if task_id and task_id not in existing_task_ids:
                    merged_tasks.append(task)
                    existing_task_ids.add(task_id)
            else:
                merged_tasks.append(task)
        
        # Thêm tasks từ DAG config
        merged_tasks.extend(dag_tasks)
        merged_config["tasks"] = merged_tasks
    
    return merged_config


# Render tất cả YAML/YML trong thư mục test_templates
# Tìm cả .yaml và .yml files
yaml_files = list(CONFIG_ROOT_TEST_DIR.glob("*.yaml")) + list(CONFIG_ROOT_TEST_DIR.glob("*.yml"))

for yaml_file in sorted(yaml_files):
    # Bỏ qua file defaults.yml/yaml trong loop này
    if yaml_file.stem in ["defaults", "default"]:
        continue
    
    # Load DAG config từ file
    with open(yaml_file, "r", encoding="utf-8") as f:
        dag_configs = yaml.safe_load(f) or {}
    
    # Merge defaults vào từng DAG config nếu có defaults
    if DEFAULTS_CONFIG:
        merged_configs = {}
        for dag_id, dag_config in dag_configs.items():
            # Bỏ qua section "default" nếu có
            if dag_id == "default":
                continue
            merged_configs[dag_id] = merge_defaults_with_dag_config(
                dag_config, DEFAULTS_CONFIG
            )
        dag_configs = merged_configs
    
    # Tạo file tạm với merged config
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp_file:
        yaml.dump(dag_configs, tmp_file, default_flow_style=False, allow_unicode=True)
        tmp_config_path = tmp_file.name
    
    try:
        # Tạo DagFactory với merged config
        dag_factory = _DagFactory(config_filepath=tmp_config_path)
        
        # Build DAGs và đăng ký vào globals() để Airflow nhận diện
        dags = dag_factory.build_dags()
        
        # Đăng ký các DAGs vào globals() để Airflow có thể tìm thấy
        for dag_id, dag in dags.items():
            globals()[dag_id] = dag
    finally:
        # Xóa file tạm
        try:
            os.unlink(tmp_config_path)
        except OSError:
            pass