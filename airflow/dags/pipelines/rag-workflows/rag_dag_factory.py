from pathlib import Path

# The following import is here so Airflow parses this file
# from airflow import DAG
from dagfactory import load_yaml_dags

# Thư mục chứa các template YAML cho rag-workflows
CONFIG_ROOT_DIR = Path("/opt/airflow/dags/pipelines/rag-workflows/templates")

# Render tất cả YAML trong thư mục templates
for yaml_file in sorted(CONFIG_ROOT_DIR.glob("*.yaml")):
    load_yaml_dags(
        globals_dict=globals(),
        config_filepath=str(yaml_file),
    )