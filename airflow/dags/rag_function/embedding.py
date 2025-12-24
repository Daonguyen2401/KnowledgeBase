"""
Function to get embedding config.
"""
from typing import Dict
from pinwheel.helper import make_serializable


def get_embedding_config(
    type: str,
    model: str = None,
    api_key: str = None,
    **kwargs
) -> Dict:
    kwargs = make_serializable(kwargs)
    config = {
        "type": type,
        **kwargs
    }
    if model:
        config["model"] = model
    if api_key:
        config["api_key"] = api_key
    return config

