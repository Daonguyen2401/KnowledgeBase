# Import embedders to ensure they are registered
from . import embedders  # noqa: F401

# Import public API
from .embedding_builder import create_embedder, get_embedder  # noqa: F401

