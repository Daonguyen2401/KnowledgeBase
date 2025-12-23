# Import indexers to ensure they are registered
from . import indexers  # noqa: F401

# Import public API
from .indexing_builder import index_documents, index_documents_with_embeddings_config, get_indexer  # noqa: F401

