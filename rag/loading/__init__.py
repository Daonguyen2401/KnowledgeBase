# Import loaders to ensure they are registered
from . import loaders  # noqa: F401

# Import public API
from .loading_builder import load_documents, get_document_loader  # noqa: F401

