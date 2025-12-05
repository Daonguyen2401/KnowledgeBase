# Import chunkers to ensure they are registered
from . import chunkers  # noqa: F401

# Import public API
from .chunking_builder import chunk_documents, get_text_splitter  # noqa: F401

