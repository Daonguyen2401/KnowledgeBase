from langchain_openai import OpenAIEmbeddings

from rag.embedding.build_embedder import register_embedder


@register_embedder("openai")
def create_openai_embedder(model: str, api_key: str, **kwargs):
    return OpenAIEmbeddings(model=model, api_key=api_key, **kwargs)

