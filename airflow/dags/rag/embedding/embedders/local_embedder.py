from langchain.embeddings import OpenAIEmbeddings
from dags.rag.embedding.build_embedder import register_embedder
from langchain.embeddings.base import Embeddings
import requests


class CustomEmbeddings(Embeddings):
    def __init__(self, api_url: str, model: str):
        self.api_url = api_url
        self.model = model
 
    def embed_documents(self, texts):
        return [self._get_embedding(t) for t in texts]
 
    def embed_query(self, text):
        return self._get_embedding(text)
 
    def _get_embedding(self, text: str):
        response = requests.post(
            self.api_url,
            json={"model": self.model, "input": text}
        )
        response.raise_for_status()
        return response.json()["data"][0]["embedding"]

@register_embedder("local-gamma")
def create_embedding_gamma_embedder(model: str, api_key: str):
    base_url = f'https://ai-platform-uat.msb.com.vn/embeddinggamma/v1/embeddings'
    return CustomEmbeddings(model=model,api_url=base_url)

if __name__ == "__main__":
    embeddings = create_embedding_gamma_embedder(model="local-sentence-transformer", api_key="")
    result = embeddings.embed_query("Xin chào, world!")
    print(result)
    