from langchain_community.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import PGVector
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

if __name__ == "__main__":
    embeddings = CustomEmbeddings(
    api_url="https://ai-platform-uat.msb.com.vn/embeddinggamma/v1/embeddings",
    model="local-sentence-transformer"
    )
    result = embeddings.embed_query("Xin chào, world!")
    print(result)