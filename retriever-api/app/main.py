from fastapi import FastAPI

from app.api.retrive import router as retrieve_router


app = FastAPI(title="Retriever API")

app.include_router(retrieve_router)


@app.get("/health")
async def health_check():
    return {"status": "ok"}

