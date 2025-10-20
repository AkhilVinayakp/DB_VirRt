from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="HPPRD_RAG_BACKEND")

# defining schemas
class QueryRequest(BaseModel):
    query: str

class QueryResponse(BaseModel):
    response: str
    code: int

@app.get("/")
async def default():
    return {"status": 200, "message": "API is running"}

@app.post("/v1/search", response_model=QueryResponse)
async def search(request: QueryRequest):
    return QueryResponse(response="Works", code=200)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
