from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from rag.chain import qa_chain

router = APIRouter()


@router.get("/health")
def health():
    print("healthy")
    return {"message": "all good"}


@router.get("/ask")
def ask(q: str = Query(..., description="question to ask")):
    response = qa_chain.invoke(q)
    sources = [doc.page_content for doc in response["source_documents"]]
    return JSONResponse(
        {
            "answer": response["result"],
            "sources": sources,
        }
    )
