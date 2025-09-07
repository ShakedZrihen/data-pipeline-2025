from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes.api import health, prices, promos, branches
import uvicorn

app = FastAPI(
    title="Salim API",
    description="""
    A FastAPI server with PostgreSQL integration.

    ---

    **Supermarket Codes**  
    Here are the supported provider IDs:

    - `7290785400000` – קשת טעמים  
    - `7290103152017` – אושר עד  
    - `7291059100008` – פוליצר  
    - `7290873255550` – טיב טעם  
    - `7290803800003` – יוחננוף  
    - `7290055700007` – קרפור
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Welcome to Salim API!"}

app.include_router(health.router)
app.include_router(prices.router)
app.include_router(promos.router)
app.include_router(branches.router)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 