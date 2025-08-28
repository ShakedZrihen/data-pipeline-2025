import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ייבואים שהיו חסרים
from .database import engine, Base
from . import models
from .routes.api import api_router

print("--- EXECUTING: Base.metadata.create_all(bind=engine) ---")
Base.metadata.create_all(bind=engine)

from .routes.api import api_router  # <--- הוספנו את ה-import הזה

print("--- EXECUTING: Base.metadata.create_all(bind=engine) ---")
print("--- CREATING DATABASE TABLES... ---")
Base.metadata.create_all(bind=engine)


# שגיאת הסינטקס הייתה כאן - כל הפרמטרים צריכים להיות בתוך הסוגריים
app = FastAPI(
    title="Salim API",
    description="A FastAPI server with PostgreSQL integration",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
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

# Include API routes
app.include_router(api_router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)