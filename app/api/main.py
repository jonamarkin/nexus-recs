from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

from app.services.recommender_service import RecommenderService
from app.models.data_models import RecommendationResponse

app = FastAPI()

# --- Add CORS Middleware ---
origins = [
    "http://localhost:8002",
    "http://127.0.0.1:8002",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    print("Application startup...")

@app.get("/")
def read_root():
    return {"message": "Welcome to Nexus Recs API"}

@app.get("/recommendations/category/{user_id}", response_model=RecommendationResponse)
def get_category_recommendations(user_id: str, recommender_service: RecommenderService = Depends()):
    print(f"API call received for user: {user_id}")
    try:
        # CORRECTED: Changed the method call to get_category_recommendations
        recommendations = recommender_service.get_category_recommendations(user_id, limit=5)
        if not recommendations:
            return RecommendationResponse(user_id=user_id, recommendations=[], message=f"No recent history found for user '{user_id}'.")
        return RecommendationResponse(user_id=user_id, recommendations=recommendations)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/recommendations/collaborative/{user_id}", response_model=RecommendationResponse)
def get_collaborative_recommendations(user_id: str, recommender_service: RecommenderService = Depends()):
    print(f"API call received for collaborative filtering for user: {user_id}")
    try:
        recommendations = recommender_service.get_collaborative_recommendations(user_id, limit=5)
        if not recommendations:
            return RecommendationResponse(user_id=user_id, recommendations=[], message=f"No similar users found for user '{user_id}'.")
        return RecommendationResponse(user_id=user_id, recommendations=recommendations)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
@app.get("/recommendations/semantic/{user_id}", response_model=RecommendationResponse)
def get_semantic_recommendations(user_id: str, query: str, recommender_service: RecommenderService = Depends()):
    print(f"API call received for semantic search for user: {user_id}, query: '{query}'")
    try:
        recommendations = recommender_service.get_semantic_recommendations(query, user_id, limit=5)
        if not recommendations:
            return RecommendationResponse(user_id=user_id, recommendations=[], message="No similar items found for your query.")
        return RecommendationResponse(user_id=user_id, recommendations=recommendations)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))