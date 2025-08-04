from fastapi import FastAPI
from typing import List

from app.services.recommender_service import RecommenderService
from app.models.data_models import Item

app = FastAPI()

recommender_service = RecommenderService()

@app.get("/")
def read_root():
    """Simple root endpoint to confirm the API is running."""
    return {"message": "Welcome to the Nexus Recommendation API"}

@app.get("/recommendations/category/{user_id}", response_model=List[Item])
def get_category_recommendations(user_id: str):
    """
    Endpoint to get category-based recommendations for a user.
    """
    print(f"API call received for user: {user_id}")
    recommendations = recommender_service.get_recommended_items(user_id, limit=5)
    return recommendations

@app.get("/recommendations/collaborative/{user_id}", response_model=List[Item])
def get_collaborative_recommendations(user_id: str):
    """
    Endpoint to get collaborative filtering recommendations for a user.
    """
    print(f"API call received for collaborative filtering for user: {user_id}")
    recommendations = recommender_service.get_collaborative_recommendations(user_id, limit=5)
    return recommendations