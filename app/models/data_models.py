from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional

class User(BaseModel):
    user_id: str
    username: str
    signup_date: datetime
    last_login: datetime

class Item(BaseModel):
    item_id: str
    title: str
    description: str
    category: str
    created_at: datetime
    
class ItemWithScore(BaseModel):
    item_id: str
    title: str
    description: str
    category: str
    score: float

class UserEvent(BaseModel):
    user_id: str
    item_id: str
    event_type: str
    timestamp: datetime

class RecommendationResponse(BaseModel):
    user_id: str
    recommendations: List[ItemWithScore]
    message: Optional[str] = None