from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class User(BaseModel):
    user_id: str
    username: str
    signup_date: datetime
    last_login: Optional[datetime] = None
    age: Optional[int] = None
    location: Optional[str] = None
    recent_items_viewed: List[str] = []

class Item(BaseModel):
    item_id: str
    title: str
    category: str
    description: Optional[str] = None
    tags: List[str] = []
    created_at: datetime

class UserEvent(BaseModel):
    user_id: str
    item_id: str
    event_type: str  # e.g., "view", "click", "purchase"
    timestamp: datetime