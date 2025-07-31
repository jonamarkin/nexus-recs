import json
import redis
from redis.commands.search.query import Query
from collections import Counter
from typing import List, Dict
from app.models.data_models import Item
from datetime import datetime

class RecommenderService:
    def __init__(self):
        try:
            self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
            self.redis_client.ping()
            print("✅ Recommender service connected to Redis.")
        except redis.ConnectionError as e:
            print(f"❌ Failed to connect to Redis: {e}")
            raise

    def get_user_recent_items(self, user_id: str, limit: int = 10) -> List[Dict]:
        """Fetches a user's most recent interactions from their Redis Stream."""
        stream_key = f"user_history:{user_id}"
        recent_events = self.redis_client.xrevrange(stream_key, count=limit)
        
        recent_items = []
        for event_id, event_data in recent_events:
            if 'item_id' in event_data:
                recent_items.append(event_data['item_id'])
        return recent_items

    def get_recommended_items(self, user_id: str, limit: int = 5) -> List[Item]:
        """
        Generates recommendations based on a user's recent item interactions.
        Logic: Find the most common category from a user's recent history,
               then recommend other popular items from that category.
        """
        recent_item_ids = self.get_user_recent_items(user_id)
        if not recent_item_ids:
            print("No recent history found for this user.")
            return []

        recent_categories = []
        for item_id in recent_item_ids:
            item_key = f"item:{item_id}"
            try:
                item_data = self.redis_client.json().get(item_key, '$.category')
                if item_data:
                    recent_categories.append(item_data[0])
            except Exception:
                pass
        
        if not recent_categories:
            return []

        most_common_category = Counter(recent_categories).most_common(1)[0][0]
        print(f"User '{user_id}' most common recent category: '{most_common_category}'")

        query = (
            Query(f"@category:{{{most_common_category}}}")
            .return_field('$.item_id', as_field='item_id')
            .return_field('$.title', as_field='title')
            .return_field('$.category', as_field='category')
            .return_field('$.created_at', as_field='created_at')
            .sort_by('created_at', asc=False)
            .paging(0, limit)
        )
        
        search_results = self.redis_client.ft('idx:items').search(query)
        
        recommendations = []
        for doc in search_results.docs:
            item_id = doc.item_id
            if item_id not in recent_item_ids:
                recommendations.append(Item(
                    item_id=item_id,
                    title=doc.title,
                    category=doc.category,
                    created_at=datetime.fromtimestamp(int(doc.created_at)) # CORRECTED: Parse timestamp
                ))
        
        return recommendations

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python -m app.services.recommender_service <user_id>")
        sys.exit(1)

    user_id = sys.argv[1]
    recommender = RecommenderService()
    
    print(f"Generating recommendations for user '{user_id}'...")
    recommendations = recommender.get_recommended_items(user_id, limit=5)
    
    if recommendations:
        print("\nRecommendations:")
        for item in recommendations:
            print(f" - {item.title} (ID: {item.item_id}, Category: {item.category})")
    else:
        print("No recommendations found.")