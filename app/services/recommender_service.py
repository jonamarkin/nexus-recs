import json
import redis
from redis.commands.search.query import Query
from collections import Counter
from typing import List, Dict, Set
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

    def get_user_recent_items(self, user_id: str, limit: int = 20) -> Set[str]:
        """
        Fetches a user's most recent interactions from their Redis Stream and
        returns a set of unique item IDs.
        """
        stream_key = f"user_history:{user_id}"
        recent_events = self.redis_client.xrevrange(stream_key, count=limit)
        
        recent_items = set()
        for event_id, event_data in recent_events:
            if 'item_id' in event_data:
                recent_items.add(event_data['item_id'])
        return recent_items

    def get_all_user_ids(self) -> List[str]:
        """Fetches all user keys from Redis."""
        user_keys = self.redis_client.keys("user:*")
        return [key.split(':')[1] for key in user_keys]

    def jaccard_similarity(self, set1: Set, set2: Set) -> float:
        """Computes Jaccard similarity between two sets."""
        if not set1 or not set2:
            return 0.0
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        return intersection / union if union > 0 else 0.0

    def get_collaborative_recommendations(self, user_id: str, limit: int = 5) -> List[Item]:
        """
        Generates recommendations based on collaborative filtering.
        Logic: Find users most similar to the target user, then recommend
               items from their history that the target user hasn't seen.
        """
        target_user_items = self.get_user_recent_items(user_id)
        
        if not target_user_items:
            print(f"No recent history for user '{user_id}'. Cannot perform collaborative filtering.")
            return []

        all_users = self.get_all_user_ids()
        similarities = {}

        for other_user_id in all_users:
            if other_user_id == user_id:
                continue

            other_user_items = self.get_user_recent_items(other_user_id)
            if not other_user_items:
                continue

            sim = self.jaccard_similarity(target_user_items, other_user_items)
            similarities[other_user_id] = sim

        sorted_similar_users = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
        top_similar_users = [user for user, sim in sorted_similar_users if sim > 0.1][:3]

        if not top_similar_users:
            print(f"No similar users found for user '{user_id}'.")
            return []
            
        print(f"Found similar users for '{user_id}': {top_similar_users}")
        
        recommended_item_ids = set()
        for similar_user_id in top_similar_users:
            similar_user_items = self.get_user_recent_items(similar_user_id)
            
            for item_id in similar_user_items:
                if item_id not in target_user_items:
                    recommended_item_ids.add(item_id)
        
        recommendations = []
        for item_id in list(recommended_item_ids)[:limit]:
            item_key = f"item:{item_id}"
            try:
                item_data_json = self.redis_client.json().get(item_key)
                if item_data_json:
                    item_data_dict = json.loads(item_data_json)
                    recommendations.append(Item.model_validate(item_data_dict))
            except Exception:
                pass
        
        return recommendations

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
                    created_at=datetime.fromtimestamp(int(doc.created_at))
                ))
        
        return recommendations