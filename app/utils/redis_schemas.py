import redis
from redis.commands.search.field import TagField, TextField, NumericField, VectorField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Define the constants from the vectorizer module
# Note: These must match the values used in your vectorizer.py file
VECTOR_DIMENSION = 384

def create_user_search_index():
    """
    Creates a RediSearch index for user data.
    """
    index_name = "user_idx"

    try:
        redis_client.ft(index_name).info()
        print(f"Index '{index_name}' already exists.")
    except redis.exceptions.ResponseError:
        schema = (
            TagField("$.user_id", as_name="user_id"),
            TextField("$.username", as_name="username"),
            NumericField("$.last_login", as_name="last_login"),
            TagField("$.preferences", as_name="preferences"),
        )
        definition = IndexDefinition(prefix=['user:'], index_type=IndexType.JSON)
        redis_client.ft(index_name).create_index(fields=schema, definition=definition)
        print(f"Index '{index_name}' created successfully.")

def create_item_search_index():
    """
    Creates a RediSearch index for item data, including the vector field.
    """
    index_name = "item_idx"
    try:
        redis_client.ft(index_name).info()
        print(f"Index '{index_name}' already exists.")
    except redis.exceptions.ResponseError:
        schema = (
            TagField("$.item_id", as_name="item_id"),
            TextField("$.title", as_name="title"),
            TextField("$.description", as_name="description"),
            TagField("$.category", as_name="category"),
            NumericField("$.created_at", as_name="created_at"),
            # --- THIS IS THE NEW VECTOR FIELD ---
            VectorField(
                "$.embedding",
                "HNSW", # HNSW is a good choice for approximate nearest neighbor search
                {
                    "TYPE": "FLOAT32",
                    "DIM": VECTOR_DIMENSION,
                    "DISTANCE_METRIC": "COSINE" # Cosine similarity is a great metric for text embeddings
                },
                as_name="embedding"
            ),
        )
        definition = IndexDefinition(prefix=['item:'], index_type=IndexType.JSON)
        redis_client.ft(index_name).create_index(fields=schema, definition=definition)
        print(f"Index '{index_name}' created successfully.")

if __name__ == '__main__':
    create_user_search_index()
    create_item_search_index()
    print("✅ All Redis indexes created.")