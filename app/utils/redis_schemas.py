import redis
from redis.commands.search.field import TagField, TextField, NumericField
from redis.commands.search.index_definition import IndexDefinition, IndexType

def create_redis_indexes(r: redis.Redis):
    """
    Creates RediSearch indexes for User and Item data models.
    """
    # Define the index name and key prefix for our Users
    user_index_name = "idx:users"
    user_prefix = "user:"
    
    try:
        # Check if the user index already exists
        r.ft(user_index_name).info()
        print(f"Index '{user_index_name}' already exists.")
    except redis.exceptions.ResponseError:
        # Define the schema for the User index
        user_schema = (
            TagField("$.user_id", as_name="user_id"),
            TextField("$.username", as_name="username"),
            NumericField("$.signup_date", as_name="signup_date"),
            NumericField("$.age", as_name="age"),
            TagField("$.location", as_name="location")
        )
        # Define the index with a specific key prefix and type JSON
        definition = IndexDefinition(
            prefix=[user_prefix],
            index_type=IndexType.JSON
        )
        r.ft(user_index_name).create_index(user_schema, definition=definition)
        print(f"Index '{user_index_name}' created successfully.")

    # Define the index name and key prefix for our Items
    item_index_name = "idx:items"
    item_prefix = "item:"

    try:
        # Check if the item index already exists
        r.ft(item_index_name).info()
        print(f"Index '{item_index_name}' already exists.")
    except redis.exceptions.ResponseError:
        # Define the schema for the Item index
        item_schema = (
            TagField("$.item_id", as_name="item_id"),
            TextField("$.title", as_name="title"),
            TagField("$.category", as_name="category"),
            TextField("$.description", as_name="description"),
            TagField("$.tags", as_name="tags")
        )
        definition = IndexDefinition(
            prefix=[item_prefix],
            index_type=IndexType.JSON
        )
        r.ft(item_index_name).create_index(item_schema, definition=definition)
        print(f"Index '{item_index_name}' created successfully.")

if __name__ == '__main__':
    print("Running Redis index creation script...")
    r_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    try:
        r_client.ping()
        create_redis_indexes(r_client)
        print("Redis index creation script finished.")
    except redis.exceptions.ConnectionError as e:
        print(f"Could not connect to Redis: {e}")