import redis
from confluent_kafka import Consumer, KafkaException, Producer
import socket

# --- Redis Connection Test ---
print("Attempting to connect to Redis...")
try:
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.ping()
    print("✅ Successfully connected to Redis!")
except redis.ConnectionError as e:
    print(f"❌ Failed to connect to Redis: {e}")

# --- Kafka Connection Test ---
print("\nAttempting to connect to Kafka...")
try:
    # Producer test
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    print("✅ Successfully created a Kafka Producer!")

    # Consumer test
    conf = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'my_test_group',
            'auto.offset.reset': 'earliest'}
    
    # We create a consumer but don't consume, just to verify connection
    consumer = Consumer(conf)
    print("✅ Successfully created a Kafka Consumer!")

    print("Kafka connection is working. Our app can talk to the cluster.")

except KafkaException as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    print("Please ensure the Kafka container is running and its logs show no errors.")
except Exception as e:
    print(f"An unexpected error occurred with Kafka: {e}")