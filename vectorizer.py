from sentence_transformers import SentenceTransformer
import numpy as np
import base64 # <-- ADD THIS IMPORT

# Use a pre-trained model. 'all-MiniLM-L6-v2' is a good, lightweight choice.
# It produces 384-dimensional vectors.
EMBEDDING_MODEL = SentenceTransformer('all-MiniLM-L6-v2')
VECTOR_DIMENSION = 384

def get_embedding(text: str) -> str: # <-- CHANGE THE RETURN TYPE
    """
    Generates a vector embedding for a given text and returns it as a Base64-encoded string.
    """
    embedding = EMBEDDING_MODEL.encode(text)
    embedding_bytes = np.array(embedding).astype(np.float32).tobytes()
    return base64.b64encode(embedding_bytes).decode('utf-8')

if __name__ == '__main__':
    # A simple test to show it works
    test_text = "A magical movie about friendship and adventure."
    embedding_string = get_embedding(test_text)
    print(f"Generated a {VECTOR_DIMENSION}-dimensional embedding (Base64 string) for the text.")
    print(embedding_string)