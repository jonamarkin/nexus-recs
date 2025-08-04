import json
import uuid
import random
import os
from faker import Faker

fake = Faker()

def generate_items(num_items=50):
    """Generates a list of mock items with more diverse categories, titles, and descriptions."""

    categories = ['movie', 'series', 'book', 'game', 'music', 'art', 'apparel', 'gadget', 'food', 'travel']
    
    all_items = []
    
    # Generate the items
    for i in range(num_items):
        category = random.choice(categories)
        
        # Use Faker to generate more realistic titles and descriptions based on category
        if category == 'movie' or category == 'series':
            title = fake.sentence(nb_words=random.randint(3, 6)).replace('.', '')
            description = fake.paragraph(nb_sentences=random.randint(2, 4))
        elif category == 'book':
            title = fake.catch_phrase()
            description = fake.text(max_nb_chars=200)
        elif category == 'game':
            title = fake.word().capitalize() + " " + fake.word().capitalize()
            description = fake.paragraph(nb_sentences=2)
        elif category == 'music':
            title = fake.catch_phrase()
            description = f"An album by {fake.name()} released in {fake.year()}."
        else:
            title = fake.word().capitalize() + " " + fake.word().capitalize()
            description = fake.sentence(nb_words=10)
        
        all_items.append({
            "item_id": str(uuid.uuid4()),
            "title": title,
            "description": description,
            "category": category,
        })
        
    return all_items

def save_items_to_json(items, filename="data/items.json"):
    """Saves the generated items to a JSON file."""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, 'w') as f:
        json.dump(items, f, indent=4)
        
    print(f"✅ Successfully generated and saved {len(items)} items to {filename}")

if __name__ == "__main__":
    generated_items = generate_items(num_items=50)
    save_items_to_json(generated_items)