from faker import Faker
import random
from datetime import datetime
from confluent_kafka import Producer
import time
import json
from datetime import datetime, timedelta
from collections import defaultdict

fake = Faker('en_US')

def recent_datetime(days=7):
    """Generate a datetime object representing a moment within the past `days` days."""
    # Current time
    now = datetime.now()
    # Generate a time delta between now and a random number of seconds up to `days` days ago
    seconds_in_day = 86400  # Number of seconds in one day
    random_seconds = random.expovariate(1.0 / (days * seconds_in_day))  # Using an exponential distribution
    random_time_delta = timedelta(seconds=min(random_seconds, days * seconds_in_day))
    recent_time = now - random_time_delta
    return recent_time.strftime('%Y-%m-%dT%H:%M:%S')  # ISO 8601 format

def generate_customer_session(user_id, user_data, referrer, device_info):
    # Furniture items mapped to pages with prices
    furniture_items = {
        1: {"name": "sofa", "price": 5000, "category": "living room"},
        2: {"name": "coffee table", "price": 500, "category": "living room"},
        3: {"name": "accent chair", "price": 750, "category": "living room"},
        4: {"name": "bed", "price": 4500, "category": "bedroom"},
        5: {"name": "bedside table", "price": 590, "category": "bedroom"},
        6: {"name": "dresser", "price": 1290, "category": "bedroom"},
        7: {"name": "office desk", "price": 1150, "category": "office"},
        8: {"name": "office chair", "price": 899, "category": "office"},
        9: {"name": "dining table", "price": 2500, "category": "dining room"},
        10: {"name": "dining chairs", "price": 650, "category": "dining room"}
    }
    
    positive_reviews = {
    "sofa": "Absolutely love this sofa! It's so comfortable and stylish.",
    "coffee table": "The coffee table is perfect, just what I was looking for!",
    "accent chair": "Such a charming accent chair, and it fits my decor perfectly.",
    "bed": "This bed is incredibly sturdy and was easy to assemble.",
    "bedside table": "Bedside table fits perfectly next to the bed and has ample storage.",
    "dresser": "The dresser offers great space and is very functional.",
    "office desk": "Solid office desk with plenty of room and a sleek design.",
    "office chair": "Office chair is very comfortable for long working hours. Highly recommend!",
    "dining table": "Elegant and robust, this dining table is a great addition to my home.",
    "dining chairs": "These chairs are not only stylish but also very comfortable."
    }

    negative_reviews = {
    "sofa": "The sofa is less comfortable than I expected.",
    "coffee table": "The coffee table came with a few scratches and was hard to assemble.",
    "accent chair": "Accent chair looks good but not as sturdy as I hoped.",
    "bed": "The bed frame is wobbly and feels cheap.",
    "bedside table": "This bed side table is smaller than it looks in the pictures. Not satisfied.",
    "dresser": "Drawers of the dresser stick and it was a pain to put together.",
    "office desk": "The desk surface scratches easily and lacks cable management.",
    "office chair": "Office chair is uncomfortable and started squeaking within a week.",
    "dining table": "The dining table surface is prone to stains and scratches.",
    "dining chairs": "The build quality of dining chairs is questionable and uncomfortable."
    }

    neutral_reviews = {
    "sofa": "The sofa is okay, does the job.",
    "coffee table": "A standard coffee table, nothing special but works.",
    "accent chair": "Decent chair, but I wouldn't rave about it.",
    "bed": "The bed is fine, though I expected a bit more in terms of quality.",
    "bedside table": "It's a basic bedside table. Does what it needs to.",
    "dresser": "Average dresser, the build is fine.",
    "office desk": "It's an office desk, nothing more or less.",
    "office chair": "It's alright, serves its purpose for now.",
    "dining table": "The dining table is functional, though not very impressive.",
    "dining chairs": "Chairs are mediocre, good for occasional use."
    }

    # Product sets to view together
    product_sets = [
        [9, 10],  # dining table and dining chairs
        [1, 2, 3],  # sofa, coffee table, and accent chair
        [4, 5, 6],  # bed, bedside table, dresser
        [7, 8]  # office desk and office chair
    ]

    # Choose a random product set and add additional random products
    chosen_set = random.choice(product_sets)
    num_pages_viewed = random.randint(3, 10)
    browsing_pattern = random.sample(set(range(1, 11)) - set(chosen_set), num_pages_viewed - len(chosen_set))
    browsing_pattern += chosen_set
    random.shuffle(browsing_pattern)  # Shuffle to make the browsing pattern appear more natural

    # Randomly decide if a purchase was made
    purchase_made = random.choice([True, False])
    product_reviews = []

    # Calculate the total amount spent if a purchase was made
    if purchase_made:
        action = "checkout"
        amount_spent = sum(furniture_items[item_id]["price"] for item_id in browsing_pattern)
        payment_method_type = random.choice(["CreditCard", "PayPal"])
        for item_id in browsing_pattern:
            sentiment = random.choice(['positive', 'negative', 'neutral'])
            if sentiment == 'positive':
                product_reviews.append(positive_reviews[furniture_items[item_id]['name']])
            elif sentiment == 'negative':
                product_reviews.append(negative_reviews[furniture_items[item_id]['name']])
            else:
                product_reviews.append(neutral_reviews[furniture_items[item_id]['name']])
    else:
        action = random.choice(["add_to_cart", "search", "view_item"])
        amount_spent = 0
        payment_method_type = "None"
        product_reviews = [None] * len(browsing_pattern)  # No reviews if no purchase

    # Referrer sources
    #referrer_sources = ["Google", "Facebook", "Instagram", "WorldMarket", "Direct", "unknown"]

    session_data = {
        "sessionId": fake.uuid4(),
        "userId": user_id, #fake.uuid4(),
        "sessionTimestamp": recent_datetime(), 
        "productIds": browsing_pattern,
        "productNames": [furniture_items[pid]["name"] for pid in browsing_pattern],
        "productPrices": [furniture_items[pid]["price"] for pid in browsing_pattern],
        "productCategories": [furniture_items[pid]["category"] for pid in browsing_pattern],
        "productReviews": product_reviews,
        "name": user_data['name'],
        "gender": user_data['gender'],
        "email": user_data['email'],
        "deviceInformation": {
            "deviceType": device_info['deviceType'], 
            "operatingSystem": device_info['operatingSystem'], 
            "browserType": device_info['browserType'], 
        },
        "locationData": {
            "country": "United States",
            "state": fake.state(),
            "city": fake.city(),
            "zipCode": fake.zipcode(),
        },
        "ipAddress": fake.ipv4(),
        "sessionDuration": random.randint(5, 600),
        "pageViews": num_pages_viewed,
        "browsingPattern": browsing_pattern,
        "timeSpentonEachPage": [random.randint(10, 120) for _ in browsing_pattern],
        "clicks": random.randint(1, 50),
        "actions": action,
        "referrer": referrer, 
        "exitPage": furniture_items[random.choice(browsing_pattern)]["name"],  # Exit page is one of the visited pages
        "purchaseMade": purchase_made,
        "amountSpent": amount_spent,
        "paymentMethodType": payment_method_type,
        
    }

    return session_data

#Adjusting the function to generate multiple sessions with improved logic
def generate_random_sessions(min_users=3, max_users=10, min_sessions=1, max_sessions=5):
    num_users = random.randint(min_users, max_users)
    users_data = defaultdict(list)

    for _ in range(num_users):
        user_id = fake.uuid4()  # Create a unique User ID
        # Generate user-specific data once per user
        user_data = {
            "name": fake.name(),
            "email": fake.email(),
            "gender": random.choice(['Male', 'Female', 'Other'])
        }
        
        # Initial settings
        possible_referrers = ["Google", "Facebook", "Instagram", "WorldMarket", "Direct"]
        possible_devices = ["mobile", "desktop", "tablet"]
        referrer = random.choice(possible_referrers + ["unknown"])
        device = random.choice(possible_devices + ["unknown"])

        # Determine subsequent options based on the initial choice
        subsequent_referrers = possible_referrers if referrer != "unknown" else ["unknown"]
        subsequent_devices = possible_devices if device != "unknown" else ["unknown"]
        
        num_sessions = random.randint(min_sessions, max_sessions)  # Randomly decide the number of sessions for this user

        for _ in range(num_sessions):
            if i == 0:  # Use initial settings for the first session
                session_referrer = referrer
                session_device = device
            else:  # Randomize from the possible subsequent options
                session_referrer = random.choice(subsequent_referrers)
                session_device = random.choice(subsequent_devices)

            device_info = {
                "deviceType": session_device,
                "operatingSystem": fake.user_agent(),
                "browserType": fake.user_agent(),
            }
            
            session_data = generate_customer_session(user_id, user_data, session_referrer, device_info)
            print(session_data)
            session_data["userId"] = user_id  # Assign the same User ID to all sessions of this user
            users_data[user_id].append(session_data)

    return users_data

# Create a Kafka producer instance
producer = Producer({'bootstrap.servers': '10.0.0.244:29092'})

def delivery_callback(err, msg):
    if err:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to topic:', msg.topic(), 'partition:', msg.partition())


def send_sessions_to_kafka():
    user_sessions = generate_random_sessions(min_users=3, max_users=10, min_sessions=1, max_sessions=5)
    for user_id,sessions in user_sessions.items():
        for session in sessions:
            print(session)
            producer.produce("user_session_info", key=str(session['sessionId']),value=json.dumps(session).encode("utf-8"), callback=delivery_callback)
            producer.poll(0)
        producer.flush()
        time.sleep(1)  # Adjust interval as needed

i=0
while i<50:
    send_sessions_to_kafka()
    i=i+1
