from kafka import KafkaConsumer
from pymongo import MongoClient
import json

    
def map_data(data):
    product_flags = [0] * 10  # There are 10 flags, one for each product ID from 1 to 10.
    for product_id in data['browsingPattern']:
        if 1 <= product_id <= 10:
            product_flags[product_id - 1] = 1

    result = {
        'sessionId': data['sessionId'],
        'sessionTimestamp': data['sessionTimestamp'],
        'userId': data['userId'],
        'userName': data['name'],
        'email': data['email'],
        'gender':data['gender'],
        'state': data['locationData']['state'],
        'ipAddress': data['ipAddress'],
        'sessionDuration': data['sessionDuration'],
        'clicks': data['clicks'],
        'exitPage': data['exitPage'],
        'referrer':data['referrer'],
        'deviceType': data['deviceInformation']['deviceType'],
        'paymentMethodType': data['paymentMethodType'],
        'amountSpent': data['amountSpent'],
        'action' : data['actions'],
        'purchaseMade': data['purchaseMade']
    }

    for i in range(10):
        result[f'isViewedProduct{i + 1}'] = product_flags[i]

    return result


def main():
    # Setup Kafka Consumer
    consumer = KafkaConsumer(
        'valid_customer_session',
        bootstrap_servers=['10.0.0.244:29092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Setup MongoDB connection
    client = MongoClient('10.0.0.244', 49153)
    db = client['user_database']
    collection = db['user_sessions']

    # Consume messages from Kafka
    for message in consumer:
        data = message.value
        processed_data = map_data(data)
        collection.insert_one(processed_data)
        print(f"Inserted into MongoDB: {processed_data}")

if __name__ == "__main__":
    main()