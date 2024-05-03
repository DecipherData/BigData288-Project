from kafka import KafkaConsumer
import json
import redis
import logging
from pymongo import MongoClient
from kafka import KafkaProducer

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

global skipped_users_count
skipped_users_count = 0

# Setup Redis connection
r = redis.Redis(host='localhost', port=6379, db=0)
bloom_filter_name = "users_bloomfilter"
hll_name = "distinct_users_hyperloglog"



# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['10.0.0.244:29092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# # Setup MongoDB connection
# client = MongoClient('localhost', 49153)  # Connect to the MongoDB server
# db = client['user_database']  # Use (or create) a database named "user_database"
# collection = db['valid_users']  # Use (or create) a collection named "valid_users"

# Correct typo in bootstrap_servers to bootstrap_servers
consumer = KafkaConsumer('user_session_info',
                         bootstrap_servers=['10.0.0.244:29092'],
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))


# HINCRBY is used here to calculate the number/count of sessions per user. 
# HINCRBY is a command in Redis used to increment the integer value of a field in a hash stored at a key. 
# If the key or field does not exist, it is set to 0 before the operation.
def increment_number_of_sessions(userId):
    """
    Increments the session count for a specific user.
    :param userId: The ID of the user.
    """
    # Use HINCRBY to increment the session count for the user
    r.hincrby("user_session_counts", userId, 1)

    print(f"Incremented session count for User ID {userId}.")

def get_session_count(userId):
    """
    Retrieves the session count for a specific user.

    :param userId: The ID of the user.
    :return: Session count for the user.
    """
    session_count = r.hget("user_session_counts", userId)
    
    return int(session_count) if session_count else 0

    
def process_message(message):
    user_id = message['userId']
    device_type = message['deviceInformation']['deviceType']
    #device_info = message['deviceInformation']
    referrer = message['referrer']
    user_name = message['name']
    
    # Check if the user already exists in the Bloom filter
    if r.execute_command('BF.EXISTS', bloom_filter_name, user_id):
        print(f"User id : {user_id} is present in the Bloom Filter.")
        # HINCRBY is used here to calculate the number of sessions per user. 
        increment_number_of_sessions(user_id)
        # Fetching the updated count to verify
        session_count = get_session_count(user_id)
        logger.info(f"Session count for User ID {user_id}: {session_count}")
        # If the user exists, send the message to Kafka
        producer.send('valid_customer_session', message)
        logger.info(f"Existing user {user_id}: Sent to valid_customer_session.")
    else:
        # If the user does not exist, check device and referrer if unknown.. we are assuming that these records may be mallicious
        if device_type == 'unknown' or referrer == 'unknown':
            logger.info(f"Skipped user {user_id} due to unknown device type or referrer.")
            return  # Skip processing this user

        # Valid user is added to the bloom filter
        logger.info(f"Going to add a new user name: {user_name} and Id: {user_id} in the Bloom Filter.")
        r.execute_command('BF.ADD', bloom_filter_name, user_id)
        
        '''HyperLogLog: An improvement over the Flajolet-Martin algorithm, HyperLogLog (HLL) provides 
        better accuracy with substantially less memory. It uses multiple hash functions to divide the data 
        into several registers and applies harmonic mean based calculations to estimate cardinality.
        Memory Efficiency: HyperLogLog requires significantly less memory (typically around 12 KB) 
        regardless of the size of the data, making it extremely memory efficient.
        Precision and Performance: Provides a standard error rate of about 0.81% for 
        cardinality estimation, which is typically acceptable for many applications, especially given its low memory usage.
        '''
        
        r.pfadd(hll_name, user_id) # also new user entry to hyperloglog.
    
        # distinct user count using hyperloglog (advance method of flajotlet martin, done in the class)
        # PFCOUNT is the command used to retrieve the estimated count of unique users from HyperLogLog data structures in Redis.
        estimated_users_ids = r.execute_command('PFCOUNT', hll_name)
        logger.info(f"Unique user Ids: {estimated_users_ids}")
        
        # Send the new valid user to Kafka
        producer.send('valid_customer_session', message)
        logger.info(f"New user {user_id}: Added to Bloom filter and sent to valid_customer_session.")
        
try:
    # Bloom Filter
    if not r.execute_command('EXISTS', bloom_filter_name):
        logger.info("Configuring REDIS Bloom Filter")
        r.execute_command('BF.RESERVE', bloom_filter_name, '0.01', '1000') # create Bloom filter
    else:
        logger.info(f'Bloom filter with name {bloom_filter_name} already present.')
        
    for msg in consumer:
        process_message(msg.value)
except Exception as e:
    logger.error("Error processing Kafka message: %s", e)
finally:
    consumer.close()
    producer.close()