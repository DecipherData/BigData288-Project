from pyflink.table.expressions import col, lit
from pyflink.common import Row
from pyflink.table.udf import udf, udtf, ScalarFunction
import codecs
import re
import string
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSink
from pyflink.datastream.connectors import FlinkKafkaConsumer,FlinkKafkaProducer
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.datastream.functions import FlatMapFunction, MapFunction
from pyflink.common import Row
#from pyflink.datastream import MapFunction, FlatMapFunction, ProcessFunction, RuntimeContext
#from pyflink.datastream.connectors import ElasticsearchSink
from datetime import datetime
#from pyflink.datastream.connectors import ElasticsearchSink
from elasticsearch import Elasticsearch
import requests
import nltk
nltk.download('vader_lexicon')
from nltk.sentiment import SentimentIntensityAnalyzer
from pymongo import MongoClient
from pyflink.datastream import SinkFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import ProcessFunction, RuntimeContext
from kafka import KafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
#from pyflink.datastream import DeliveryGuarantee
from pyflink.datastream.connectors.base import DeliveryGuarantee
from datetime import datetime
# from pyflink.datastream.state import ValueStateDescriptor
# from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
# from pyflink.datastream.state import ValueState
# from datasketch import MinHash, MinHashLSH


from pyflink.common.typeinfo import RowTypeInfo
import json

from pyflink.table import (
    DataTypes, TableEnvironment, EnvironmentSettings
)

class ExpandSessionToProductDetails(FlatMapFunction):
    def flat_map(self, value):
        # Assuming 'value' is a JSON string containing the session data
        data = json.loads(value)
        for i, product_id in enumerate(data['productIds']):
            product_detail = {
                'sessionId': data['sessionId'],
                'timestamp': data['sessionTimestamp'],
                'userId': data['userId'],
                'userName': data['name'],
                'productId': product_id,
                'productName': data['productNames'][i],
                'productPrice': data['productPrices'][i],
                'productCategory': data['productCategories'][i],
                'productReview': data['productReviews'][i],
                'timeSpentOnPage': data['timeSpentonEachPage'][i],
                'action': data['actions'],
                'purchaseMade': data['purchaseMade']
            }
            yield product_detail
            

class SessionSummary(MapFunction):
    def map(self, value):
        data = json.loads(value)
        print("data is : ", data)
        
        # Convert sessionTimestamp to datetime
        session_timestamp = datetime.fromisoformat(data['sessionTimestamp'])
        
        # Extract parts of the timestamp into separate fields
        hour_of_day = session_timestamp.hour
        day_of_week = session_timestamp.strftime('%A')
        is_weekend = int(day_of_week in ['Saturday', 'Sunday'])
        month = session_timestamp.month
        week_of_year = int(session_timestamp.strftime('%V'))
        part_of_day = self.map_hour_to_part_of_day(hour_of_day)
        
        product_flags = [0] * 10  # There are 10 flags, one for each product ID from 1 to 10.
        for product_id in data['browsingPattern']:
            if 1 <= product_id <= 10:  # Ensure product_id is within the range of 1 to 10.
                product_flags[product_id - 1] = 1  # Set the flag corresponding to the product ID.

        # Create a dictionary for the result, mapping flag names to their values
        result = {
            'sessionId': data['sessionId'],
            'timestamp': data['sessionTimestamp'],
            'userId': data['userId'],
            'userName': data['name'],
            'email': data['email'],
            'gender':data['gender'],
            'state': data['locationData']['state'],
            'ipAddress': data['ipAddress'],
            'sessionDuration': data['sessionDuration'],
            'clicks': data['clicks'],
            'exitPage': data['exitPage'],
            'referrer': data['referrer'],
            'deviceType': data['deviceInformation']['deviceType'],
            'action': data['actions'],
            'paymentMethodType': data['paymentMethodType'],
            'amountSpent': data['amountSpent'],
            'purchaseMade': data['purchaseMade'],
            # Update the original data with the new fields
            'hour_of_day': hour_of_day,
            'day_of_week': day_of_week,
            'is_weekend': is_weekend,
            'month': month,
            'week_of_year': week_of_year,
            'part_of_day': part_of_day
        
        }

        # Add the product flags to the result dictionary dynamically
        for i in range(10):
            result[f'isViewedProduct{i + 1}'] = product_flags[i]

        return result
    
    @staticmethod
    def map_hour_to_part_of_day(hour):
        if 5 <= hour < 12:
            return 'Morning'
        elif 12 <= hour < 17:
            return 'Afternoon'
        elif 17 <= hour < 21:
            return 'Evening'
        else:
            return 'Night'
        
# class LSHKeyedProcessFunction(KeyedProcessFunction):
#     def __init__(self, num_perm):
#         self.num_perm = num_perm
#         self.minhash_state_desc = None
#         self.lsh = MinHashLSH(threshold=0.5, num_perm=self.num_perm)

#     def open(self, runtime_context: RuntimeContext):
#         state_descriptor = ValueStateDescriptor("minhash", Types.PICKLED_BYTE_ARRAY())
#         self.minhash_state_desc = runtime_context.get_state(state_descriptor)

#     def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
#         # Get current MinHash state for the user
#         current_minhash = self.minhash_state_desc.value()
#         if current_minhash is None:
#             current_minhash = MinHash(num_perm=self.num_perm)

#         # Update MinHash with new session data
#         session_data = json.loads(value)
#         for feature in session_data:
#             if feature not in ['sessionId', 'userId', 'userName']:
#                 current_minhash.update(str(session_data[feature]).encode('utf-8'))

#         # Save updated MinHash state
#         self.minhash_state_desc.update(current_minhash)

#         # Query LSH for similar users and emit results
#         similar_user_ids = self.lsh.query(current_minhash)
#         yield (session_data['userId'], [uid for uid in similar_user_ids if uid != session_data['userId']])


class ElasticsearchSinkFunction(MapFunction):
    def __init__(self, index_name):
        self.index_name = index_name
        self.es_url = 'http://localhost:9200'  # Adjust as necessary for your ES URL

    def map(self, value):
        headers = {'Content-Type': 'application/json'}
        response = requests.post(f"{self.es_url}/{self.index_name}/_doc/", headers=headers, data=json.dumps(value))
        print("response code", response.status_code)
        return value  # For demonstration, return status code; adjust based on your needs

def create_elasticsearch_sink(stream, index_name):
    return stream.map(ElasticsearchSinkFunction(index_name))

class SentimentAnalysisFunction(MapFunction):
    def __init__(self):
        self.analyzer = SentimentIntensityAnalyzer()

    def map(self, value):
        review = value['productReview']
        if review:  # Check if review is not None
            scores = self.analyzer.polarity_scores(review)
            sentiment = 'positive' if scores['compound'] > 0.05 else 'negative' if scores['compound'] < -0.05 else 'neutral'
        else:
            sentiment = 'neutral'  # Default sentiment for None or empty reviews
        value['sentiment'] = sentiment
        return value
    
def create_sentiment_elasticsearch_sink(stream, index_name):
    return stream.map(SentimentAnalysisFunction()).map(ElasticsearchSinkFunction(index_name))

def main():
    
    jar_path = "file:///C:/Users/bhati/BigData228/jars/flink-connector-kafka-3.0.1-1.18.jar"
    jar_path1 = "file:///C:/Users/bhati/BigData228/jars/kafka-clients-3.2.3.jar"
    jar_path2 = "file:///C:/Users/bhati/BigData228/jars/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar"
    
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(jar_path)
    env.add_jars(jar_path1)
    env.add_jars(jar_path2)
    env.set_parallelism(1)
    

    properties = {
        'bootstrap.servers': '10.0.0.244:29092',
        'group.id': 'kafka-flink-group',
    }

    # First step is to listen to valid customer session topic coming from kafka producer
    kafka_consumer = FlinkKafkaConsumer(
        topics='valid_customer_session',
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    # Add the source to the environment
    data_stream = env.add_source(kafka_consumer)
    #data_stream.print()
    
    #Process the data using the flat_map and map functions
    session_summary_stream = data_stream.map(SessionSummary())
    #process product variables that came as a list..flattened for further analysis
    product_details_stream = data_stream.flat_map(ExpandSessionToProductDetails())
    
    
    #keyed_stream = session_summary_stream.key_by(lambda x: json.loads(x)['userId'])  # Key by userId
    # Process keyed stream with LSH logic
    #similar_users_stream = keyed_stream.process(LSHKeyedProcessFunction(num_perm=500))

    #session_summary_stream.print() # tested to see if coming properly
    #product_details_stream.print()
    
    # Create Elasticsearch sinks for kibana Visualization and analysis
    session_summary_stream = create_elasticsearch_sink(session_summary_stream, "session_summary")
    product_details_stream = create_elasticsearch_sink(product_details_stream, "product_details")
    sentiment_product_stream = create_sentiment_elasticsearch_sink(product_details_stream, "product_sentiment")
    #similar_users_stream = create_sentiment_elasticsearch_sink(product_details_stream, "similar_users")
    
    # print them to see the piepline changes with sentiments
    session_summary_stream.print()
    product_details_stream.print()
    sentiment_product_stream.print()
    #similar_users_stream.print()
            
    # Execute the program
    env.execute("Process User Session Data")

if __name__ == "__main__":
    main()