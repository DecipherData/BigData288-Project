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
            'state': data['locationData']['state'],
            'ipAddress': data['ipAddress'],
            'sessionDuration': data['sessionDuration'],
            'clicks': data['clicks'],
            'exitPage': data['exitPage'],
            'deviceType': data['deviceInformation']['deviceType'],
            'paymentMethodType': data['paymentMethodType'],
            'amountSpent': data['amountSpent'],
            'purchaseMade': data['purchaseMade']
        }

        # Add the product flags to the result dictionary dynamically
        for i in range(10):
            result[f'isViewedProduct{i + 1}'] = product_flags[i]

        return result

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
    ## process product variables that came as a list..flattened for further analysis
    product_details_stream = data_stream.flat_map(ExpandSessionToProductDetails())
    

    #session_summary_stream.print() # tested to see if coming properly
    #product_details_stream.print()
    
    # Create Elasticsearch sinks for kibana Visualization and analysis
    session_summary_stream = create_elasticsearch_sink(session_summary_stream, "session_summary")
    product_details_stream = create_elasticsearch_sink(product_details_stream, "product_details")
    sentiment_product_stream = create_sentiment_elasticsearch_sink(product_details_stream, "product_sentiment")
    
    # print them to see the piepline changes with sentiments
    session_summary_stream.print()
    product_details_stream.print()
    sentiment_product_stream.print()
    
        
    # Execute the program
    env.execute("Process User Session Data")

if __name__ == "__main__":
    main()
    
    # def consume_topic_to_mongodb(kafka_topic, mongo_collection):
    # # Setup MongoDB connection
    # client = MongoClient('localhost', 49153)
    # print("Connection established -mongodb")
    # db = client['user_database']
    # print("user_datbase created/exists.")
    # collection = db[mongo_collection]

    # properties = {
    #     'bootstrap.servers': '10.0.0.244:29092',
    #     'group.id': 'flink-mongo-group',
    # }

    # # First step is to listen to valid customer session topic coming from kafka producer
    # consumer = FlinkKafkaConsumer(
    #     topics=kafka_topic,
    #     deserialization_schema=SimpleStringSchema(),
    #     auto_offset_reset='earliest',  # Start reading at the earliest message
    #     properties=properties
    # )
    
    # print(f"Consuming messages from topic: {kafka_topic}")
    # for message in consumer:
    #     try:
    #         # Parse the message value from bytes to dict
    #         record = json.loads(message.value.decode('utf-8'))
    #         # Insert the record into MongoDB
    #         collection.insert_one(record)
    #         print(f"Inserted record into {mongo_collection}: {record}")
    #     except Exception as e:
    #         print(f"Error processing message: {e}")
            
    # def create_kafka_sink(brokers, topic_name):
    # return KafkaSink.builder() \
    #     .set_bootstrap_servers(brokers) \
    #     .set_record_serializer(
    #         KafkaRecordSerializationSchema.builder()
    #             .set_topic(topic_name)
    #             .set_value_serialization_schema(SimpleStringSchema())
    #             .build()
    #     ) \
    #     .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    #     .build()
    
    # Attach the sinks
    #session_summary_stream.sink_to(session_summary_sink)
    #product_details_stream.sink_to(product_details_sink)
    #sentiment_product_stream.sink_to(sentiment_product_details_sink)

    # MongoDB ingestion from the above streams
    # Consume from session_summary topic to session_summary collection
    #consume_topic_to_mongodb('session_summary_topic', 'session_summary')

    # Consume from product_details topic to product_details collection
   # consume_topic_to_mongodb('product_details_topic', 'product_details')

    
    #collection.insert_one(value)
