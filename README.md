# BigData288-Project
BigData228 Project
Group2_projectreport.pdf contains the details of the project. 

The landscape of e-commerce analytics is rapidly
evolving with the advent of real-time data processing and
advanced machine learning techniques. A critical challenge in this
domain is the effective integration of real-time analytics with deep
offline analysis to enhance user experience and operational efficiency. 
Prior works have predominantly focused on either offline batch processing 
for user behavior analysis or real-time monitoring without deeper predictive insights. 
Our work bridges this gap by introducing a hybrid analytical framework that leverages both
real-time stream processing and comprehensive offline machine
learning models while employing cutting edge technologies. Our
methodology involves generating synthetic datasets representing
a hypothetical furniture website’s sessions containing users, 
session and products information.
This data is streamed to Apache Kafka and then utilized by
Redis for immediate security checks while computing unique
users and session counts per user. The data is then ingested into
MongoDB for batch processing, where Locality
Sensitive Hashing (LSH) is implemented to identify similar users and 
also experiment with machine learning models like Random Forest, XGBoost, and
Artificial Neural Networks to predict if the user is going to make a purchase or not. 
These models are augmented with differential privacy to safeguard user information 
and utilize Explainable AI technique, SHAP, for model interpretability.
Concurrently, the real-time component of our pipeline leverages
Apache Flink’s capabilities to process the valid user sessions data further, which
is then sent to Elasticsearch known for its powerful full-text
search and analytics engine. Subsequently, Kibana is employed
for its robust visualization features, enabling stakeholders to gain
insights into user behavior, product performance, and session
activity dynamically.This holistic approach ensures a fine balance
between real-time responsiveness and the strategic depth of
offline analysis. Users benefit from personalized experiences and
improved service, while businesses gain a dual advantage of
immediate intelligence and long-term strategic insights, paving
the way for enhanced data-driven decision-making in e-commerce
platforms. Following figures shows the project workflow and algorithms stack used.

![image](https://github.com/DecipherData/BigData288-Project/assets/96799273/15f657c6-da1c-46ca-b0b6-1a93324158ab) 

![image](https://github.com/DecipherData/BigData288-Project/assets/96799273/04b5f3e0-1a1a-40b5-a191-388bafdf681e)


