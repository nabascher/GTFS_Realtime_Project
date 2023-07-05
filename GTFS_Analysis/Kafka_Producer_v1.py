import time
import requests
from confluent_kafka import Producer
from google.transit import gtfs_realtime_pb2

#Configure Kafka producer
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'mbta-realtime'

#retrieve GTFS reatlime data
def fetch_gtfs_realtime_data():
    #Initialize FeedMessage Parser from Google
    feed = gtfs_realtime_pb2.FeedMessage()

    #Access the realtime data from API
    reatlime_url = 'http://cdn.mbta.com/realtime/VehiclePositions.pb'
    response = requests.get(reatlime_url)

    #Parse the data
    feed.ParseFromString(response.content)

    #Return the data in bytes  
    return feed.SerializeToString()

#Create Kafka producer
producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})


#Publish the data to kafka
def publish_gtfs_realtime_data():
    counter = 1
    while True:
        gtfs_realtime_data = fetch_gtfs_realtime_data()

        #produce data to kafka topic
        producer.produce(kafka_topic, value=gtfs_realtime_data)

        #Flush producer and sleep 
        producer.flush()
        
        print(rf"published gtfs realtime data {counter} time(s).")
        counter = counter + 1

        time.sleep(5)
        
publish_gtfs_realtime_data()