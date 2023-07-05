import requests
from google.transit import gtfs_realtime_pb2

#Initialize FeedMessage Parser from Google
feed = gtfs_realtime_pb2.FeedMessage()

#Access the realtime data from API
reatlime_url = 'http://cdn.mbta.com/realtime/VehiclePositions.pb'
response = requests.get(reatlime_url)

#Parse the data
feed.ParseFromString(response.content)

#Check data
print (feed.entity)