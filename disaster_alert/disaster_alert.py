import requests
import tts.carter as carter
import time
import yaml
import json
from geopy.geocoders import Nominatim
from pprint import pprint
from geojson import Point, Feature
import geojson


with open(r'config.yaml') as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    config = yaml.load(file, Loader=yaml.FullLoader)

def parse_event_type(response):
        response = json.loads(response.text)
        data = response['features'][0]['properties']['title']
        return data


headers = {
        'Content-type': 'application/json'
    }

geolocator = Nominatim(user_agent="disaster_alert")

def lookuplatlong(address):
    location = geolocator.geocode(address)
    return {"latitude":location.latitude, "longitude":location.longitude}

geojson_addresses = []
addresses = config['addresses']
for address in addresses:
    location = lookuplatlong(address)
    point_geojson = Point((location['longitude'], location['latitude']))
    geojson_addresses.append(Feature(geometry=point_geojson))
    print(geojson_addresses)


while True:

    for location in geojson_addresses:
        response = requests.post(config['server_ip']+"get_events_by_point/",data=geojson.dumps(location), headers=headers)
        statuscode = response.status_code
        print(statuscode)

        if statuscode == 200:
            event_type = parse_event_type(response)
            carter.say(f"Disaster {event_type} detected near PLACEHOLDER")
        else:
            msg = f'error {statuscode} talking to jackson server'
            print(msg)
            carter.say(msg)



    time.sleep(60)
