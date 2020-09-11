import requests
import tts.carter as carter
import time
import yaml
import json
from geopy.geocoders import Nominatim
from pprint import pprint

with open(r'config.yaml') as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    config = yaml.load(file, Loader=yaml.FullLoader)

def parse_event_type(response):
        response = json.loads(response.text)
        data = response['data'][0]['features'][0]['properties']['title']
        return data

while True:
    #select favorate locations from the database

    #Ask database if there were any desasters at locations
    # for location in locations:
    # rest call to return any active event at *location*, and time now()
    response = requests.get(config['server_ip']+"spoof_get_events.json/")
    statuscode = response.status_code
    print(statuscode)

    # if event found then have carter tell us
    if statuscode == 200:
        event_type = parse_event_type(response)
        carter.say(f"Disaster  {event_type} detected near PLACEHOLDER")
    else:
        msg = f'error {statuscode} talking to jackson server'
        print(msg)
        carter.say(msg)




    time.sleep(60)
