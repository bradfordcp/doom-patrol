import requests
import tts.carter as carter
import time
import yaml

with open(r'config.yaml') as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    config = yaml.load(file, Loader=yaml.FullLoader)


while True:
    #select favorate locations from the database

    #Ask database if there were any desasters in or between favorate locations
    response = requests.get(config['server_ip'])
    # print(response.text)

    #alert if disasters are valid 
    time.sleep(60)
