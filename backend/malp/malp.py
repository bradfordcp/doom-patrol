import time
import requests
from pprint import pprint
from multiprocessing import Process

class Malp():

    def __init__(self):
        self.foo = True

    def collect(self):
        while True:
            self.collect_eq_data()
            time.sleep(90)

    def collect_eq_data(self):
        response = requests.get("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson")
        for feature in response.json().get('features'):
            pprint(feature)


if __name__ == "__main__":
    p1 = Process(target=Malp().collect)
    p1.start()
    while True:
        print("async!")
        time.sleep(10)