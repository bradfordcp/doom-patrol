import time
import requests
import feedparser
from pprint import pprint
from multiprocessing import Process
from astra import AstraClient, AstraDocuments
from geo import Geo
from geojson import Feature, Point

class Malp():

    def __init__(self):
        self.client = AstraClient.new().documents()

    def collect(self):
        while True:
            self.collect_eq_data()
            self.collect_gdacs_data()
            time.sleep(90)

    def collect_eq_data(self):
        try:
            response = requests.get("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson")
            for feature in response.json().get('features'):
                Geo(Feature(id=feature.get('id'), properties=feature.get('properties'), geometry=Point(feature['geometry']['coordinates']))).save()
        except Exception as e:
            print("error fetching USGS earthquake data: %s", e)
            return
        print("retrieved docs from usgs")

    def collect_gdacs_data(self):
        try:
            d = feedparser.parse('https://gdacs.org/xml/rss.xml')
            for entry in d['entries']:
                if entry.get('gdacs_calculationtype'):
                    output = {
                        'type': 'Feature',
                        'id': entry['id'],
                        'geometry': {
                            'type': 'Point',
                            "coordinates": [float(entry['geo_long']),float(entry['geo_lat'])],
                        },
                        'properties': {
                            'type': entry['gdacs_calculationtype'],
                            'title': entry['title']
                        }
                    }
                    feature = Feature(id=output['id'], properties=output['properties'], geometry=Point(output['geometry']['coordinates']))
                    Geo(feature).save()
        except Exception as e:
            print("error fetching gdacs data: %s", e)
            return
        print("retrieved docs from gdacs")


if __name__ == "__main__":
    p1 = Process(target=Malp().collect)
    p1.start()
    while True:
        time.sleep(100)