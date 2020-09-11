import time
import requests
import feedparser
from pprint import pprint
from multiprocessing import Process

class Malp():

    def __init__(self):
        self.foo = True

    def collect(self):
        while True:
            self.collect_eq_data()
            self.collect_gdacs_data()
            time.sleep(90)

    def collect_eq_data(self):
        try:
            response = requests.get("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson")
            for feature in response.json().get('features'):
                pprint(feature)
        except Exception as e:
            print("error fetching USGS earthquake data: %s", e)
            return

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
                            "coordinates": [entry['geo_long'],entry['geo_lat']],
                        },
                        'properties': {
                            'type': entry['gdacs_calculationtype'],
                            'title': entry['title']
                        }
                    }
                    pprint(output)
        except Exception as e:
            print("error fetching gdacs data: %s", e)
            return


if __name__ == "__main__":
    p1 = Process(target=Malp().collect)
    p1.start()
    while True:
        time.sleep(100)