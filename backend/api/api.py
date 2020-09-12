from flask import Flask, redirect, url_for, request, jsonify
from flask_restful import Api, Resource, reqparse
import json
import requests
from .address2latlong import geofind
from flask_cors import CORS, cross_origin
from geo import Geo
from  geojson import Point, Feature, FeatureCollection
import geojson
import pprint

class add_address(Resource):
    def get(self):
        parser = reqparse.RequestParser()

        parser.add_argument("address", default="")

        args = parser.parse_args()
        
        address = args.get('address')

        latlong = geofind(address) 

        #insert into database address, lat, long

class spoof_get_events(Resource):
    
    @cross_origin()
    def get(self):
        parser = reqparse.RequestParser()

        parser.add_argument("points", default="")
        parser.add_argument("time_range", default="")
        parser.add_argument("type", default="")

        args = parser.parse_args()
        
        points = args.get('points')
        time_range = args.get('time_range')
        event_type = args.get('type')

        #rest api call to stargate
        #stargate_call = 
        #select data from stargate
        #response = requests.get(BASE + "stargate_call")
        #prossess response
        #if query success:
        return {"data":[{
            "type": "FeatureCollection",
            "features": [
                {
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            -76.9750541388,
                            38.8410857803
                        ]
                    },
                    "type": "Feature",
                    "properties": {
                        "type": "Utility",
                        "marker-symbol": "water",
                        "title": "Blown Water Main",
                        "url": "http://example.com"
                    }
                }
            ]
        }]}
        #else:
            # return error_query_fail(query)


class get_events_by_point(Resource):
    
    @cross_origin()
    def post(self):

        geojson_payload = geojson.loads(json.dumps(request.json))

        results = Geo(geojson_payload).intersects_with()

        geojson_results = []
        for result in results:
            geojson_results.append(Feature(id=result.get('id'), properties=result.get('properties'), geometry=Point(result['geometry']['coordinates'])))

        return jsonify(FeatureCollection(geojson_results))

class get_events_by_address(Resource):
    
    @cross_origin()
    def get(self, address):

        latlong_address = geofind().lookuplatlong(address)

        # latlong_address = {'longitude':-118.0768, 'latitude':38.0512}

        point_geojson = Point((latlong_address['longitude'], latlong_address['latitude']))

        geojson_results = []
        results = Geo(Feature(geometry=point_geojson)).intersects_with()
        for result in results:
            geojson_results.append(Feature(id=result.get('id'), properties=result.get('properties'), geometry=Point(result['geometry']['coordinates'])))

        return jsonify(FeatureCollection(geojson_results))

class jackson():
    def __init__(self):
        self.app = Flask(__name__)
        api = Api(self.app)
        cors = CORS(self.app)
        api.add_resource(get_events_by_address, "/api/get_events_by_address/address=<address>")
        api.add_resource(get_events_by_point, "/api/get_events_by_point/")
        api.add_resource(spoof_get_events, "/api/spoof_get_events.json/")
        api.add_resource(add_address, "/api/add_address/")

    def run(self):
        self.app.run(debug=True)

    def test_client(self):
        return self.app.test_client()
