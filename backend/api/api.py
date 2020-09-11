from flask import Flask, redirect, url_for, request
from flask_restful import Api, Resource, reqparse
import json
import requests
# from backend.address2latlog import geofind
from flask_cors import CORS, cross_origin

BASE = "http://127.0.0.1:5000"
state = {"data" : "hello world"}

def error_key_not_found(key):
    return {"status":"error", "error":"key "+key+" not found"}

def error_query_fail(key):
    return {"status":"error", "error":"query "+key+" to stargate failed"}

def error_key_already_exists(key):
    return {"status":"error", "error":"key "+key+" already exists"}

class add_address(Resource):
    def get(self):
        parser = reqparse.RequestParser()

        parser.add_argument("address", default="")

        args = parser.parse_args()
        
        address = args.get('address')

        # latlong = geofind(address) 

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



class jackson():
    def __init__(self):
        self.app = Flask(__name__)
        api = Api(self.app)
        cors = CORS(self.app)
        api.add_resource(spoof_get_events, "/api/spoof_get_events/")
        api.add_resource(add_address, "/api/add_address/")

    def run(self):
        self.app.run(debug=True)

    def test_client(self):
        return self.app.test_client()
