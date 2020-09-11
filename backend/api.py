
from flask import Flask, redirect, url_for, request
from flask_restful import Api, Resource, reqparse
import json
import requests


BASE = "http://127.0.0.1:5000/"
state = {"data" : "hello world"}

def error_key_not_found(key):
    return {"status":"error", "error":"key "+key+" not found"}

def error_query_fail(key):
    return {"status":"error", "error":"query "+key+" to stargate failed"}


def error_key_already_exists(key):
    return {"status":"error", "error":"key "+key+" already exists"}

class get_state(Resource):
    def get(self):
        return state
    def post(self):
        return state

class get_key(Resource):
    def get(self, key):
        if key in state:
            return state[key]
        else:
            return error_key_not_found(key)
    def post(self, key):
        if key in state:
            return state[key]
        else:
            return error_key_not_found(key)

class insert(Resource):
    def get(self,key,value):
        if key in state:
            return error_key_already_exists(key)
        else:
            state[key] = value
            return {"status":"success"}
    def post(self,key,value):
        if key in state:
            return error_key_already_exists(key)
        else:
            state[key] = value
            return {"status":"success"}

class delete(Resource):
    def get(self,key):
        if key in state:
            state.pop(key, None)
            return {"status":"success"}
        else:
            return error_key_not_found(key)
    def post(self, key):
        if key in state:
            state.pop(key, None)
            return {"status":"success"}
        else:
            return error_key_not_found(key)

class update(Resource):
    def get(self, key, value):
        if key in state:
            state[key] = value
            return {"status":"success"}
        else:
            return error_key_not_found(key)
    def post(self, key, value):
        if key in state:
            state[key] = value
            return {"status":"success"}
        else:
            return error_key_not_found(key)



class spoof_get_events(Resource):
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
        app = Flask(__name__)
        api = Api(app)
        api.add_resource(get_state, "/api/get_state")
        api.add_resource(get_key, "/api/get_key/<string:key>")
        api.add_resource(spoof_get_events, "/api/spoof_get_events/")
        api.add_resource(insert, "/api/insert/<string:key>=<string:value>")
        api.add_resource(delete, "/api/delete/<string:key>")
        api.add_resource(update, "/api/update/<string:key>=<string:value>")
        app.run(debug=True)
    
