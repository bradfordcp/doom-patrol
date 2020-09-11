#!/usr/bin/env python3

from flask import Flask, redirect, url_for, request
from flask_restful import Api, Resource
import json

app = Flask(__name__)
api = Api(app)

state = {"data" : "hello world"}

def error_key_not_found(key):
    return {"status":"error", "error":"key "+key+" not found"}


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

api.add_resource(get_state, "/api/get_state")
api.add_resource(get_key, "/api/get_key/<string:key>")
api.add_resource(insert, "/api/insert/<string:key>=<string:value>")
api.add_resource(delete, "/api/delete/<string:key>")
api.add_resource(update, "/api/update/<string:key>=<string:value>")



if __name__ == "__main__":
    app.run(debug=True)