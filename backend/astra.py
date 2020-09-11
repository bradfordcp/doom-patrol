import os
import requests
import uuid
from datetime import datetime,timezone
import json

class Astra:
    __REFRESH_INTERVAL = 1800

    def __init__(self, database_id, region, username, password, keyspace):
        self.database_id = database_id
        self.region = region

        self.__API_ROOT = f"https://{self.database_id}-{self.region}.apps.astra.datastax.com/api/rest"
        
        self.username = username
        self.password = password
        self.keyspace = keyspace

        self.__token = None
        self.__token_refreshed_at = None
    
    def __needs_refresh(self):
        return self.__token_refreshed_at == None or (datetime.now(timezone.utc) - self.__token_refreshed_at).seconds > __REFRESH_INTERVAL
    
    def __refresh_token(self):
        if  self.__needs_refresh():
            url = f"{self.__API_ROOT}/v1/auth"
            headers = {
                'X-Cassandra-Request-Id': uuid.uuid4().hex,
                'Content-Type': "application/json"
            }
            body = {
                'username': self.username,
                'password': self.password
            }

            resp = requests.post(url, headers=headers, data=json.dumps(body))
            
            if resp.status_code == requests.codes.created:
                auth_info = resp.json()
                self.__token = auth_info['authToken']
                self.__token_refreshed_at = datetime.now(timezone.utc)
    
    def create_document(self, collection, document=dict(), id=None):
        self.__refresh_token()
        
        url = f"{self.__API_ROOT}/v2/namespaces/{self.keyspace}/collections/{collection}"
        headers = {
            'X-Cassandra-Request-Id': uuid.uuid4().hex,
            'X-Cassandra-Token': self.__token,
            'Content-Type': "application/json"
        }
        
        resp = requests.post(url, headers=headers, data=json.dumps(document))
        if resp.status_code == requests.codes.created:
            return resp.json()
        else:
            return resp.status_code
