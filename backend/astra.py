import os
import requests
import uuid
from datetime import datetime,timezone
import json

class AstraClient:
    __REFRESH_INTERVAL = 1800

    __instance = None
    def __new__(cls, database_id, region, username, password):
        if AstraClient.__instance is None:
            AstraClient.__instance = object.__new__(cls)
        
        AstraClient.__instance.database_id = database_id
        AstraClient.__instance.region = region
        AstraClient.__instance.username = username
        AstraClient.__instance.password = password
        AstraClient.__instance.__token = None
        AstraClient.__instance.__token_refreshed_at = None
        AstraClient.__instance.__API_ROOT = f"https://{database_id}-{region}.apps.astra.datastax.com/api/rest"

        return AstraClient.__instance
    
    def from_environment():
        db = os.environ['ASTRA_DATABASE_ID']
        region = os.environ['ASTRA_REGION']
        username = os.environ['ASTRA_DATABASE_USERNAME']
        password = os.environ['ASTRA_DATABASE_PASSWORD']

        client = AstraClient(db, region, username, password)

        if 'ASTRA_KEYSPACE' in os.environ:
            return client.documents(os.environ['ASTRA_KEYSPACE'])
        else:
            return client
    
    def documents(self, keyspace):
        return AstraDocuments(self, keyspace)

    def __needs_refresh(self):
        return self.__token_refreshed_at == None or (datetime.now(timezone.utc) - self.__token_refreshed_at).seconds > __REFRESH_INTERVAL
    
    def __refresh_token(self):
        if  self.__needs_refresh():
            url = f"{self.__api_root()}/v1/auth"
            headers = self.__unauthenticated_headers()
            body = {
                'username': self.username,
                'password': self.password
            }

            resp = requests.post(url, headers=headers, data=json.dumps(body))
            
            if resp.status_code == requests.codes.created:
                auth_info = resp.json()
                self.__token = auth_info['authToken']
                self.__token_refreshed_at = datetime.now(timezone.utc)
            else:
                raise RuntimeError("Could not create token")
    
    def __unauthenticated_headers(self, existing={}):
        existing.update({
            'X-Cassandra-Request-Id': uuid.uuid4().hex,
            'Content-Type': 'application/json'
        })
        return existing

    def __authenticated_headers(self, existing={}):
        self.__refresh_token()
        existing.update(self.__unauthenticated_headers())
        existing.update({
            'X-Cassandra-Token': self.__token,
        })
        return existing

    def __api_root(self):
        return f"https://{self.database_id}-{self.region}.apps.astra.datastax.com/api/rest"
    
    def get(self, path="", headers={}, **kwargs):
        url = f"{self.__api_root()}{path}"
        headers = self.__authenticated_headers(headers)
        return requests.get(url, headers=headers, **kwargs)

    def post(self, path="", headers={}, **kwargs):
        print(kwargs)
        url = f"{self.__api_root()}{path}"
        headers = self.__authenticated_headers(headers)
        return requests.post(url, headers=headers, **kwargs)
    
    def put(self, path="", headers={}, **kwargs):
        url = f"{self.__api_root()}{path}"
        headers = self.__authenticated_headers(headers)
        return requests.put(url, headers=headers, **kwargs)
    
    def patch(self, path="", headers={}, **kwargs):
        url = f"{self.__api_root()}{path}"
        headers = self.__authenticated_headers(headers)
        return requests.patch(url, headers=headers, **kwargs)
    
    def delete(self, path="", headers={}, **kwargs):
        url = f"{self.__api_root()}{path}"
        headers = self.__authenticated_headers(headers)
        return requests.delete(url, headers=headers, **kwargs)

class AstraDocuments:
    def __init__(self, client, keyspace):
        self.client = client
        self.keyspace = keyspace
    
    def create(self, collection, document=dict(), id=None):    
        path = f"/v2/namespaces/{self.keyspace}/collections/{collection}"
        resp = self.client.post(path, data=json.dumps(document))
        if resp.status_code == requests.codes.created:
            return resp.json()
        else:
            raise RuntimeError(f"{resp.status_code} response received. Expected 201")
