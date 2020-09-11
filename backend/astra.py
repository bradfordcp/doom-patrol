import os
import requests
import uuid
from datetime import datetime,timezone
import json
import urllib

class AstraClient:
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
        AstraClient.__instance.__REFRESH_INTERVAL = 1800

        return AstraClient.__instance

    def new():
        db = os.environ['ASTRA_DATABASE_ID']
        region = os.environ['ASTRA_REGION']
        username = os.environ['ASTRA_DATABASE_USERNAME']
        password = os.environ['ASTRA_DATABASE_PASSWORD']

        return AstraClient(db, region, username, password)
    
    def documents(self, keyspace=None):
        if keyspace == None and 'ASTRA_KEYSPACE' in os.environ:
            keyspace = os.environ['ASTRA_KEYSPACE']
            
        return AstraDocuments(self, keyspace)
    
    def keyspaces(self, keyspace=None):
        if keyspace == None and 'ASTRA_KEYSPACE' in os.environ:
            keyspace = os.environ['ASTRA_KEYSPACE']
        
        return AstraKeyspaces(self, keyspace)

    def __needs_refresh(self):
        return self.__token_refreshed_at == None or (datetime.now(timezone.utc) - self.__token_refreshed_at).seconds > self.__REFRESH_INTERVAL
    
    def __refresh_token(self):
        if  self.__needs_refresh():
            url = self.__url_for("/v1/auth")
            headers = self.__unauthenticated_headers()
            body = {
                'username': self.username,
                'password': self.password
            }

            resp = requests.post(url, headers=headers, json=body)
            
            if resp.status_code == requests.codes.created or resp.status_code == requests.codes.ok:
                auth_info = resp.json()
                self.__token = auth_info['authToken']
                self.__token_refreshed_at = datetime.now(timezone.utc)
            else:
                raise RuntimeError("Could not create token")
    
    def __url_for(self, path=""):
        return f"https://{self.database_id}-{self.region}.apps.astra.datastax.com/api/rest{path}"
    
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

    def get(self, path="", headers={}, **kwargs):
        url = self.__url_for(path)
        headers = self.__authenticated_headers(headers)
        return requests.get(url, headers=headers, **kwargs)

    def post(self, path="", headers={}, **kwargs):
        url = self.__url_for(path)
        headers = self.__authenticated_headers(headers)
        return requests.post(url, headers=headers, **kwargs)
    
    def put(self, path="", headers={}, **kwargs):
        url = self.__url_for(path)
        headers = self.__authenticated_headers(headers)
        return requests.put(url, headers=headers, **kwargs)
    
    def patch(self, path="", headers={}, **kwargs):
        url = self.__url_for(path)
        headers = self.__authenticated_headers(headers)
        return requests.patch(url, headers=headers, **kwargs)
    
    def delete(self, path="", headers={}, **kwargs):
        url = self.__url_for(path)
        headers = self.__authenticated_headers(headers)
        return requests.delete(url, headers=headers, **kwargs)

class AstraDocuments:
    def __init__(self, client, keyspace):
        self.client = client
        self.keyspace = keyspace
    
    def create(self, collection, document={}, id=None):
        if id != None:
            return self.replace(collection, id, document)
        else:
            path = f"/v2/namespaces/{self.keyspace}/collections/{collection}"
            resp = self.client.post(path, json=document)
            if resp.status_code == requests.codes.created:
                return resp.json()['documentId']
            else:
                raise RuntimeError(f"{resp.status_code} response received.\n\n{resp.url}\n\n{resp.text}")
    
    def get(self, collection, id):
        path = f"/v2/namespaces/{self.keyspace}/collections/{collection}/{id}"
        resp = self.client.get(path)
        
        if resp.status_code == requests.codes.ok:
            return resp.json()['data']
        else:
            raise RuntimeError(f"{resp.status_code} response received.\n\n{resp.url}\n\n{resp.text}")

    def put(self, collection, id, document={}):
        path = f"/v2/namespaces/{self.keyspace}/collections/{collection}/{id}"
        resp = self.client.put(path, json=document)
        
        if resp.status_code == requests.codes.ok:
            return resp.json()['documentId']
        else:
            raise RuntimeError(f"{resp.status_code} response received.\n\n{resp.url}\n\n{resp.text}")
    
    def patch(self, collection, id, document={}):
        path = f"/v2/namespaces/{self.keyspace}/collections/{collection}/{id}"
        resp = self.client.patch(path, json=document)
        
        if resp.status_code == requests.codes.ok:
            return resp.json()['documentId']
        else:
            raise RuntimeError(f"{resp.status_code} response received.\n\n{resp.url}\n\n{resp.text}")
    
    def delete(self, collection, id):
        path = f"/v2/namespaces/{self.keyspace}/collections/{collection}/{id}"
        resp = self.client.delete(path)
        
        if resp.status_code == requests.codes.no_content:
            return id
        else:
            raise RuntimeError(f"{resp.status_code} response received.\n\n{resp.url}\n\n{resp.text}")

    def query(self, collection, where={}):
        path = f"/v2/namespaces/{self.keyspace}/collections/{collection}"
        params = {
            'where': json.dumps(where)
        }
        
        resp = self.client.get(path, params=params)
        
        if resp.status_code == requests.codes.ok:
            return resp.json()
        elif resp.status_code == requests.codes.no_content:
            return None
        else:
            raise RuntimeError(f"{resp.status_code} response received.\n\n{resp.url}\n\n{resp.text}")


class AstraKeyspaces:
    def __init__(self, client, keyspace):
        self.client = client
        self.keyspace = keyspace

    def query(self, table, where={}):
        path = f"/v2/keyspaces/{self.keyspace}/{table}"
        params = {
            'where': json.dumps(where)
        }
        resp = self.client.get(path, params=params)
        
        if resp.status_code == requests.codes.ok:
            return resp.json()
        else:
            raise RuntimeError(f"{resp.status_code} response received.\n\n{resp.url}\n\n{resp.text}")
    
    def query_pk(self, table, primary_key):
        primary_key_path = "/".join(primary_key)
        path = f"/v2/keyspaces/{self.keyspace}/{table}/{primary_key_path}"
        resp = self.client.get(path)
        
        if resp.status_code == requests.codes.ok:
            return resp.json()
        else:
            raise RuntimeError(f"{resp.status_code} response received.\n\n{resp.url}\n\n{resp.text}")
    
    def insert(self, table, data={}):
        path = f"/v2/keyspaces/{self.keyspace}/{table}"
        resp = self.client.post(path, json=data)
        
        if resp.status_code == requests.codes.created:
            return resp.json()
        else:
            raise RuntimeError(f"{resp.status_code} response received.\n\n{resp.url}\n\n{resp.text}")
    
    def put(self, table, primary_key, data={}):
        primary_key_path = "/".join(primary_key)
        path = f"/v2/keyspaces/{self.keyspace}/{table}/{primary_key_path}"
        resp = self.client.put(path, json=data)
        
        if resp.status_code == requests.codes.ok:
            return resp.json()
        else:
            raise RuntimeError(f"{resp.status_code} response received.\n\n{resp.url}\n\n{resp.text}")
    
    def patch(self, table, primary_key, data={}):
        primary_key_path = "/".join(primary_key)
        path = f"/v2/keyspaces/{self.keyspace}/{table}/{primary_key_path}"
        resp = self.client.patch(path, json=data)
        
        if resp.status_code == requests.codes.ok:
            return resp.json()
        else:
            raise RuntimeError(f"{resp.status_code} response received.\n\n{resp.url}\n\n{resp.text}")
    
    def delete(self, table, primary_key):
        primary_key_path = "/".join(primary_key)
        path = f"/v2/keyspaces/{self.keyspace}/{table}/{primary_key_path}"
        resp = self.client.delete(path)
        
        if resp.status_code == requests.codes.no_content:
            return {}
        else:
            raise RuntimeError(f"{resp.status_code} response received.\n\n{resp.url}\n\n{resp.text}")
