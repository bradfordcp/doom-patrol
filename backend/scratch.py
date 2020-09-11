import os
from astra import Astra

db = os.environ['ASTRA_DATABASE_ID']
region = os.environ['ASTRA_REGION']
username = os.environ['ASTRA_DATABASE_USERNAME']
password = os.environ['ASTRA_DATABASE_PASSWORD']
keyspace = os.environ['ASTRA_KEYSPACE']

a = Astra(db, region, username, password, keyspace)
d = a.create_document("bar", {'foo': "bar"})

