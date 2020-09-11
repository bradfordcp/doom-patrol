import os
from astra import AstraClient

a = AstraClient.from_environment()
d = a.create("widgets", {'foo': "bar"})
