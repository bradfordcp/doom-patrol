import os
from astra import AstraClient
import json

d = AstraClient.new().documents()

# Create
id = d.create("widgets", {'foo': "baz"})

# Get
document = d.get("widgets", id)

# Replace - throwing an error right now
# e = d.put("widgets", c, {'bar':'bif'})

# Patch
id2 = d.patch("widgets", id, {'doom?': 'doom2'})

# Get
document2 = d.get("widgets", id2)

# Delete
id3 = d.delete("widgets", id2)

# Get - throws an error
try:
    document3 = d.get("widgets", id3)
except RuntimeError as err:
    print("Success")

k = AstraClient.new().keyspaces()
rs = k.query("whizzy_pops")
