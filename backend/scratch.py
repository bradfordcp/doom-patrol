import os
from astra import AstraClient

a = AstraClient.from_environment()

# Create
c = a.create("widgets", {'foo': "baz"})

# Get
d = a.get("widgets", c)

# Replace - throwing an error right now
# e = a.put("widgets", c, {'bar':'bif'})

# Patch
f = a.patch("widgets", c, {'doom?': 'doom2'})

# Get
g = a.get("widgets", c)

# Delete
h = a.delete("widgets", c)

# Get - throws an error
i = a.get("widgets", c)
