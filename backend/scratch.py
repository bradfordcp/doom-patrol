import os
from astra import AstraClient
import asyncio

print("a")

async def a_get_many(c, i):
  ac = AstraClient.new().documents()
  return await ac.get_many(c, i)

def test():
  d = AstraClient.new().documents()
  #uuid = d.create("widgets", {'foo': "baz"})
  documents = asyncio.run(a_get_many("events", {"ef0f4d33-0341-4ed3-846b-ac4fa9ea2d0a", "ef0f4d33-0341-4ed3-846b-ac4fa9ea2d0a"}))

  print(documents)


test()




# # Create
# id = d.create("widgets", {'foo': "baz"})

# # Get
# document = d.get("widgets", id)

# # Replace - throwing an error right now
# # e = d.put("widgets", c, {'bar':'bif'})

# # Patch
# id2 = d.patch("widgets", id, {'doom?': 'doom2'})

# # Get
# document2 = d.get("widgets", id2)

# # Delete
# id3 = d.delete("widgets", id2)

# # Get - throws an error
# try:
#     document3 = d.get("widgets", id3)
# except RuntimeError as err:
#     print("Success")
