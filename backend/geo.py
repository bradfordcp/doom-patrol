from polygon_geohasher.polygon_geohasher import polygon_to_geohashes, geohashes_to_polygon
import geojson
import shapely
from astra import AstraClient

class Geo:
  __astra = AstraClient.new().documents()
  __kastra = AstraClient.new().keyspaces()
  def __init__(self, geo):
    print(geo)
    poly = shapely.geometry.shape(geo.geometry)
    self.hashes = polygon_to_geohashes(poly, 3, inner=False)
    self.geo = geo

  def save(self):
    # save geojson to document store
    id = self.__astra.patch("events", self.geo['id'], self.geo)
    # save hashes to geo indexes
    for hash in self.hashes:
      self.__kastra.insert("geohash", {"hash": hash, "id": id})

    # TODO get to an async verion of this
    #result = map(lambda x: {'geohash': x, 'eid': id}, self.hashes)
    #self.__astra.save_all("geo7", result)
    return id

  def intersects_with(self):
    # break down hashes for geo and turn them into queries

    ids = set()
    for hash in self.hashes:
      result = self.__kastra.query_pk("geohash", [hash])
      for d in result['data']:
        ids.add(d['id'])
    
    #TODO get an async version of this running
    #ids = __astra.find_all("geo7", geo.hashes)
    results = []
    for id in ids:
      results.append(self.__astra.get("events", id))

    # return all documents that match unique ids from queries
    return results
