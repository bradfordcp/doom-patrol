from polygon_geohasher.polygon_geohasher import polygon_to_geohashes, geohashes_to_polygon
import geojson
import shapely
from astra import AstraClient

class Geo:
  __astra = AstraClient.new().documents()
  def __init__(self, geo):
    poly = shapely.geometry.shape(geo)
    self.hashes = polygon_to_geohashes(poly, 7, inner=False)
    self.geo = geo

  def save(self):
    print(self.hashes)
    # save geojson to document store
    id = self.__astra.create("events", self.geo)
    # save hashes to geo indexes
    result = map(lambda x: {'geohash': x, 'eid': id}, self.hashes) 
    self.__astra.save_all("geo7", result)
    #print(list(result)) 
    return id

  def intersects_with(geo):
    # break down hashes for geo and turn them into queries
    ids = __astra.find_all("geo7", geo.hashes)

    # return all documents that match unique ids from queries
    return __astra.get_docs("events", ids)
