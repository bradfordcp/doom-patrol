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
    id = __astra.create("widgets", self.geo)
    # save hashes to geo indexes
    return True

  @staticmethod
  def intersects_with(geo):
    # break down hashes for geo and turn them into queries

    # return all documents that match unique ids from queries
    return {}
