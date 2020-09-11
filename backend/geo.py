from polygon_geohasher.polygon_geohasher import polygon_to_geohashes, geohashes_to_polygon
import geojson
import shapely
class Geo:
  def __init__(self, geo):
    poly = shapely.geometry.shape(geo)
    self.hashes = polygon_to_geohashes(poly, 7, inner=False)







