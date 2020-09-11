import geo
import geojson
for x in range(100):
  geo.Geo(geojson.utils.generate_random("LineString")).save()