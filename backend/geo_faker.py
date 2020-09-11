import geo
import geojson
for x in range(10000):
  print(geo.Geo(geojson.utils.generate_random("Point")).save())