import geojson
for x in range(10000):
  g = geo.Geo(geojson.Feature(geometry=geojson.utils.generate_random("Point")))
  print(g.save())
