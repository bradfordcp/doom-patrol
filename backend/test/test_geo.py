import unittest
import geo
import geojson

class TestGeo(unittest.TestCase):

  def test_point_creates_single_hash(self):
    g = geo.Geo(geojson.utils.generate_random("Point"))
    self.assertEqual(len(g.hashes), 1)
    
