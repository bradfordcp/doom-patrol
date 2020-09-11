import unittest
import os

loader = unittest.TestLoader()
suite = loader.discover(os.path.dirname(os.path.realpath(__file__)))

runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
