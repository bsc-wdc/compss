import unittest

from .tnets import EA
from .psco_with_tasks import PSCOWithTasks

from pycompss.api.task import task
from pycompss.api.parameter import INOUT

class TestRedisApp(unittest.TestCase):

    def testTiramisu(self):
        from pycompss.api.api import compss_wait_on
        obj = EA()
        obj.makePersistent()
        image_paths = ["path1", "path2"]
        result = obj.extract_features(4, image_paths, True, False)
        result = compss_wait_on(result)
        print("result: " + str(result))
        self.assertEqual(result, {'a':[1, 2, 3, 4], 'b':[5, 6, 7, 8]})
