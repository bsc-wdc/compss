'''@author: srodrig1

PyCOMPSs Delete Object test
=========================
    This file represents PyCOMPSs Testbench.
    Checks the delete object functionality.
'''
import unittest
from pycompss.api.api import compss_wait_on, compss_delete_object
from tasks import increment_object

class testDeleteObject(unittest.TestCase):

    def testDeleteObject(self):
        from pycompss.runtime.binding import pending_to_synchronize, objid_to_filename, get_object_id
        obj_1 = [0]
        obj_2 = increment_object(obj_1)
        obj_2 = compss_wait_on(obj_2)
        obj_1_id = get_object_id(obj_1, False, False)
        deletion_result = compss_delete_object(obj_1)
        self.assertTrue(deletion_result)
        self.assertFalse(obj_1_id in pending_to_synchronize)
        self.assertTrue(get_object_id(obj_1, False, False) is None)
