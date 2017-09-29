import unittest
from psco import PSCO
from pycompss.api.task import task
from pycompss.api.parameter import *

@task(returns=int)
def compute_sum(psco):
    return sum(psco.get_content())

@task(psco = INOUT)
def modifier_task(psco):
    psco.set_content('Goodbye world')
    from copy import deepcopy
    identifier = psco.getID()
    psco.delete_persistent()
    psco.make_persistent(identifier)

@task(returns=PSCO)
def creator_task(obj):
    myPSCO = PSCO(obj)
    myPSCO.make_persistent()
    return myPSCO

class TestRedis(unittest.TestCase):
    def testMakePersistent(self):
        myPSCO = PSCO('Hello world')
        myPSCO.make_persistent()
        self.assertTrue(myPSCO.getID() is not None)

    def testDeletePersistent(self):
        myPSCO = PSCO('Hello world')
        myPSCO.make_persistent()
        self.assertFalse(myPSCO.getID() is None)
        myPSCO.delete_persistent()
        self.assertTrue(myPSCO.getID() is None)

    def testPSCOisCorrectlyRead(self):
        from pycompss.api.api import compss_wait_on as sync
        myPSCO = PSCO([1, 2, 3, 4, 5])
        myPSCO.make_persistent()
        res = compute_sum(myPSCO)
        res = sync(res)
        self.assertEqual(res, 15)

    def testPSCOisCorrectlyModifiedInsideTask(self):
        from pycompss.api.api import compss_wait_on as sync
        myPSCO = PSCO('Hello world')
        modifier_task(myPSCO)
        myPSCO = sync(myPSCO)
        self.assertEqual('Goodbye world', myPSCO.get_content())

    def testPSCOisCorrectlyCreatedInsideTask(self):
        from pycompss.api.api import compss_wait_on as sync
        obj = list(range(100))
        myPSCO = creator_task(obj)
        myPSCO = sync(myPSCO)
        self.assertEqual(list(range(100)), myPSCO.get_content())
