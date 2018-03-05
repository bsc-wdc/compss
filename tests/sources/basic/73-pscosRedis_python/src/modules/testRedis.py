import unittest
from .psco import PSCO
from .psco_with_tasks import PSCOWithTasks
from pycompss.api.task import task
from pycompss.api.parameter import *

@task(returns = int)
def compute_sum(psco):
    return sum(psco.get_content())

@task(returns = PSCO)
def modifier_task(psco):
    psco.set_content('Goodbye world')
    return psco

@task(returns = PSCO)
def creator_task(obj):
    myPSCO = PSCO(obj)
    return myPSCO

@task(returns = list)
def selfConcat(a, b):
    a.set_content(a.get_content() * 2)
    b.set_content(b.get_content() * 2)
    return [a, b]

@task(returns = PSCO)
def inc(x):
    x.content +=  1
    return x

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
        myPSCO = modifier_task(myPSCO)
        myPSCO = sync(myPSCO)
        self.assertEqual('Goodbye world', myPSCO.get_content())

    @unittest.skip("TEMPORARY")
    def testPSCOisCorrectlyCreatedInsideTask(self):
        from pycompss.api.api import compss_wait_on as sync
        myPSCO = creator_task(obj)
        obj = list(range(100))
        myPSCO = sync(myPSCO)
        self.assertEqual(list(range(100)), myPSCO.get_content())

    def testPipeline(self):
        a = PSCO('a')
        b = PSCO('b')
        c = PSCO('c')
        a.make_persistent()
        b.make_persistent()
        c.make_persistent()
        from storage.api import getByID
        an, bn, cn = getByID(a.getID(), b.getID(), c.getID())
        self.assertEqual(a.get_content(), an.get_content())
        self.assertEqual(b.get_content(), bn.get_content())
        self.assertEqual(c.get_content(), cn.get_content())

    def testMultiParam(self):
        from pycompss.api.api import compss_wait_on as sync
        a = PSCO('a')
        b = PSCO('b')
        a.make_persistent()
        b.make_persistent()
        l = selfConcat(a, b)
        l = sync(l)
        a, b = l
        self.assertEqual('aa', a.get_content())
        self.assertEqual('bb', b.get_content())

    @unittest.skip("UNSUPPORTED IN REDIS")
    def testPSCOwithTasks(self):
        from pycompss.api.api import compss_wait_on as sync
        obj = PSCOWithTasks("Hello world")
        obj.make_persistent()
        initialContent = obj.get_content()
        obj.set_content("Goodbye world")
        modifiedContent = obj.get_content()
        iC = sync(initialContent)
        mC = sync(modifiedContent)
        self.assertEqual('Hello world', iC)
        self.assertEqual('Goodbye world', mC)

    def testAutoModification(self):
        from pycompss.api.api import compss_wait_on as sync
        p = creator_task(0)
        p = inc(p)
        p = inc(p)
        p = sync(p)
        self.assertEqual(2, p.get_content())
