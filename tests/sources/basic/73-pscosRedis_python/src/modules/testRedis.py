#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest

from .psco import PSCO
from .psco_with_tasks import PSCOWithTasks

from pycompss.api.task import task
from pycompss.api.parameter import INOUT


@task(returns=int)
def compute_sum(psco):
    return sum(psco.get_content())


@task(returns=PSCO)
def modifier_task(psco):
    psco.set_content('Goodbye world')
    return psco


@task(returns=PSCO)
def creator_task(obj):
    myPSCO = PSCO(obj)
    return myPSCO


@task(returns=list)
def selfConcat(a, b):
    a.set_content(a.get_content() * 2)
    b.set_content(b.get_content() * 2)
    return [a, b]


@task(returns=PSCO)
def inc(x):
    x.content += 1
    return x


@task(returns=str)
def psco_persister(psco):
    psco.make_persistent()
    return psco.getID()


@task(psco=INOUT, returns=str)
def psco_persister_inout(psco):
    psco.make_persistent()
    return psco.getID()


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

    def testTaskPersister(self):
        from pycompss.api.api import compss_wait_on as sync
        a = PSCO('Persisted in task')
        ID = psco_persister(a)
        ID = sync(ID)
        from storage.api import getByID
        an = getByID(ID)
        self.assertEqual('Persisted in task', an.get_content())

    def testTaskPersister_inout(self):
        from pycompss.api.api import compss_wait_on as sync
        a = PSCO('Persisted in task')
        newId = psco_persister_inout(a)
        b = sync(a)
        newId = sync(newId)
        self.assertEqual(a.getID(), None)
        self.assertNotEqual(b.getID(), None)
        self.assertNotEqual(a.getID(), b.getID())
        self.assertEqual(b.getID(), newId)
        from storage.api import getByID
        bn = getByID(newId)
        self.assertEqual(a.get_content(), b.get_content(), bn.get_content())
