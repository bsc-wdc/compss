#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest

import numpy as np

from .my_class import MyClass
from .psco import PSCO
from .psco_with_tasks import PSCOWithTasks

from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import *



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


@task(returns=PSCO)
def psco_persister_return(psco):
    psco.make_persistent()
    return psco


@task(psco=INOUT, returns=str)
def psco_persister_inout(psco):
    psco.make_persistent()
    return psco.getID()

@task(c = COLLECTION_IN, returns = 1)
def select_element(c, i):
    return c[i]

@task(c = {Type: COLLECTION_IN, Depth: 2}, returns = 1)
def select_element_from_array(c, i, j):
    return c[i][j]

@task(c = {Type: COLLECTION_IN, Depth: 4}, returns = 1)
def select_element_from_matrix(c, i, j, k, l):
    return c[i][j][k][l]

@task(returns = PSCO)
def generate_object(seed):
    np.random.seed(seed)
    content = np.random.random()
    elem = PSCO(content)
    elem.make_persistent()
    return elem


def generate_basic_object(seed, persist=False):
    np.random.seed(seed)
    content = np.random.random()
    elem = PSCO(content)
    if persist:
        elem.make_persistent()
    return elem


@task(c={Type: COLLECTION_IN, Depth: 2})
def persist_non_persisted_elements(c):
    for i in c:
        for j in i:
            j.make_persistent()


@task(c=COLLECTION_INOUT)
def increase_elements_persist(c):
    for elem in c:
        elem.increase_content(1.0)


@task(c=COLLECTION_OUT)
def increase_elements_persist_out(c):
    for i in range(len(c)):
        c[i] = PSCO(content=i)
        c[i].increase_content(1.0, update=False)
        c[i].make_persistent()


@task(c=COLLECTION_INOUT)
def increase_elements(c):
    for elem in c:
        elem.increase_content(1.0)


@task(c={"type": COLLECTION_INOUT, "depth": 2})
def increase_elements_depth(c):
    for row in c:
        for elem in row:
            elem.increase_content(1.0)

@task(e = INOUT)
def increase_element(e):
    e.increase_content(1.0)

@task(kol_out={"type": COLLECTION_OUT, "depth": 1})
def append(kol_out, value):
    for i in range(len(kol_out)):
        kol_out[i].append(value)
    return True


@task(kol_out=COLLECTION_OUT)
def modify_obj(kol_out, value):
    for i in range(len(kol_out)):
        kol_out[i].set_test(value)
    return True


@task(kol_out={"type": COLLECTION_OUT, "depth": 2})
def modify_deep_obj(kol_out, to_append, to_modify):
    for i in range(len(kol_out)):
        kol_out[i][0].append(to_append)
        kol_out[i][1].set_test(to_modify)
    return True



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

    def testPSCOisCorrectlyReadAndRead(self):
        from pycompss.api.api import compss_wait_on as sync
        myPSCO = PSCO([1, 2, 3, 4, 5])
        myPSCO.make_persistent()
        res = compute_sum(myPSCO)
        res = sync(res)
        res = sync(res)
        self.assertEqual(res, 15)

    def testPSCOisCorrectlyModifiedInsideTask(self):
        from pycompss.api.api import compss_wait_on as sync
        myPSCO = PSCO('Hello world')
        myPSCO = modifier_task(myPSCO)
        myPSCO = sync(myPSCO)
        self.assertEqual('Goodbye world', myPSCO.get_content())

    def testPSCOisCorrectlyCreatedInsideTask(self):
        from pycompss.api.api import compss_wait_on as sync
        obj = list(range(100))
        myPSCO = creator_task(obj)
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
        self.assertEqual('Hello world', initialContent)
        obj.set_content("Goodbye world")
        modifiedContent = obj.get_content()
        iC = sync(initialContent)
        mC = sync(modifiedContent)
        self.assertEqual('Hello world', iC)
        self.assertEqual('Goodbye world', mC)

    def testPSCOwithTasksPersist(self):
        from pycompss.api.api import compss_wait_on as sync
        obj = PSCOWithTasks("Hello world")
        obj.persist_notIsModifier()
        obj = sync(obj)
        self.assertTrue(obj.getID() is None)
        obj = PSCOWithTasks("Hello world2")
        obj.persist_isModifier()
        obj = sync(obj)
        self.assertTrue(obj.getID() is not None)

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

    def testTaskPersisterReturn(self):
        from pycompss.api.api import compss_wait_on as sync
        a = PSCO('Persisted in task')
        self.assertTrue(a.getID() is None)
        b = psco_persister_return(a)
        b = sync(b)
        self.assertTrue(b.getID() is not None)
        self.assertEqual('Persisted in task', b.get_content())

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

    #############################
    # TESTING WITH COLLECTIONS: #
    #############################

    def testMasterGenerationIn(self):
        matrix = []
        for i in range(10):
            content = np.random.rand(i)
            elem = PSCO(content)
            elem.make_persistent()
            matrix.append(elem)

        fifth_row = compss_wait_on(select_element(matrix, 4))

        self.assertTrue(
            np.allclose(
                matrix[4].get_content(),
                fifth_row.get_content()
            )
        )

    def testWorkerGenerationIn(self):
        matrix = [
            generate_object(i) for i in range(10)
        ]
        fifth_row = compss_wait_on(select_element(matrix, 4))
        for i in range(len(matrix)):
            matrix[i] = compss_wait_on(matrix[i])

        self.assertTrue(
            np.allclose(
                compss_wait_on(matrix[4]).get_content(),
                fifth_row.get_content()
            )
        )

    def testDepthWorkerGenerationIn(self):
        two_two_two_two_matrix = \
            [
                [
                    [
                        [
                            generate_object(8 * i + 4 * j + 2 * k + l) for l in range(2)
                        ] for k in range(2)
                    ] for j in range(2)
                ] for i in range(2)
            ]
        zero_one_zero_one = select_element_from_matrix(two_two_two_two_matrix, 0, 1, 0, 1)

        np.random.seed(4 + 1)
        should = np.random.random()

        self.assertTrue(
            np.allclose(
                compss_wait_on(zero_one_zero_one).get_content(),
                should
            )
        )

    def testWorkerGenerationInout(self):
        # Generate ten random vectors with pre-determined seed
        ten_random_pscos = [generate_object(i) for i in range(10)]
        increase_element(ten_random_pscos[4])
        # Pick the fifth vector from a COLLECTION_IN parameter
        fifth_vector = compss_wait_on(select_element(ten_random_pscos, 4))
        np.random.seed(4)
        self.assertTrue(
            np.allclose(
                fifth_vector.get_content(),
                np.random.random() + 1.0
            )
        )


    def testWorkerGenerationColInBasic(self):
        # Generate two x two random non persistent storage objects vector
        two_two_random_pscos = [[generate_basic_object(1), generate_basic_object(2)],
                                 [generate_basic_object(1), generate_basic_object(2)]]
        persist_non_persisted_elements(two_two_random_pscos)
        result = compss_wait_on(two_two_random_pscos)
        self.assertEqual(len(two_two_random_pscos), len(result))
        self.assertEqual(len(two_two_random_pscos[0]), len(result[0]))
        self.assertEqual(len(two_two_random_pscos[1]), len(result[1]))
        for i in range(len(two_two_random_pscos)):
            for j in range(len(two_two_random_pscos[i])):
                self.assertEqual(two_two_random_pscos[i][j].get_content(),
                                 result[i][j].get_content())

    def testWorkerGenerationColInoutBasic(self):
        # Generate two random non persistent storage objects vector
        two_random_pscos = [generate_basic_object(i) for i in range(2)]
        increase_elements_persist(two_random_pscos)
        result = compss_wait_on(two_random_pscos)
        self.assertEqual(len(two_random_pscos), len(result))
        for i in range(len(two_random_pscos)):
            self.assertEqual(two_random_pscos[i].get_content() + 1,
                             result[i].get_content())

    def testWorkerGenerationColOutBasic(self):
        # Generate two random non persistent storage objects vector
        two_random_pscos = [generate_basic_object(i) for i in range(2)]
        increase_elements_persist_out(two_random_pscos)
        result = compss_wait_on(two_random_pscos)
        self.assertEqual(len(two_random_pscos), len(result))
        for i in range(len(two_random_pscos)):
            self.assertEqual( i + 1.0,
                             result[i].get_content())

    def testWorkerGenerationColInout(self):
        # Generate ten random vectors with pre-determined seed
        ten_random_pscos = [generate_object(i) for i in range(10)]
        increase_elements(ten_random_pscos)
        # Pick the fifth vector from a COLLECTION_INOUT parameter
        fifth_vector = compss_wait_on(select_element(ten_random_pscos, 4))
        np.random.seed(4)
        self.assertTrue(
            np.allclose(
                fifth_vector.get_content(),
                np.random.random() + 1.0
            )
        )

    def testWorkerGenerationColInColInout(self):
        # Generate ten random vectors with pre-determined seed
        four_random_pscos = [[generate_object(i + j) for i in range(2)] for j in range(2)]
        increase_elements_depth(four_random_pscos)
        # Pick the element (1,1) from a COLLECTION_INOUT parameter
        one_one = compss_wait_on(select_element_from_array(four_random_pscos, 1, 1))
        np.random.seed(2)
        self.assertTrue(
            np.allclose(
                one_one.get_content(),
                np.random.random() + 1.0
            )
        )

    def testCollectionOut(self):
        array_1 = [[i, MyClass()] for i in range(5)]
        array_2 = [MyClass() for _ in range(5)]
        array_3 = [[[i, i * 2], MyClass()] for i in range(5)]

        for i in range(5):
            append(array_1, "appended")
            modify_obj(array_2, "modified")
            modify_deep_obj(array_3, "appended", "modified")

        array_1 = compss_wait_on(array_1)
        array_2 = compss_wait_on(array_2)
        array_3 = compss_wait_on(array_3)

        appends = True
        modifies = True
        deep_modifies = True

        for i in range(5):

            if appends:
                appends = "appended" in array_1[i]

            if modifies:
                modifies = array_2[i].get_test() == "modified"

            if deep_modifies:
                _0 = "appended" in array_3[i][0]
                _1 = array_3[i][1].get_test() == "modified"
                deep_modifies = _0 and _1

        self.assertTrue(appends)
        self.assertTrue(modifies)
        self.assertTrue(deep_modifies)
