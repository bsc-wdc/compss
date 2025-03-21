#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# -*- coding: utf-8 -*-

"""PyCOMPSs Testbench for PSCOs."""

from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import INOUT
from pycompss.api.parameter import OUT
from pycompss.api.task import task
from pycompss.tests.integration.launch.resources.storage_app.models import (
    InputData,
)
from pycompss.tests.integration.launch.resources.storage_app.models import MySO
from pycompss.tests.integration.launch.resources.storage_app.models import (
    Result,
)
from pycompss.tests.integration.launch.resources.storage_app.models import (
    Words,
)

GENERIC_STRING = "This is a test"
FIRST = "first"
SECOND = "second"
THIRD = "third"
FOURTH = "fourth"
SCO_GET = "sco.get = "


@task(returns=int)
def simple_task(r):
    """Get r value and it by itself.

    :param r: Storage object to multiply.
    :returns: r value * r value
    """
    val = r.get()
    print("R: ", r)
    print("R.get = ", val)
    return val * val


@task(returns=MySO)
def complex_task(p):
    """Persist p with the value multiplied by 10.

    :param p: Storage object to multiply and persist.
    :returns: r value * r value
    """
    v = p.get()
    print("P: ", p)
    print("P.get: ", v)
    q = MySO(v * 10)
    q.makePersistent()
    print("Q: ", q)
    print("Q.get = ", q.get())
    return q


@task(returns=dict)
def wordcount_task(words):
    """Word count task.

    :param words: String.
    :returns: Dictionary with the number of appearances per word.
    """
    partial_result = {}
    data = words.get()
    for entry in data.split():
        if entry in partial_result:
            partial_result[entry] += 1
        else:
            partial_result[entry] = 1
    return partial_result


@task(dic1=INOUT)
def reduce_task(dic1, dic2):
    """Word count reduction task.

    CAUTION! Modifies dic1.

    :param dic1: First partial dictionary.
    :param dic2: Second partial dictionary.
    :returns: None.
    """
    for k in dic2:
        if k in dic1:
            dic1[k] += dic2[k]
        else:
            dic1[k] = dic2[k]


@task(final=INOUT)
def reduce_task_pscos(final, dic2):
    """Word count reduction task with persistent objects.

    CAUTION! Modifies final.

    :param final: First partial dictionary.
    :param dic2: Second partial dictionary.
    :returns: None.
    """
    dic1 = final.get()
    for k in dic2:
        if k in dic1:
            dic1[k] += dic2[k]
        else:
            dic1[k] = dic2[k]
    final.set(dic1)


def basic_test() -> bool:
    """Test basic functionalities.

    This Test:
        - Instantiates a SCO.
        - Makes it persistent.
        - Calls a task with that PSCO -> IN parameter.
        - Waits for the task result.
        - Deletes the PSCO.

    :return: True if success. False otherwise.
    """
    o = MySO(10)
    print("BEFORE MAKEPERSISTENT: o.id: ", o.getID())
    # Persist the object to disk (it will be at /tmp/uuid.PSCO)
    o.makePersistent()
    print("AFTER MAKEPERSISTENT:  o.id: ", o.getID())
    v = simple_task(o)
    v1 = compss_wait_on(v)
    # Remove the persisted object from disk (from /tmp/uuid.PSCO)
    o.deletePersistent()
    if v1 == 100:
        print("- Simple Test Python PSCOs: OK")
        return True
    else:
        print("- Simple Test Python PSCOs: ERROR")
        return False


def basic_2_test() -> bool:
    """Test more basic functionalities.

    This Test:
        - Instantiates a SCO.
        - Makes it persistent.
        - Calls a task with that PSCO -> IN parameter.
            - The task receives the PSCO and gets its value.
            - Instantiates another SCO and makes it persistent within the task.
            - Returns the new PSCO.
        - Calls another task with the input from the first one.
            - Gets the output PSCO and receives it as input.
        - Waits for the first task result.
        - Waits for the second task result.

    :return: True if success. False otherwise.
    """
    p = MySO(1)
    p.makePersistent()
    x = complex_task(p)
    y = simple_task(x)
    res1 = compss_wait_on(x)
    res2 = compss_wait_on(y)
    if p.get() == 1 and res1.get() == 10 and res2 == 100:
        print("- Complex Test Python PSCOs: OK")
        return True
    else:
        print("- Complex Test Python PSCOs: ERROR")
        return False


def basic_3_test() -> bool:
    """Test even more basic functionalities.

    This Test:
        - Instantiates a SCO.
        - Makes it persistent.
        - Calls a task within that PSCO -> self INOUT
            - The task increments its value.
        - Calls it again.
        - Calls it again.
        - Waits for the self result.

    :return: True if success. False otherwise.
    """
    p = MySO(1)
    p.makePersistent()
    p.increment()
    p.increment()
    p.increment()
    p = compss_wait_on(p)
    print("p: " + str(p.get()))
    if p.get() == 4:
        print("- Complex Test Python PSCOs: OK")
        return True
    else:
        print("- Complex Test Python PSCOs: ERROR")
        return False


def wordcount() -> bool:
    """Wordcount Test.

        - Wordcount task receives a PSCO and returns a dictionary.
        - Reduce task works with python dictionaries.

    :return: True if success. False otherwise.
    """
    words = [
        Words(GENERIC_STRING),
        Words(GENERIC_STRING),
        Words(GENERIC_STRING),
        Words(GENERIC_STRING),
    ]
    for w in words:
        w.makePersistent()
    result = {}

    for w in words:
        partial_results = wordcount_task(w)
        reduce_task(result, partial_results)
    result = compss_wait_on(result)

    if (
        result["This"] == 4
        and result["is"] == 4
        and result["a"] == 4
        and result["test"] == 4
    ):
        print("- Python Wordcount 1 with PSCOs: OK")
        return True
    else:
        print("- Python Wordcount 1 with PSCOs: ERROR")
        return False


def wordcount2() -> bool:
    """Wordcount Test.

        - Wordcount task receives a PSCO and returns a dictionary.
        - Reduce task receives a INOUT PSCO (result) where accumulates
          the partial results.

    :return: True if success. False otherwise.
    """
    words = [
        Words(GENERIC_STRING),
        Words(GENERIC_STRING),
        Words(GENERIC_STRING),
        Words(GENERIC_STRING),
    ]
    for w in words:
        w.makePersistent()
    result = Result()
    result.makePersistent()

    for w in words:
        partial_results = wordcount_task(w)
        reduce_task_pscos(result, partial_results)

    final = compss_wait_on(result)

    print(final.myd)
    result = final.get()

    if (
        result["This"] == 4
        and result["is"] == 4
        and result["a"] == 4
        and result["test"] == 4
    ):
        print("- Python Wordcount 2 with PSCOs: OK")
        return True
    else:
        print("- Python Wordcount 2 with PSCOs: ERROR")
        return False


@task(o2=INOUT)
def transform1(o1, o2):
    """Do first transformation (INOUT).

    CAUTION! Modifies o2.

    :param o1: First persistent object.
    :param o2: Second persistent object.
    :results: None.
    """
    pow2 = {}
    images = o1.get()
    for k, v in images.items():
        lst = []
        for value in v:
            lst.append(value * value)
            pow2[k] = lst
    o2.set(pow2)
    print("Function: Pow 2.")
    print("Transformation 1 result in o2: ", o2.get())


@task(o2=INOUT)
def transform2(o2, o1):
    """Do second transformation (INOUT).

    CAUTION! Modifies o2.

    :param o1: First persistent object.
    :param o2: Second persistent object.
    :results: None.
    """
    add1 = {}
    images = o1.get()
    for k, v in images.items():
        lst = []
        for value in v:
            lst.append(value + 1)
            add1[k] = lst
    o2.set(add1)
    print("Function: Add 1.")
    print("Transformation 2 result in o2: ", o2.get())


@task(returns=InputData, o2=INOUT)
def transform3(o1, o2):
    """Do third transformation (returns and INOUT).

    CAUTION! Modifies o2.

    :param o1: First persistent object.
    :param o2: Second persistent object.
    :results: Updated o2.
    """
    mult3 = {}
    images = o1.get()
    for k, v in images.items():
        lst = []
        for value in v:
            lst.append(value * 3)
            mult3[k] = lst
    o2.set(mult3)
    print("Function: Multiply per 3.")
    print("Transformation 3 result in o2: ", o2.get())
    return o2


def tiramisu_mockup() -> bool:
    """Tiramisu Mockup Test.

    :return: True if success. False otherwise.
    """
    my_obj = InputData()
    my_obj.set(
        {
            FIRST: [1, 1, 1, 1],
            SECOND: [2, 2, 2, 2],
            THIRD: [3, 3, 3, 3],
            FOURTH: [4, 4, 4, 4],
        }
    )
    out1 = InputData()
    out2 = InputData()
    out3 = InputData()
    my_obj.makePersistent()
    out1.makePersistent()
    out2.makePersistent()
    out3.makePersistent()

    transform1(my_obj, out1)
    transform2(out2, out1)  # INOUT in first position
    result = transform3(out2, out3)

    result = compss_wait_on(result)
    out1 = compss_wait_on(out1)
    out2 = compss_wait_on(out2)
    out3 = compss_wait_on(out3)

    print("INOUTS:")
    return __check_transformations(
        "Tiramisu Mockup", out1, out2, out3, result
    )  # noqa


def __check_transformations(
    transformation: str,
    out1: InputData,
    out2: InputData,
    out3: InputData,
    result: InputData,
) -> bool:
    """Check the transformation to evaluate the result.

    :param transformation: Transformation name
    :param out1: Transformation 1 output.
    :param out2: Transformation 2 output.
    :param out3: Transformation 3 output.
    :param result: Result object.
    :return: True if success. False otherwise.
    """
    print("Transformation 1: ", out1.get())
    print("Transformation 2: ", out2.get())
    print("Transformation 3: ", out3.get())
    print("RESULT: ", result.get())

    assert out1.get() == {
        FIRST: [1, 1, 1, 1],
        SECOND: [4, 4, 4, 4],
        THIRD: [9, 9, 9, 9],
        FOURTH: [16, 16, 16, 16],
    }
    assert out2.get() == {
        FIRST: [2, 2, 2, 2],
        SECOND: [5, 5, 5, 5],
        THIRD: [10, 10, 10, 10],
        FOURTH: [17, 17, 17, 17],
    }
    assert out3.get() == {
        FIRST: [6, 6, 6, 6],
        SECOND: [15, 15, 15, 15],
        THIRD: [30, 30, 30, 30],
        FOURTH: [51, 51, 51, 51],
    }

    final_results = result.get()
    message = "- Python " + transformation + " with PSCOs: "
    if (
        all(x == 6 for x in final_results[FIRST])
        and all(x == 15 for x in final_results[SECOND])
        and all(x == 30 for x in final_results[THIRD])
        and all(x == 51 for x in final_results[FOURTH])
    ):
        print(message + "OK")
        return True
    else:
        print(message + "ERROR")
        return False


@task(o2=OUT)
def transform1_2(o1, o2):
    """Do first transformation second version (OUT).

    CAUTION! Modifies o2.

    :param o1: First persistent object.
    :param o2: Second persistent object.
    :results: None.
    """
    pow2 = {}
    images = o1.get()
    for k, v in images.items():
        lst = []
        for value in v:
            lst.append(value * value)
            pow2[k] = lst
    o2.set(pow2)
    print("Function: Pow 2.")
    print("Transformation 1 result in o2: ", o2.get())


@task(o2=OUT)
def transform2_2(o2, o1):
    """Do second transformation second version (OUT).

    CAUTION! Modifies o2.

    :param o1: First persistent object.
    :param o2: Second persistent object.
    :results: None.
    """
    add1 = {}
    images = o1.get()
    for k, v in images.items():
        lst = []
        for value in v:
            lst.append(value + 1)
            add1[k] = lst
    o2.set(add1)
    print("Function: Add 1.")
    print("Transformation 2 result in o2: ", o2.get())


@task(returns=InputData, o2=OUT)
def transform3_2(o1, o2):
    """Do third transformation second version (OUT).

    CAUTION! Modifies o2.

    :param o1: First persistent object.
    :param o2: Second persistent object.
    :results: None.
    """
    mult3 = {}
    images = o1.get()
    for k, v in images.items():
        lst = []
        for value in v:
            lst.append(value * 3)
            mult3[k] = lst
    o2.set(mult3)
    print("Function: Multiply per 3.")
    print("Transformation 3 result in o2: ", o2.get())
    return o2


def tiramisu_mockup2() -> bool:
    """Tiramisu Mockup Test 2.

    :return: True if success. False otherwise.
    """
    my_obj = InputData()
    my_obj.set(
        {
            FIRST: [1, 1, 1, 1],
            SECOND: [2, 2, 2, 2],
            THIRD: [3, 3, 3, 3],
            FOURTH: [4, 4, 4, 4],
        }
    )
    out1 = InputData()
    out2 = InputData()
    out3 = InputData()
    my_obj.makePersistent()
    out1.makePersistent()
    out2.makePersistent()
    out3.makePersistent()

    transform1_2(my_obj, out1)
    transform2_2(out2, out1)  # OUT in first position
    result = transform3_2(out2, out3)

    result = compss_wait_on(result)

    out1 = compss_wait_on(out1)
    out2 = compss_wait_on(out2)
    out3 = compss_wait_on(out3)

    print("OUTPUTS:")
    return __check_transformations(
        "Tiramisu Mockup 2", out1, out2, out3, result
    )  # noqa


@task(sco=INOUT)
def make_persistent_in_task_as_parameter(sco):
    """Make persistent the sco within the task as INOUT.

    :param sco: Non persisted persistent object.
    :returns: None.
    """
    print("sco: ", sco)
    print(SCO_GET, sco.get())
    sco.put(27)
    print("Update sco value:")
    print(SCO_GET, sco.get())
    sco.makePersistent()
    print("sco made persistent")
    print("sco id: ", sco.getID())


@task(returns=MySO)
def make_persistent_in_task_as_return(sco):
    """Make persistent the sco within the task as return.

    :param sco: Non persisted persistent object.
    :returns: None.
    """
    print("sco: ", sco)
    print(SCO_GET, sco.get())
    sco.put(37)
    print("Update sco value:")
    print(SCO_GET, sco.get())
    sco.makePersistent()
    print("sco made persistent")
    print("sco id: ", sco.getID())
    return sco


def evaluate_make_persistent_in_task() -> bool:
    """Check sco persistence within task (as INOUT).

    This Test checks what happens when a not persisted persistent object
    is passed an INOUT task parameter and made persistent within the task.

    :return: True if success. False otherwise.
    """
    o = MySO(10)
    make_persistent_in_task_as_parameter(o)
    v = compss_wait_on(o)
    o.deletePersistent()
    if v.get() == 27:
        print("- Persistence of PSCOS in task as INOUT parameter: OK")
        return True
    else:
        print("- Persistence of PSCOS in task as INOUT parameter: ERROR")
        return False


def evaluate_make_persistent_in_task2() -> bool:
    """Check sco persistence within task (as return).

    This Test checks what happens when a not persisted persistent object
    is passed as IN task parameter, made persistent within the task, and
    returned.

    :return: True if success. False otherwise.
    """
    o = MySO(10)
    sco = make_persistent_in_task_as_return(o)
    v = compss_wait_on(sco)
    v.deletePersistent()
    if v.get() == 37:
        print("- Persistence of PSCOS in task as return: OK")
        return True
    else:
        print("- Persistence of PSCOS in task as return: ERROR")
        return False


def main():
    """Run all pscos tests.

    :returns: None.
    """
    results = {
        "basic": basic_test(),
        "basic2": basic_2_test(),
        "basic3": basic_3_test(),
        "wordcount": wordcount(),
        "wordcount2": wordcount2(),
        "tiramisu": tiramisu_mockup(),
        "tiramisu2": tiramisu_mockup2(),
        "persistInTask": evaluate_make_persistent_in_task(),
        "persistInTask2": evaluate_make_persistent_in_task2(),
    }

    assert all(
        x for x in list(results.values())
    ), "PSCOs TEST FINISHED WITH ERRORS: " + str(results)
