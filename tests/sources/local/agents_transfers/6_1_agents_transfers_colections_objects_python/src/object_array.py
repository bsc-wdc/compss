"""
Agent transfers collections objects python test.
"""

import numpy as np

from pycompss.api.task import task
from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import COLLECTION_IN
from pycompss.api.parameter import COLLECTION_INOUT
from pycompss.api.parameter import COLLECTION_OUT

from dummy import Dummy


TEST_FILE_PATH = "/tmp/in_out_objects_file_python/"

MATRIX_SIZE_Y = 5
MATRIX_SIZE_X = 5


def print_mat(matrix, label):
    """Show the given matrix first element.

    :param matrix: Matrix to be displayed.
    :param label: Label to be displayed for the matix.
    :return: None.
    """
    print("printing matrix " + label + ":")
    print(matrix[0][0])
    print("------------------------------------", flush=True)


def create_empty_mat():
    """Create a new empty matrix using Dummy objects.

    Just placeholders for COLLECTION_OUT initial parameters.

    :return: A new list with one list with a Dummy object.
    """
    return [[Dummy()]]


def create_mat(value):
    """Create a new matrix with the given value.

    :return: A new list with one list containing a 2D numpy array.
    """
    return [[np.full((MATRIX_SIZE_Y, MATRIX_SIZE_X), value)]]


@task(mat_a=COLLECTION_IN, mat_c=COLLECTION_OUT)
def nested_in_out(mat_a, label, mat_c):
    """Check nested collection in and collection out.

    :param mat_a: Matrix A.
    :param label: Label string.
    :param mat_c: Matrix C.
    :return: None.
    """
    print_mat(mat_a, "input " + label)
    current_value = mat_a[0][0][0][0]
    _mat_c = create_mat(current_value + 1)
    print_mat(_mat_c, "output " + label)
    for i, row in enumerate(mat_c):
        for j in range(len(row)):
            mat_c[i][j] = _mat_c[i][j]


@task(mat_a=COLLECTION_IN, mat_c=COLLECTION_OUT)
def in_out(mat_a, mat_c):
    """Check collection in and collection out invoking nested task.

    :param mat_a: Matrix A.
    :param mat_c: Matrix C.
    :return: None.
    """
    nested_in_out(mat_a, "nested_in_out", mat_c)


@task(mat_a=COLLECTION_IN, mat_c=COLLECTION_OUT)
def in_out_w_print(mat_a, mat_c):
    """Check collection in and collection out invoking nested task with print.

    :param mat_a: Matrix A.
    :param mat_c: Matrix C.
    :return: None.
    """
    print_mat(mat_a, "input in_out_w_print")
    nested_in_out(mat_a, "nested_in_out_w_print", mat_c)
    _mat_c = compss_wait_on(mat_c)
    print_mat(_mat_c, "output in_out_w_print")
    for i, row in enumerate(mat_c):
        for j in range(len(row)):
            mat_c[i][j] = _mat_c[i][j]


@task(mat_c=COLLECTION_INOUT)
def nested_inout(mat_c, label):
    """Check nested collection inout.

    :param mat_c: Matrix C.
    :param label: Label string.
    :return: None.
    """
    mat = mat_c[0][0]
    print_mat(mat_c, "input " + label)

    for i, row in enumerate(mat):
        for j in range(len(row)):
            mat[i][j] += 1

    print_mat(mat_c, "output " + label)


@task(mat_c=COLLECTION_INOUT)
def inout(mat_c):
    """Check collection inout invoking nested task.

    :param mat_c: Matrix C.
    :return: None.
    """
    nested_inout(mat_c, "nested_inout")


@task(mat_c=COLLECTION_INOUT)
def inout_w_print(mat_c):
    """Check collection inout invoking nested task with print.

    :param mat_c: Matrix C.
    :return: None.
    """
    print_mat(mat_c, "input inout_w_print")
    nested_inout(mat_c, "nested_inout_w_print")
    mat_c_res = compss_wait_on(mat_c)
    for i, row in enumerate(mat_c_res):
        for j in range(len(row)):
            mat_c[i][j] = mat_c_res[i][j]
    print_mat(mat_c_res, "output inout_w_print")


@task()
def print_task(mat_c, label):
    """Print mat_c and label.

    :param mat_c: Matrix C.
    :param label: Label string.
    :return: None.
    """
    print_mat(mat_c, label)


@task(mat_c=COLLECTION_OUT)
def nested_generation_out(mat_c):
    """Check nested out generation.

    :param mat_c: Matrix C.
    :return: None.
    """
    _mat_c = create_mat(30)
    print_mat(_mat_c, "output nested_generation_out")
    for i, row in enumerate(mat_c):
        for j in range(len(row)):
            mat_c[i][j] = _mat_c[i][j]


@task(mat_c=COLLECTION_OUT)
def generation_out(mat_c):
    """Check out generation invoking nested task.

    :param mat_c: Matrix C.
    :return: None.
    """
    nested_generation_out(mat_c)


@task(mat_c=COLLECTION_INOUT)
def consumption(mat_c, label):
    """Print mat_c and label invoking nested task (print_task -> print_mat).

    :param mat_c: Matrix C.
    :param label: Label string.
    :return: None.
    """
    print_task(mat_c, label)


@task(mat_c=COLLECTION_INOUT)
def nested_generation_inout(mat_c):
    """Check nested collection inout generation.

    :param mat_c: Matrix C.
    :return: None.
    """
    print_mat(mat_c, "input nested_generation_inout")

    mat = mat_c[0][0]
    for i, row in enumerate(mat):
        for j in range(len(row)):
            mat[i][j] += 1

    print_mat(mat_c, "output nested_generation_inout")


@task(mat_c=COLLECTION_INOUT)
def generation_inout(mat_c):
    """Check collection inout generation invoking nested task.

    :param mat_c: Matrix C.
    :return: None.
    """
    nested_generation_inout(mat_c)


def main():
    """Test main code.

    :return: None.
    """

    #####################################################
    # Nested Collection IN and Collection OUT
    #####################################################
    mat_a = create_mat(0)
    mat_c = create_empty_mat()
    in_out(mat_a, mat_c)
    mat_c = compss_wait_on(mat_c)
    print_mat(mat_c, "main result in_out")

    #####################################################
    # Nested Collection IN and Collection OUT with print
    #####################################################
    mat_a = create_mat(10)
    mat_c = create_empty_mat()
    in_out_w_print(mat_a, mat_c)
    mat_c = compss_wait_on(mat_c)
    print_mat(mat_c, "main result in_out_w_print")

    #####################################################
    # Nested Collection INOUT with print
    #####################################################
    mat_c = create_mat(20)
    inout_w_print(mat_c)
    mat_c = compss_wait_on(mat_c)
    print_mat(mat_c, "main result inout_w_print")

    #####################################################
    # Nested Collection INOUT (uses result from previous)
    #####################################################
    inout(mat_c)
    mat_c = compss_wait_on(mat_c)
    print_mat(mat_c, "main result inout")

    #####################################################
    # Nested Collection OUT/INOUT with consumption
    #####################################################
    mat_c = create_empty_mat()
    generation_out(mat_c)
    consumption(mat_c, "consumption_out")
    generation_inout(mat_c)
    consumption(mat_c, "consumption_inout")
    mat_c = compss_wait_on(mat_c)
    print_mat(mat_c, "end in main")


@task()
def main_agents():
    """Test main entry point with agents.

    :return: None.
    """
    main()


if __name__ == "__main__":
    # Test main entry point without agents
    main()
