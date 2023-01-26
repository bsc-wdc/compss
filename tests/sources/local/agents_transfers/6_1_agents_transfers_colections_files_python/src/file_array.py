"""
Agent transfers file collection python test.
"""
import os
import numpy as np

from pycompss.api.task import task
from pycompss.api.api import compss_wait_on_file
from pycompss.api.parameter import COLLECTION_FILE_IN
from pycompss.api.parameter import COLLECTION_FILE_OUT
from pycompss.api.parameter import COLLECTION_FILE_INOUT


TEST_FILE_PATH = "/tmp/in_out_objects_file_python/"

MATRIX_SIZE_Y = 5
MATRIX_SIZE_X = 5


def print_mat(path, label):
    """Prints matrix on file path[0][0].

    :param path: Array with path to the Matrix file to be displayed.
    :param label: Label to be displayed for the matrix.
    :return: None.
    """
    print("printing matrix " + label + ":")
    print(str(np.load(path[0][0])))
    print("------------------------------------", flush=True)


def fill_with(path, value):
    """Creates a matrix in the file filled with value.

    :param path: Array with the path to the file to be written.
    :param label: Value for the matrix in the file to be filled with.
    :return: None.
    """
    block = np.full((MATRIX_SIZE_Y, MATRIX_SIZE_X), value)
    np.save(path[0][0], block)


@task(path_a=COLLECTION_FILE_IN, path_c=COLLECTION_FILE_OUT)
def nested_in_out(path_a, path_c, label):
    """Creates a matrix in a file in path_c with the same values as the matrix
        in the file in path_a.

    :param path_a: array with path to input matrix.
    :param path_c: array with path to output matrix.
    :param label: Label string to identify and verify the outputs of this
        function.
    :return: None.
    """
    print_mat(path_a, "input " + label)
    current_value = np.load(path_a[0][0])[0][0]
    fill_with(path_c, current_value + 1)
    print_mat(path_c, "output " + label)


@task(path_a=COLLECTION_FILE_IN, path_c=COLLECTION_FILE_OUT)
def in_out(path_a, path_c):
    """Check collection in and collection out invoking nested task.

    :param path_a: Array with path to input matrix.
    :param path_c: Array with path to output matrix.
    :return: None.
    """
    nested_in_out(path_a, path_c, "nested in_out")


@task(path_a=COLLECTION_FILE_IN, path_c=COLLECTION_FILE_OUT)
def in_out_w_print(path_a, path_c):
    """Check collection in and out out invoking a nested task and printing the
        inputs and outputs.

    :param path_a: Array with path to input matrix.
    :param path_c: Array with path to output matrix.
    :return: None.
    """
    print_mat(path_a, "input in_out_w_print")
    nested_in_out(path_a, path_c, "nested_in_out_w_print")
    path_c = compss_wait_on_file(path_c)
    print_mat(path_c, "output in_out_w_print")


@task(path_c=COLLECTION_FILE_INOUT)
def nested_inout(path_c, label):
    """Increments by 1 the values of the matrix on path_c[0][0].

    :param path_c: Array with path to the matrix.
    :param label: Label string to identify and verify the outputs of this
        function.
    :return: None.
    """
    print_mat(path_c, "input " + label)

    mat = np.load(path_c[0][0])
    for i, row in enumerate(mat):
        for j in range(len(row)):
            mat[i][j] += 1
    np.save(path_c[0][0], mat)

    print_mat(path_c, "output " + label)


@task(
    path_c=COLLECTION_FILE_INOUT,
)
def inout(path_c):
    """Check collection inout out invoking a nested task.

    :param path_c: Array with path to the matrix.
    :return: None.
    """
    nested_inout(path_c, "nested_inout")


@task(
    path_c=COLLECTION_FILE_INOUT,
)
def inout_w_print(path_c):
    """Check collection inout out invoking a nested task and printing the
        inputs and outputs.

    :param path_c: Array with path to the matrix.
    :return: None.
    """
    print_mat(path_c, "input inout_w_print")
    nested_inout(path_c, "nested_inout_w_print")
    path_c = compss_wait_on_file(path_c)
    print_mat(path_c, "output inout_w_print")


@task(path_c=COLLECTION_FILE_IN)
def nested_print_task(path_c, label):
    """Print matrix on path_c[0][0] and label.

    :param path_c: Array with path to input matrix.
    :param label: Label string to identify and verify the outputs of this
        sfunction.
    :return: None.
    """
    print_mat(path_c, label)


@task(path_c=COLLECTION_FILE_OUT)
def nested_generation_out(path_c):
    """Creates a matrix in file path_c[0][0].

    :param path_c: Array with path to matrix file.
    :return: None.
    """
    fill_with(path_c, 30)
    print_mat(path_c, "output nested_generation_out")


@task(path_c=COLLECTION_FILE_OUT)
def generation_out(path_c):
    """Check out generation invoking nested task.

    :param path_c: Array with path to matrix.
    :return: None.
    """
    nested_generation_out(path_c)


@task(path_c=COLLECTION_FILE_IN)
def consumption(path_c, label):
    """Check in consumption invoking nested task.

    :param path_c: Array with path to matrix.
    :return: None.
    """
    nested_print_task(path_c, label)


@task(path_c=COLLECTION_FILE_INOUT)
def nested_generation_inout(path_c):
    """Creates/overwrites a matrix in file path_c[0][0].

    :param path_c: Array with path to matrix file.
    :return: None.
    """
    print_mat(path_c, "input nested_generation_inout")

    mat = np.load(path_c[0][0])
    for i, row in enumerate(mat):
        for j in range(len(row)):
            mat[i][j] += 1
    np.save(path_c[0][0], mat)

    print_mat(path_c, "output nested_generation_inout")


@task(path_c=COLLECTION_FILE_INOUT)
def generation_inout(path_c):
    """Check inout generation invoking nested task.

    :param path_c: Array with path to matrix.
    :return: None.
    """
    nested_generation_inout(path_c)


def main():
    """Test main code.

    :return: None.
    """
    path_a1 = [[TEST_FILE_PATH + "A1.npy"]]
    path_a2 = [[TEST_FILE_PATH + "A2.npy"]]
    path_c = [[TEST_FILE_PATH + "C.npy"]]

    if not os.path.exists(TEST_FILE_PATH):
        os.mkdir(TEST_FILE_PATH)

    if os.path.exists(path_c[0][0]):
        os.remove(path_c[0][0])

    #####################################################
    # Nested COLLECTION_FILE IN and COLLECTION_FILE OUT
    #####################################################
    fill_with(path_a1, 0)
    in_out(path_a1, path_c)
    path_c = compss_wait_on_file(path_c)
    print_mat(path_c, "main result in_out")

    #####################################################
    # Nested COLLECTION_FILE IN and COLLECTION_FILE OUT with print
    #####################################################
    fill_with(path_a2, 10)
    in_out_w_print(path_a2, path_c)
    path_c = compss_wait_on_file(path_c)
    print_mat(path_c, "main result in_out_w_print")

    #####################################################
    # Nested COLLECTION_FILE INOUT
    #####################################################
    fill_with(path_c, 20)
    inout(path_c)
    path_c = compss_wait_on_file(path_c)
    print_mat(path_c, "main result inout")

    #####################################################
    # Nested COLLECTION_FILE INOUT with print (uses result from previous)
    #####################################################
    inout_w_print(path_c)
    path_c = compss_wait_on_file(path_c)
    print_mat(path_c, "main result inout_w_print")

    #####################################################
    # Nested Collection OUT/INOUT with consumption
    #####################################################
    generation_out(path_c)
    consumption(path_c, "consumption_out")

    generation_inout(path_c)
    consumption(path_c, "consumption_inout")

    path_c = compss_wait_on_file(path_c)
    print_mat(path_c, "end in main")


@task()
def main_agents():
    """Test main entry point with agents.

    :return: None.
    """
    main()


if __name__ == "__main__":
    # Test main entry point without agents
    main()
