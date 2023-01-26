"""
Agent transfers single object python test.
"""

import numpy as np

from pycompss.api.task import task
from pycompss.api.api import compss_wait_on
from pycompss.api.constraint import constraint

from pycompss.api.parameter import INOUT


TEST_FILE_PATH = "/tmp/in_return_objects_file_python/"

MATRIX_SIZE_Y = 5
MATRIX_SIZE_X = 5


def print_mat(matrix, label):
    """Prints matrix.

    :param matrix: Matrix to be displayed.
    :param label: Label to be displayed for the matrix.
    :return: None.
    """
    print("printing matrix " + label + ":")
    print(matrix)
    print("------------------------------------", flush=True)


def create_mat(value):
    """Create a new matrix with the given value.

    :return: A new list with one list containing a 2D numpy array.
    """
    return np.full((MATRIX_SIZE_Y, MATRIX_SIZE_X), value)


@constraint(processor_architecture="processor_ag_3")
@task(returns=1)
def nested_in_return(mat_a, label):
    """Check nested IN and return.

    :param mat_a: input matrix.
    :param label: Label string.
    :return: The result of applying create_mat task with mat_a.
    """
    print_mat(mat_a, "input " + label)
    current_value = mat_a[0][0]
    mat_c = create_mat(current_value + 1)
    print_mat(mat_c, "output " + label)
    return mat_c


@constraint(processor_architecture="processor_ag_2", operating_system_type="agent_2")
@task(returns=1)
def in_return(mat_a):
    """Check IN and return.

    :param mat_a: input matrix.
    :return: The result of applying nested_in_return task to mat_a.
    """
    res = nested_in_return(mat_a, "nested in_return")
    return res


@constraint(processor_architecture="processor_ag_2", operating_system_type="agent_2")
@task(returns=1)
def in_return_w_print(mat_a):
    """Check IN and return with print.

    :param mat_a: input matrix.
    :return: The result of applyting nested_in_return to mat_a.
    """
    print_mat(mat_a, "input in_return_w_print")
    mat_c = nested_in_return(mat_a, "nested_in_return_w_print")
    mat_c = compss_wait_on(mat_c)
    print_mat(mat_c, "output in_return_w_print")
    return mat_c


@constraint(processor_architecture="processor_ag_3")
@task(mat_c=INOUT)
def nested_inout(mat_c, label):
    """Check nested INOUT.

    :param mat_c: Matrix c.
    :param label: Label string.
    :return: None.
    """
    print_mat(mat_c, "input " + label)

    for i, row in enumerate(mat_c):
        for j in range(len(row)):
            mat_c[i][j] += 1

    print_mat(mat_c, "output " + label)


@constraint(processor_architecture="processor_ag_2", operating_system_type="agent_2")
@task(mat_c=INOUT)
def inout(mat_c):
    """Check INOUT.

    :param mat_c: inout Matrix.
    :return: None.
    """
    print_mat(mat_c, "input inout_print")
    nested_inout(mat_c, "nested_inout")


@constraint(processor_architecture="processor_ag_2", operating_system_type="agent_2")
@task(mat_c=INOUT)
def inout_w_print(mat_c):
    """Check INOUT with print.

    :param mat_c: inout matrix.
    :return: None.
    """
    print_mat(mat_c, "input inout_w_print")
    nested_inout(mat_c, "nested_inout_w_print")
    mat_c_res = compss_wait_on(mat_c)
    for i, row in enumerate(mat_c):
        for j in range(len(row)):
            mat_c[i][j] = mat_c_res[i][j]
    print_mat(mat_c, "output inout_w_print")


@constraint(processor_architecture="processor_ag_3")
@task()
def nested_print_task(mat_c, label):
    """Print mat_c and label.

    :param mat_c: inout matrix.
    :param label: Label string.
    :return: None.
    """
    print_mat(mat_c, label)


@constraint(processor_architecture="processor_ag_3")
@task(returns=1)
def nested_generation_return():
    """Check nested return generation.

    :return: New matrix initialized with 30s.
    """
    mat_c = create_mat(30)
    print_mat(mat_c, "output nested_generation_return")
    return mat_c


@constraint(processor_architecture="processor_ag_2", operating_system_type="agent_2")
@task(returns=1)
def generation_return():
    """Check return generation invoking nested task.

    :return: The result of nested_generation_return task.
    """
    return nested_generation_return()


@constraint(processor_architecture="processor_ag_2", operating_system_type="agent_2")
@task()
def consumption(mat_c, label):
    """Print mat_c and label invoking nested task (print_task -> print_mat).

    :param mat_c: input matrix.
    :param label: Label string.
    :return: None.
    """
    nested_print_task(mat_c, label)


@constraint(processor_architecture="processor_ag_3")
@task(mat_c=INOUT)
def nested_generation_inout(mat_c):
    """Check nested inout generation.

    :param mat_c: inout matrix.
    :return: None.
    """
    print_mat(mat_c, "input nested_generation_inout")

    for i, row in enumerate(mat_c):
        for j in range(len(row)):
            mat_c[i][j] += 1

    print_mat(mat_c, "output nested_generation_inout")


@constraint(processor_architecture="processor_ag_2", operating_system_type="agent_2")
@task(mat_c=INOUT)
def generation_inout(mat_c):
    """Check inout generation invoking nested task.

    :param mat_c: inout matrix.
    :return: None.
    """
    nested_generation_inout(mat_c)


def main():
    """Test main code.

    :return: None.
    """

    #####################################################
    # Nested IN and return
    #####################################################
    mat_a = create_mat(0)
    mat_c = in_return(mat_a)
    mat_c = compss_wait_on(mat_c)
    print_mat(mat_c, "main result in_return")

    #####################################################
    # Nested IN and return with print
    #####################################################
    mat_a = create_mat(10)
    mat_c = in_return_w_print(mat_a)
    mat_c = compss_wait_on(mat_c)
    print_mat(mat_c, "main result in_return_w_print")

    #####################################################
    # Nested INOUT with print
    #####################################################
    mat_c = create_mat(20)
    inout_w_print(mat_c)
    mat_c = compss_wait_on(mat_c)
    print_mat(mat_c, "main result inout_w_print")

    #####################################################
    # Nested INOUT (uses result from previous)
    #####################################################
    inout(mat_c)
    mat_c = compss_wait_on(mat_c)
    print_mat(mat_c, "main result inout")

    #####################################################
    # Nested INOUT and return with consumption
    #####################################################
    mat_c = generation_return()
    consumption(mat_c, "consumption_return")
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
