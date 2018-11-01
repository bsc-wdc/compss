#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import argparse
from pycompss.api.task import task
from pycompss.api.api import compss_barrier


@task()
def assign(letter):
    print("%s squad ready to distribute computation" % letter)


def main():
    print(" - Parsing arguments")
    # Arguments parsing
    parser = argparse.ArgumentParser()

    parser.add_argument('-a', '--alpha', type=int, help="Alpha parameter", required=True)
    parser.add_argument('-b', '--beta', type=int, help="Beta parameter", required=True)
    parser.add_argument('-g', '--gamma', type=int, help="Gamma parameter", required=True)
    parser.add_argument('-d', '--delta', type=str, help="Delta parameter",
                        required=True)
    parser.add_argument('-e', '--epsilon', type=float, help="Epsilon parameter",
                        required=True)
    parser.add_argument('-z', '--zeta', type=int, help="Zeta parameter",
                        required=True)

    args = parser.parse_args()
    alpha = args.alpha
    beta = args.beta
    gamma = args.gamma
    delta = args.delta
    epsilon = args.epsilon
    zeta = args.zeta

    values = [alpha, beta, gamma, delta, epsilon, zeta]
    expected_values = [6, 66, 666, "z6666", 6.6, 666666]

    for v in values:
        if type(v) == int:
            assign(v)

    compss_barrier()

    if values == expected_values:
        print("Argparse received the expected arguments and values:\n%s" % values)
    else:
        print("Argparse did not received the expected arguments and values.")
        print("Expected: %s" % (expected_values))
        print("Got: %s" % (values))


if __name__ == "__main__":
    main()
