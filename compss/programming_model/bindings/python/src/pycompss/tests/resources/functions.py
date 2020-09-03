# ############################## #
# This test checks the functions #
# ############################## #

from pycompss.functions.data import generator


def check_generator():
    random_data = generator((12, 12), 4, 5, 'random', True)
    normal_data = generator((12, 12), 4, 5, 'normal', True)
    uniform_data = generator((12, 12), 4, 5, 'uniform', True)

    assert random_data != normal_data != uniform_data, \
        "The generator did not produce different data for different distributions"  # noqa: E501


def main():
    check_generator()


# if __name__ == "__main__":
#     main()
