import unittest
from modules.testOpenclDecorator import testOpenclDecorator


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testOpenclDecorator)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
