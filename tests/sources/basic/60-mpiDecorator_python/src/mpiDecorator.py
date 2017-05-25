import unittest
from modules.testMpiDecorator import testMpiDecorator


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testMpiDecorator)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
