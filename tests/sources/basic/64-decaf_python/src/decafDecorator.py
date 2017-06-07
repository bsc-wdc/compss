import unittest
from modules.testDecafDecorator import testDecafDecorator


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testDecafDecorator)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
