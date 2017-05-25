import unittest
from modules.testBinaryDecorator import testBinaryDecorator


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testBinaryDecorator)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
