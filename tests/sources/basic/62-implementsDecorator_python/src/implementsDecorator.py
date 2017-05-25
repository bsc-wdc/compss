import unittest
from modules.testImplementsDecorator import testImplementsDecorator


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testImplementsDecorator)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
