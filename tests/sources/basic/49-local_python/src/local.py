import unittest
from modules.testLocal import testLocal


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testLocal)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
