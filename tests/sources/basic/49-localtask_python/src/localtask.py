import unittest
from modules.testLocalTask import testLocalTask


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testLocalTask)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
