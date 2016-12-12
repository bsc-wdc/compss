import unittest
from modules.testFiles import testFiles


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testFiles)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
