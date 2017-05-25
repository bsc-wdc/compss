import unittest
from modules.testOmpssDecorator import testOmpssDecorator


def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testOmpssDecorator)
    unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == "__main__":
    main()
