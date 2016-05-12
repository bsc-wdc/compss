import unittest
from test.modules.testFunctions import testFunctions
#from test.modules.testLaunch import testLaunch


def main():
    suiteAdvance = unittest.TestLoader().loadTestsFromTestCase(testFunctions)
    #suiteAdvance.addTest(unittest.TestLoader().loadTestsFromTestCase(testLaunch))
    unittest.TextTestRunner(verbosity=2).run(suiteAdvance)

if __name__ == "__main__":
    main()
