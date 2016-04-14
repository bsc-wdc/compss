import unittest
from modules.testDecorator import testDecorator
from modules.testFunction import testFunction
from modules.testWaitOn import testWaitOn
from modules.testParameter import testParameter
from modules.testClass import testClass


def main():
    #log_file = '../log_file.txt'
    #f = open(log_file, "w")
    suite = unittest.TestLoader().loadTestsFromTestCase(testDecorator)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testFunction))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testWaitOn))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testParameter))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testClass))
    
    unittest.TextTestRunner(verbosity=2).run(suite)
    #f.close()


if __name__ == "__main__":
    main()
