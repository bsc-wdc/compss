import unittest
from test.modules.testDecorator import testDecorator
from test.modules.testFunction import testFunction
# from test.modules.testFunction import testFunctions # TODO: Include this test when finished the functions
from test.modules.testWaitOn import testWaitOn
from test.modules.testParameter import testParameter
from test.modules.testClass import testClass

# Duplicated modules with return statements set to numbers
from test.modules.testDecoratorRetInt import testDecoratorRetInt
from test.modules.testFunctionRetInt import testFunctionRetInt
# from test.modules.testFunctionsRetInt import testFunctionsRetInt # TODO: Include this test when finished the functions
from test.modules.testWaitOnRetInt import testWaitOnRetInt
from test.modules.testParameterRetInt import testParameterRetInt
from test.modules.testClassRetInt import testClassRetInt



def main():
    #log_file = '../log_file.txt'
    #f = open(log_file, "w")

    # Usual tests (returns with type)
    suite = unittest.TestLoader().loadTestsFromTestCase(testDecorator)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testFunction))
    # suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testFunctions)) # TODO: Include this test when finished the functions
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testWaitOn))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testParameter))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testClass))

    # Usual tests (returns with number of return elements)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testDecoratorRetInt))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testFunctionRetInt))
    # suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testFunctionsRetInt)) # TODO: Include this test when finished the functions
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testWaitOnRetInt))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testParameterRetInt))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testClassRetInt))
    
    unittest.TextTestRunner(verbosity=2).run(suite)
    #f.close()


if __name__ == "__main__":
    main()
