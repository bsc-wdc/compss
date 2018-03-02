import unittest
from modules.testMultiReturnFunctions import testMultiReturnFunctions
from modules.testMultiReturnInstanceMethods import testMultiReturnInstanceMethods
from modules.testMultiReturnIntFunctions import testMultiReturnIntFunctions
from modules.testMultiReturnIntInstanceMethods import testMultiReturnIntInstanceMethods

def main():
	suite = unittest.TestLoader().loadTestsFromTestCase(testMultiReturnFunctions)
	suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testMultiReturnInstanceMethods))
	suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testMultiReturnIntFunctions))
	suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testMultiReturnIntInstanceMethods))
	unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
	main()
