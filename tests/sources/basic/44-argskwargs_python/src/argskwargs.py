import unittest
from modules.testArgsKwargsFunctions import testArgsKwargsFunctions
from modules.testArgsKwargsInstanceMethods import testArgsKwargsInstanceMethods

def main():
	suite = unittest.TestLoader().loadTestsFromTestCase(testArgsKwargsFunctions)
	suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testArgsKwargsInstanceMethods))
	unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
	main()