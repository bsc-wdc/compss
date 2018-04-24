import sys
import unittest
from modules.testPythonInterpreter3 import testPythonInterpreter3

def main():
	suite = unittest.TestLoader().loadTestsFromTestCase(testPythonInterpreter3)
	unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
	main()
