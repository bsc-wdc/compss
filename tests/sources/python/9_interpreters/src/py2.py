import sys
import unittest
from modules.testPythonInterpreter2 import testPythonInterpreter2

def main():
	suite = unittest.TestLoader().loadTestsFromTestCase(testPythonInterpreter2)
	unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
	main()
