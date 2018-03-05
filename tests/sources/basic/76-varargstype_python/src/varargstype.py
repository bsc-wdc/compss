import unittest
from modules.testVarargsType import testVarargsType

def main():
	suite = unittest.TestLoader().loadTestsFromTestCase(testVarargsType)
	unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
	main()
