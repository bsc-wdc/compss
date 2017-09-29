import unittest
from modules.testRedis import TestRedis

def main():
	suite = unittest.TestLoader().loadTestsFromTestCase(TestRedis)
	unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
	main()
