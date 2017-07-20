import unittest
from modules.testDeleteFile import testDeleteFile
from modules.testDeleteObject import testDeleteObject

def main():
    suite = unittest.TestLoader().loadTestsFromTestCase(testDeleteFile)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testDeleteObject))
    unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    main()
