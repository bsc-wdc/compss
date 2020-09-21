import sys
import unittest
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task

@task(returns=int)
def workerInterpreter():
    worker_version = sys.version_info[:][0]
    print("WORKER INTERPRETER: ", worker_version)
    return worker_version


class testPythonInterpreter3(unittest.TestCase):

    testing_version = 3

    def testPythonVersion(self):
        self.assertEqual(sys.version_info[:][0], self.testing_version)
        worker_version = workerInterpreter()
        worker_version = compss_wait_on(worker_version)
        self.assertEqual(worker_version, self.testing_version)
