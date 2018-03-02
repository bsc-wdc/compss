import unittest
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task
from pycompss.api.parameter import *

'''Basic test: args are file ins
'''
@task(varargsType = FILE_IN, returns = str)
def file_in(a, b, *args):
    return 'OK' \
    if a == open(args[0], 'r').read().strip() and \
    b == open(args[1], 'r').read().strip() \
    else 'NO'


class testVarargsType(unittest.TestCase):

    def testVarargsType(self):
        import os
        path1 = os.path.join(os.getcwd(), 'hello.txt')
        path2 = os.path.join(os.getcwd(), 'world.txt')
        open(path1, 'w').write('hello')
        open(path2, 'w').write('world')
        res = file_in('hello', 'world', path1, path2)
        res = compss_wait_on(res)
        self.assertEqual('OK', res)
