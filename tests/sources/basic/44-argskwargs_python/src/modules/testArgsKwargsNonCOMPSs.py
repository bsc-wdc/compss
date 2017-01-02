import unittest

class testArgsKwargsNonCOMPSs(unittest.TestCase):

    def argTask(self, *args):
        print "ARG: ", args
        return sum(args)
      
    def varargTask(self, v, w, *args):
        print "V: ", v
        print "W: ", w
        print "ARG: ", args
        return (v * w) + sum(args)

    def kwargTask(self, **kwargs):
        print "KARG: ", kwargs
        return len(kwargs)
      
    def varkwargTask(self, v, w , **kwargs):
        print "V: ", v
        print "W: ", w
        print "KARG: ", kwargs
        return (v * w) + len(kwargs)
      
    def argkwargTask(self, *args, **kwargs):
        print "ARG: ", args
        print "KARG: ", kwargs
        return sum(args) + len(kwargs)

    def varargkwargTask(self, v, w , *args, **kwargs):
        print "V: ", v
        print "W: ", w
        print "ARG: ", args
        print "KARG: ", kwargs
        return (v * w) + sum(args) + len(kwargs)

    def varargdefaultkwargTask(self, v, w, s = 2, *args, **kwargs):
        print "V: ", v
        print "W: ", w
        print "S: ", s
        print "ARGS: ", args
        print "KWARG: ", kwargs
        return (v * w) + sum(args) + len(kwargs) + s

    '''
    FUNCTION WITH *ARGS
    '''

    # we have arguments
    def testArgTask1(self):
        result = self.argTask(1, 2)
        self.assertEqual(result, 3)

    def testArgTask2(self):
        result = self.argTask(1, 2, 3, 4)
        self.assertEqual(result, 10)

    # args is empty
    def testArgTask3(self):
        result = self.argTask()
        self.assertEqual(result, 0)

    '''
        FUNCTION WITH ARGS + *ARGS
    '''

    def testVarArgTask1(self):
        result = self.varargTask(10, 20, 1, 2, 3, 4)
        self.assertEqual(result, 210)

    def testVarArgTask2(self):
        result = self.varargTask(4, 50, 5, 4, 3, 2, 1)
        self.assertEqual(result, 215)

    def testVarArgTask3(self):
        result = self.varargTask(4, 50)
        self.assertEqual(result, 200)

    '''
        FUNCTION WITH **KWARGS
    '''

    def testKwargTask1(self):
        result = self.kwargTask(hello='world')
        self.assertEqual(result, 1)

    def testKwargTask2(self):
        result = self.kwargTask(this='is', a='test')
        self.assertEqual(result, 2)

    def testKwargTask3(self):
        result = self.kwargTask()
        self.assertEqual(result, 0)

    '''
        FUNCTION WITH ARGS + **KWARGS
    '''

    def testVarKwargTask1(self):
        result = self.varkwargTask(1, 2, hello='world')
        self.assertEqual(result, 3)

    def testVarArgKwargTask2(self):
        result = self.varkwargTask(2, 3, this='is', a='test')
        self.assertEqual(result, 8)

    def testVarArgKwargTask3(self):
        result = self.varkwargTask(2, 3)
        self.assertEqual(result, 6)

    '''
        FUNCTION WITH *ARGS + **KWARGS
    '''

    def testArgKwargTask1(self):
        result = self.argkwargTask(1, 2, hello='world')
        self.assertEqual(result, 4)

    def testArgKwargTask2(self):
        result = self.argkwargTask(1, 2, 3, 4, this='is', a='test')
        self.assertEqual(result, 12)

    def testArgKwargTask3(self):
        result = self.argkwargTask(1, 2, 3, 4)
        self.assertEqual(result, 10)

    def testArgKwargTask4(self):
        result = self.argkwargTask()
        self.assertEqual(result, 0)

    '''
        FUNCTION WITH ARGS, *ARGS AND **KWARGS
    '''

    def testVarArgKwargTask1(self):
        result = self.varargkwargTask(1, 2, 3, 4, hello='world')
        self.assertEqual(result, 10)

    def testVarArgKwargTask2(self):
        result = self.varargkwargTask(1, 2, 3, 4, 5, 6, this='is', a='test')
        self.assertEqual(result, 22)

    '''
        FUNCTION WITH ARGS, DEFAULTED ARGS, *ARGS AND **KWARGS
    '''

    def testVarArgDefaultKwargTask1(self):
        result = self.varargdefaultkwargTask(1, 1)
        self.assertEqual(result, 3)

    def testVarArgDefaultKwargTask2(self):
        result = self.varargdefaultkwargTask(1, 2, 3)
        self.assertEqual(result, 5)

    def testVarArgDefaultKwargTask3(self):
        result = self.varargdefaultkwargTask(1, 2, 3, 4)
        self.assertEqual(result, 9)

    def testVarArgDefaultKwargTask4(self):
        result = self.varargdefaultkwargTask(1, 2, 3, 4, five=5)
        self.assertEqual(result, 10)

if __name__ == "__main__":
    unittest.main(verbosity=2)