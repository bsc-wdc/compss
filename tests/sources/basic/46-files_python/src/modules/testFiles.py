import unittest
from pycompss.api.task import task
from pycompss.api.parameter import *
import os.path

class testFiles(unittest.TestCase):

    @task(fin=FILE, returns=str)
    def fileIn(self, fin):
        print "TEST FILE IN"
        # Open the file and read the content
        fin_d = open(fin, 'r')
        content = fin_d.read()
        print "- In file content:\n", content
        # Close and return the content
        fin_d.close()
        return content

    @task(finout=FILE_INOUT, returns=str)
    def fileInOut(self, finout):
        print "TEST FILE INOUT"
        # Open the file and read the content
        finout_d = open(finout, 'r+')
        content = finout_d.read()
        print "- Inout file content:\n", content
        # Add some content
        content += "\n===> INOUT FILE ADDED CONTENT"
        finout_d.write("\n===> INOUT FILE ADDED CONTENT")
        print "- Inout file content after modification:\n", content
        # Close and return with the modification
        finout_d.close()
        return content

    @task(fout=FILE_OUT, returns=str)
    def fileOut(self, fout, content):
        print "TEST FILE OUT"
        # Open the file for writting and write some content
        with open(fout, 'w') as fout_d:
            fout_d.write(content)
        print "- Out file content added:\n", content
        return content

    @task(returns=FILE)
    def returnFile(self, filename, content):
        print "TEST RETURN FILE"
        # Open the file for writting and write some content
        fout_d = open(filename, 'w')
        fout_d.write(content)
        print "- Out file name: ", filename
        print "- Out file content added: ", content
        # Close and return the content written
        fout_d.close()
        return filename

    @task(returns=(FILE, FILE))
    def multireturnFile(self, content1, content2):
        print "TEST MULTIRETURN FILE"
        filename1 = 'retFile1'
        filename2 = 'retFile2'
        # Open the file for writting and write some content
        fout_d1 = open(filename1, 'w')
        fout_d1.write(content1)
        print "- Out file name: ", filename1
        print "- Out file content added: ", content1
        fout_d2 = open(filename2, 'w')
        fout_d2.write(content2)
        print "- Out file name: ", filename2
        print "- Out file content added: ", content2
        # Close and return the content written
        fout_d1.close()
        fout_d2.close()
        return filename1, filename2

    def testFileIN(self):
        """ Test FILE_IN """
        from pycompss.api.api import compss_wait_on
        fin = "infile"
        content = "IN FILE CONTENT"
        with open(fin, 'w') as f:
            f.write(content)

        res = self.fileIn(fin)
        res = compss_wait_on(res)
        self.assertEqual(res, content, "strings are not equal: {}, {}".format(res, content))

    def testFileOUT(self):
        """ Test FILE_OUT """
        from pycompss.api.api import compss_wait_on
        from pycompss.api.api import compss_open
        fout = "outfile"
        content = "OUT FILE CONTENT"
        res = self.fileOut(fout, content)
        res = compss_wait_on(res)
        with compss_open(fout, 'r') as fout_r:
            content_r = fout_r.read()
        # The final file is only stored after the execution.
        # During the execution, you have to use the compss_open, which will
        # provide the real file where the output file is.
        #fileInFolder = os.path.exists(fout)
        #self.assertTrue(fileInFolder, "FILE_OUT is not in the final location")
        self.assertEqual(res, content, "strings are not equal: {}, {}".format(res, content))
        self.assertEqual(content_r, content, "strings are not equal: {}, {}".format(content_r, content))

    def testFileINOUT(self):
        """ Test FILE_INOUT """
        from pycompss.api.api import compss_wait_on
        from pycompss.api.api import compss_open
        finout = "inoutfile"
        content = "INOUT FILE CONTENT"
        with open(finout, 'w') as f:
            f.write(content)

        res = self.fileInOut(finout)
        res = compss_wait_on(res)
        with compss_open(finout, 'r') as finout_r:
            content_r = finout_r.read()

        content += "\n===> INOUT FILE ADDED CONTENT"
        self.assertEqual(res, content, "strings are not equal: {}, {}".format(res, content))
        self.assertEqual(content_r, content, "strings are not equal: {}, {}".format(content_r, content))

    @unittest.skip("not supported skipping")
    def testReturnFile(self):
        """ Test return FILE """
        from pycompss.api.api import compss_wait_on
        fret = "fret"
        content = "RETURN FILE CONTENT"
        res = self.returnFile(fret, content)
        res = compss_wait_on(res)
        with compss_open(res, 'r') as f:
            content_r = f.read()
        self.assertEqual(res, fret, "strings are not equal: {}, {}".format(res, content))
        self.assertEqual(content_r, content, "strings are not equal: {}, {}".format(content_r, content))

    @unittest.skip("not supported skipping")
    def testMultiReturnFile(self):
        """ Test multireturn FILE """
        fretout1 = "retFile1"
        fretout2 = "retFile2"
        content1 = "RETURN FILE CONTENTS A"
        content2 = "RETURN FILE CONTENTS B"
        retResA, retResB = self.multireturnFile(fretout1, fretout2, content1, content2)
        retResA = compss_wait_on(retResA)
        retResB = compss_wait_on(retResB)
        with compss_open(retResA, 'r') as f:
            content1_r = f.read()
        with compss_open(retResB, 'r') as f:
            content2_r = f.read()
        self.assertEqual(res, fret, "strings are not equal: {}, {}".format(content2_r, content2))
        self.assertEqual(content_r, content, "strings are not equal: {}, {}".format(content1_r, content1))

    def testWorkflowFiles(self):
        """ Test a workflow with FILES """
        from pycompss.api.api import compss_open
        fin = "inwork"
        fout = "outwork"

        content = "Before the task "
        with open(fin, 'w') as f:
            f.write(content)
        res = self.fileIn(fin)
        f2 = self.fileOut(fout, res)
        f3 = self.fileInOut(fout)

        with compss_open(fout, 'r') as f:
            content_r = f.read()
        content += "\n===> INOUT FILE ADDED CONTENT"
        self.assertEqual(content, content_r, "strings are not equal: {}, {}".format(content_r, content))
        pass
