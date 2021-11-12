#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest
import os
from pycompss.api.task import task
from pycompss.api.parameter import *


class testFiles(unittest.TestCase):
    
    @task(fin=FILE, fin2=FILE, returns=2)
    def fileInDpulicated(self, fin, fin2, name):
        print("TEST FILE NAME")
        if fin.endswith(name):
            print("The file name is OK: " + str(fin))
        else:
            raise Exception("FILENAME NOT AS EXPECTED: " + str(fin) + " != " + str(name))
        fin_d = open(fin, 'r')
        content = fin_d.read()
        print("- Inout file content:\n", content)
        if fin2.endswith(name):
            print("The file name is OK: " + str(fin2))
        else:
            raise Exception("FILENAME NOT AS EXPECTED: " + str(fin2) + " != " + str(name))
        fin2_d = open(fin2, 'r')
        content2 = fin2_d.read()
        print("- Inout file content:\n", content2)
        return content, content2

    @task(fin=FILE, fin2=FILE_INOUT, returns=2)
    def fileInInoutDpulicated(self, fin, fin2, name):
        print("TEST FILE NAME")
        if fin.endswith(name):
            print("The file name is OK: " + str(fin))
        else:
            raise Exception("FILENAME NOT AS EXPECTED: " + str(fin) + " != " + str(name))
        fin_d = open(fin, 'r')
        content = fin_d.read()
        print("- In file content:\n", content)
        if fin2.endswith(name):
            print("The file name is OK: " + str(fin2))
        else:
            raise Exception("FILENAME NOT AS EXPECTED: " + str(fin2) + " != " + str(name))
        fin2_d = open(fin2, 'r+')
        content2 = fin2_d.read()
        print("- Inout file content:\n", content2)
        # Add some content
        content2 += "\n===> INOUT FILE ADDED CONTENT"
        fin2_d.write("\n===> INOUT FILE ADDED CONTENT")
        print("- Inout file content after modification:\n", content2)
        # Close and return with the modification
        fin2_d.close()
        return content, content2 
   
    @task(fin=FILE, fout=FILE_OUT, returns=2)
    def fileInOutDuplicated(self, fin, fout, name, content_out):
        if fin.endswith(name):
            print("The file name is OK: " + str(fin))
        else:
            raise Exception("FILENAME NOT AS EXPECTED: " + str(fin) + " != " + str(name))
        fin_d = open(fin, 'r')
        content = fin_d.read()
        print("- In file content:\n", content)
        if fout.endswith(name):
            print("The file name is OK: " + str(fout))
        else:
            raise Exception("FILENAME NOT AS EXPECTED: " + str(fout) + " != " + str(name))

        # Open the file for writting and write some content
        with open(fout, 'w') as fout_d:
            fout_d.write(content_out)
        print("- Out file content added:\n", content_out)
        return content, content_out

    
    @task(target_direction=IN, fout=FILE_OUT, returns=str)
    def fileOut(self, fout, content_out):
        print("TEST FILE OUT")
        # Open the file for writting and write some content
        with open(fout, 'w') as fout_d:
            fout_d.write(content_out)
        print("- Out file content added:\n", content_out)
        return content_out

    def testFileINDuplic(self):
        """ Test FILE_IN """
        from pycompss.api.api import compss_wait_on
        if not os.path.exists("id1"):
            os.mkdir("id1")
        if not os.path.exists("id2"):
            os.mkdir("id2")
        fin1 = "id1/infile.txt"
        fin2 = "id2/infile.txt"

        content1 = "IN FILE CONTENT 1"
        content2 = "IN FILE CONTENT 2"
        self.fileOut(fin1, content1)
        self.fileOut(fin2, content2)
        res1,res2 = self.fileInDpulicated(fin1, fin2, "infile.txt")
        res1 = compss_wait_on(res1)
        res2 = compss_wait_on(res2)
        self.assertEqual(res1, content1, "strings are not equal: {}, {}".format(res1, content1))
        self.assertEqual(res1, content1, "strings are not equal: {}, {}".format(res1, content1))

    def testFileIN_OUTDuplic(self):
        """ Test FILE_OUT """
        from pycompss.api.api import compss_wait_on
        from pycompss.api.api import compss_open
        if not os.path.exists("id1"):
            os.mkdir("id1")
        if not os.path.exists("id2"):
            os.mkdir("id2")
        fin = "id1/outfile" 
        fout = "id2/outfile"
        content_1 = "IN FILE CONTENT"
        content_2 = "OUT FILE CONTENT"
        self.fileOut(fin, content_1)
        res1, res2 = self.fileInOutDuplicated(fin, fout, "outfile", content_2)
        res1 = compss_wait_on(res1)
        with compss_open(fout, 'r') as fout_r:
            content_r = fout_r.read()
        # The final file is only stored after the execution.
        # During the execution, you have to use the compss_open, which will
        # provide the real file where the output file is.
        # fileInFolder = os.path.exists(fout)
        # self.assertTrue(fileInFolder, "FILE_OUT is not in the final location")
        self.assertEqual(res1, content_1, "strings are not equal: {}, {}".format(res1, content_1))
        self.assertEqual(content_r, content_2, "strings are not equal: {}, {}".format(content_r, content_2))

    def testFileIN_INOUTDuplic(self):
        """ Test FILE_INOUT """
        from pycompss.api.api import compss_wait_on
        from pycompss.api.api import compss_open
        if not os.path.exists("id1"):
            os.mkdir("id1")
        if not os.path.exists("id2"):
            os.mkdir("id2")
        fin = "id1/inoutfile"   
        finout = "id2/inoutfile"
        content1 = "INOUT FILE CONTENT 1"
        content2 = "INOUT FILE CONTENT 2"
        self.fileOut(fin, content1)
        self.fileOut(finout, content2)
        res1, res2 = self.fileInInoutDpulicated(fin, finout, "inoutfile")
        res1 = compss_wait_on(res1)
        with compss_open(finout, 'r') as finout_r:
            content_r = finout_r.read()
        content2 += "\n===> INOUT FILE ADDED CONTENT"
        self.assertEqual(res1, content1, "strings 1 are not equal: {}, {}".format(res1, content1))
        self.assertEqual(content_r, content2, "strings 2 are not equal: {}, {}".format(content_r, content2))
