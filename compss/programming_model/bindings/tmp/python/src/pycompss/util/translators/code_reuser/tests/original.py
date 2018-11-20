#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# 

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest


def unmodified_header():
    print("This code should remain the same")


def test_func():
    print("Hello World")


def unmodified_footer():
    print("This code should remain the same")


#
# MAIN FOR UNIT TEST
#

if __name__ == '__main__':
    unittest.main()
