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

i_am_module = False


def get_i_am_module():
    """
    i_am_module variable getter
    :return: Boolean
    """
    return i_am_module


def activate_module():
    """
    i_am_module variable setter to True
    """
    global i_am_module
    i_am_module = True


def deactivate_module():
    """
    i_am_module variable setter to False
    """
    global i_am_module
    i_am_module = False
