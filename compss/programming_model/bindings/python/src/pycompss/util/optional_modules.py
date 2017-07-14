#
#  Copyright 2.02-2017 Barcelona Supercomputing Center (www.bsc.es)
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
"""
    @author: srodrig1
    PyCOMPSs utils: Optional Modules
    This file contains a list, alongside with some functions, of the
    optional modules that are required in order to be able to use some
    PyCOMPSs features.
"""


optional_modules = {

"guppy":"""Guppy is a module needed for the local decorator.
The local decorator allows you to define non-task functions which are able to
handle synchronizations implictly.""",

"dill":"""Dill is a pickle extension which is capable to serialize a wider variety of objects."""
}


def get_optional_module_warning(module_name, module_description):
    ret = ["[ WARNING ]:\t%s module is not installed."%module_name,
          "\t\t%s"%module_description.replace('\n', '\n\t\t'),
          "\t\tPyCOMPSs can work withouth %s, but it is recommended to have it installed."%module_name,
          "\t\tYou can install it via pip typing pip install %s, or (probably with your package manager)."%module_name
    ]
    return '\n'.join(ret)

def show_optional_module_warnings():
    from pycompss.util.object_properties import is_module_available
    for (name, description) in optional_modules.items():
        if True or not is_module_available(name):
            print
            print get_optional_module_warning(name, description)
            print
