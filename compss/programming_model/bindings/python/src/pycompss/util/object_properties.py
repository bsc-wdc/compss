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

PyCOMPSs Utils: Object properties.

Offers some functions that check properties about objects.
For example, check if an object belongs to a module and so on.

"""
import imp

def is_module_available(module_name):
    """
    Checks if a module is available in the current Python installation.
    @param module_name: Name of the module
    @return: Boolean -> True if the module is available, False otherwise
    """
    try:
        imp.find_module(module_name)
        return True
    except:
        return False

def object_belongs_to_module(obj, module_name):
    """
    Checks if a given object belongs to a given module.
    @param obj: Object to be analysed
    @param module_name: Name of the module we want to check
    @return: Boolean -> True if obj belongs to the given module, False otherwise
    """
    return type(obj).__module__ == module_name


def has_subobjects_of_module(obj, module_name):
    """
    Detects if an object belongs to a module or, at least, has
    some sub-object that belongs to it.
    @param obj: Object to be analysed
    @param module_name: Name of the module we want to check
    @return: Boolean -> True if obj or some subobject belongs to the given module, False otherwise
    """
    if not is_module_available(module_name):
        return False
    object_stack = [obj]
    vis = set()
    while object_stack:
        current_object = object_stack.pop()
        current_object_id = id(current_object)
        # if this is the first time we find this object...
        if not current_object_id in vis:
            vis.add(current_object_id)
            # if this object belongs to our module return true
            if object_belongs_to_module(current_object, module_name):
                return True
            if hasattr(current_object, '__dict__'):
            	map(object_stack.append, current_object.__dict__.values())

    # We have found no object that belongs to our module
    return False

def has_numpy_objects(obj):
	"""
	Checks if the given object is a numpy object or
	some of its subojects are.
	@param obj: An object
	@return: Boolean -> True if obj is a numpy objects (or some of 
	its subobjects). False otherwise
	"""
	return has_subobjects_of_module(obj, 'numpy')
