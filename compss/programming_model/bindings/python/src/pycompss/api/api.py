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
@author: etejedor
@author: fconejer

PyCOMPSs API
============
    This file defines the public PyCOMPSs API functions.
    It implements the start, stop, open and wait_on functions.
"""

from pycompss.runtime.binding import start_runtime, stop_runtime
from pycompss.runtime.binding import get_file, delete_file
from pycompss.runtime.binding import compss_barrier
from pycompss.runtime.binding import synchronize, get_compss_mode
from pycompss.runtime.binding import get_task_objects

from pycompss.runtime.binding import Future
import types


def compss_start():
    """
    Starts the runtime.
    """
    start_runtime()


def compss_stop():
    """
    Stops the runtime.
    """
    stop_runtime()


def compss_open(file_name, mode='r'):
    """
    Open a file -> Calls runtime.
    @param file_name: File name.
    @param mode: Open mode. Options = [w, r+ or a , r or empty]. Default = r
    @return: An object of 'file' type.
    @raise IOError: If the file can not be opened.
    """
    compss_mode = get_compss_mode(mode)
    compss_name = get_file(file_name, compss_mode)
    return open(compss_name, mode)


def compss_delete(file_name):
    """
    Delete a file -> Calls runtime.
    :param file_name: File name.
    """
    delete_file(file_name)


def barrier():
    """
    Perform a barrier when called.
    Stop until all the submitted tasks have finished.
    """
    compss_barrier()

def compss_wait_on(*args):
    def _compss_wait_on(obj, to_write=False):
        """
        Waits on an object.
        @param obj: Object to wait on.
        @param to_write: Write enable?. Options = [True, False]. Default = True
        @return: An object of 'file' type.
        """
        # print "Waiting on", obj
        if to_write:
            mode = 'r+'
        else:
            mode = 'r'
        compss_mode = get_compss_mode(mode)

        task_objects = get_task_objects()

        # Private function used below (recursively)
        def wait_on_list(l):
            # check if the object is in our task_objects dictionary
            from pycompss.runtime.binding import get_object_id
            obj_id = get_object_id(l)
            if obj_id in task_objects:
                return synchronize(l, compss_mode)
            else:
                if type(l) == list:
                    return [wait_on_list(x) for x in l]
                else:
                    return synchronize(l, compss_mode)

        if isinstance(obj, Future) or not isinstance(obj, types.ListType):
            return synchronize(obj, compss_mode)
        else:
            if len(obj) == 0:      # FUTURE OBJECT
                return synchronize(obj, compss_mode)
            else:
                # Will be a List
                res = wait_on_list(obj)
                return res
    ret = map(_compss_wait_on, args)
    return ret[0] if len(ret) == 1 else ret


##############
# DEPRECATED #
##############

# Version 4.0
# ==============================================================================
# def compss_wait_on(obj, to_write=True):
#     """
#     Waits on an object.
#     @param obj: Object to wait on.
#     @param to_write: Write enable?. Options = [True, False]. Default = True
#     @return: An object of 'file' type.
#     """
#     # print "Waiting on", obj
#     if to_write:
#         mode = 'r+'
#     else:
#         mode = 'r'
#     compss_mode = get_compss_mode(mode)
#
#     # Private function used below (recursively)
#     def wait_on_list(l):
#         if type(l) == list:
#             return [wait_on_list(x) for x in l]
#         else:
#             return synchronize(l, compss_mode)
#
#     if isinstance(obj, Future) or not isinstance(obj, types.ListType):
#         return synchronize(obj, compss_mode)
#     else:
#         if len(obj) == 0:  # FUTURE OBJECT
#             return synchronize(obj, compss_mode)
#         else:
#             # Will be a List
#             res = wait_on_list(obj)
#             return res
# ==============================================================================


# Version 3.0
# ==============================================================================
# def compss_wait_on(obj, to_write=True):
#     """
#     Waits on an object.
#     @param obj: Object to wait on.
#     @param to_write: Write enable?. Options = [True, False]. Default = True
#     @return: An object of 'file' type.
#     """
#     # print "Waiting on", obj
#     if to_write:
#         mode = 'r+'
#     else:
#         mode = 'r'
#     compss_mode = get_compss_mode(mode)
#
#     if isinstance(obj, Future) or not isinstance(obj, types.ListType):
#         return synchronize(obj, compss_mode)
#     else:
#         if len(obj) == 0:      # FUTURE OBJECT
#             return synchronize(obj, compss_mode)
#         else:
#             # Will be a List
#             io = iter(obj)  # get iterator
#             res = []
#             for o in io:
#                 res.append(synchronize(o, compss_mode))
#             return res
# ==============================================================================


# Version 2.0
# ==============================================================================
# def compss_wait_on(obj, to_write = True):
#      """
#      Waits on an object.
#      @param obj: Object to wait on.
#      @param to_write: Write enable?. Options = [True, False]. Default = True
#      @return: An object of 'file' type.
#      """
#      #print "Waiting on", obj
#      if to_write:
#          mode = 'r+'
#      else:
#          mode = 'r'
#      compss_mode = get_compss_mode(mode)
#
#      if isinstance(obj, Future)  or not isinstance(obj, types.ListType):
#          return synchronize(obj, compss_mode)
#      else:
#          if isinstance(obj, collections.Iterable):
#              if (len(obj) == 0): # FUTURE OBJECT
#                  return synchronize(obj, compss_mode)
#              else:
#                  if  isinstance(obj, basestring):         # if str or unicode
#                      return synchronize(obj, compss_mode)
#                  else:
#                      # will be a list, tuple or user object (iterable)
#                      io = iter(obj) # get iterator
#                      res = []
#                      for o in io:
#                          res.append(synchronize(o, compss_mode))
#                      return res
#          else:
#              # not iterable
#              return synchronize(obj, compss_mode)
# ==============================================================================


# Version 1.0
# ==============================================================================
# def compss_wait_on(obj, to_write = True):
#     """
#     Waits on an object.
#     @param obj: Object to wait on.
#     @param to_write: Write enable?. Options = [True, False]. Default = True
#     @return: An object of 'file' type.
#     """
#     #print "Waiting on", obj
#     if to_write:
#         mode = 'r+'
#     else:
#         mode = 'r'
#     compss_mode = get_compss_mode(mode)
#     return synchronize(obj, compss_mode)
# ==============================================================================
