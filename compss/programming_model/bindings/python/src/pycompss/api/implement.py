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
@author: fconejer

PyCOMPSs API - Implement (Versioning)
=====================================
    This file contains the class constraint, needed for the implement
    definition through the decorator.
"""
import inspect
import logging
import os
from functools import wraps
from pycompss.util.location import i_am_at_master


logger = logging.getLogger(__name__)


class implement(object):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on mpi task creation.
    """
    def __init__(self, *args, **kwargs):
        # store arguments passed to the decorator
        self.args = args
        self.kwargs = kwargs
        logger.debug("Init @implement decorator...")
        # self = itself.
        # args = not used.
        # kwargs = dictionary with the given constraints.

    def __call__(self, func):

        if i_am_at_master():
            # master code
            mod = inspect.getmodule(func)
            self.module = mod.__name__    # not func.__module__

            if(self.module == '__main__' or
               self.module == 'pycompss.runtime.launch'):
                # The module where the function is defined was run as __main__,
                # we need to find out the real module name.

                # path=mod.__file__
                # dirs=mod.__file__.split(os.sep)
                # file_name=os.path.splitext(os.path.basename(mod.__file__))[0]

                # Get the real module name from our launch.py variable
                path = getattr(mod, "app_path")

                dirs = path.split(os.path.sep)
                file_name = os.path.splitext(os.path.basename(path))[0]
                mod_name = file_name

                i = len(dirs) - 1
                while i > 0:
                    new_l = len(path) - (len(dirs[i]) + 1)
                    path = path[0:new_l]
                    if "__init__.py" in os.listdir(path):
                        # directory is a package
                        i -= 1
                        mod_name = dirs[i] + '.' + mod_name
                    else:
                        break
                self.module = mod_name

            # Include the registering info related to @implement
            func.__to_register__[__name__] = "@implementStuff"
            # Do the task register if I am the top decorator
            if func.__who_registers__ == __name__:
                logger.debug("[@IMPLEMENT] I have to do the register of function %s in module %s" % (func.__name__, self.module))
                logger.debug("[@IMPLEMENT] %s" % str(func.__to_register__))

            #logger.debug("Registering Implement parameters for function %s of module %s" % (func.__name__, self.module))
            for key, value in self.kwargs.iteritems():
                logger.debug("%s -> %s" % (key, value))
            # set_constraints(func.__name__, self.module, self.kwargs)
        else:
            # worker code
            pass


        @wraps(func)
        def implement_f(*args, **kwargs):
            # This is executed only when called.
            logger.debug("Executing implement_f wrapper.")

            # The 'self' for a method function is passed as args[0]
            slf = args[0]

            # Replace and store the attributes
            saved = {}
            for k, v in self.kwargs.items():
                if hasattr(slf, k):
                    saved[k] = getattr(slf, k)
                    setattr(slf, k, v)

            # Call the method
            ret = func(*args, **kwargs)

            # Put things back
            for k, v in saved.items():
                setattr(slf, k, v)

            return ret
        implement_f.__doc__ = func.__doc__
        return implement_f
