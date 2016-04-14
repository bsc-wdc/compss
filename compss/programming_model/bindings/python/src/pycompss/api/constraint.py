"""
@author: fconejer

PyCOMPSs API - Constraint
=========================
    This file contains the class constraint, needed for the constraint
    definition through the decorator.
"""
import inspect
import logging
import os
from functools import wraps


logger = logging.getLogger(__name__)


class constraint(object):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on task constraint creation.
    """
    def __init__(self, *args, **kwargs):
        # store arguments passed to the decorator
        self.args = args
        self.kwargs = kwargs
        logger.debug("Init constraint.")
        # self = itself.
        # args = not used.
        # kwargs = dictionary with the given constraints.

    def __call__(self, func):

        if not inspect.stack()[-2][3] == 'compss_worker':
            # master code
            from pycompss.runtime.binding import set_constraints

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
                i = -1

                while True:
                    new_l = len(path) - (len(dirs[i]) + 1)
                    path = path[0:new_l]
                    if "__init__.py" in os.listdir(path):
                        # Directory is a package
                        i -= 1
                        mod_name = dirs[i] + '.' + mod_name
                    else:
                        break
                self.module = mod_name

            logger.debug("Registering constraints for function %s of module %s"
                         % (func.__name__, self.module))
            for key, value in self.kwargs.iteritems():
                logger.debug("%s -> %s" % (key, value))
            set_constraints(func.__name__, self.module, self.kwargs)
        else:
            # worker code
            pass

        @wraps(func)
        def constrained_f(*args, **kwargs):
            # This is executed only when called.
            logger.debug("Executing constrained_f wrapper.")

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
        constrained_f.__doc__ = func.__doc__
        return constrained_f
