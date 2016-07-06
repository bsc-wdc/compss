"""
@author: etejedor
@author: fconejer

PyCOMPSs API - Task
===================
    This file contains the class task, needed for the task definition and the
    reveal_objects function.
"""

import inspect
import os
import logging
from functools import wraps


logger = logging.getLogger(__name__)


class task(object):

    def __init__(self, *args, **kwargs):
        """
        If there are decorator arguments, the function
        to be decorated is not passed to the constructor!
        """
        self.args = args      # Not used
        self.kwargs = kwargs  # The only ones actually used: (decorators)
        self.is_instance = False

        if 'isModifier' not in self.kwargs:
            self.kwargs['isModifier'] = True
        if 'returns' not in self.kwargs:
            self.kwargs['returns'] = False
        if 'priority' not in self.kwargs:
            self.kwargs['priority'] = False

        # Pre-process decorator arguments
        from pycompss.api.parameter import Parameter, Type, Direction
        import copy

        if (not inspect.stack()[-2][3] == 'compss_worker') and (not inspect.stack()[-2][3] == 'compss_persistent_worker'):
            for arg_name in self.kwargs.keys():
                if arg_name not in ['isModifier', 'returns', 'priority']:
                    # Prevent p.value from being overwritten later by ensuring
                    # each Parameter is a separate object
                    p = self.kwargs[arg_name]
                    pcopy = copy.copy(p)  # shallow copy
                    self.kwargs[arg_name] = pcopy

        if self.kwargs['isModifier']:
            d = Direction.INOUT
        else:
            d = Direction.IN
        # Add callee object parameter
        self.kwargs['self'] = Parameter(p_type=Type.OBJECT, p_direction=d)
        if self.kwargs['returns']:
            self.kwargs['compss_retvalue'] = Parameter(p_type=Type.FILE, p_direction=Direction.OUT)
        logger.debug("Init task...")
        

    def __call__(self, f):
        """
        If there are decorator arguments, __call__() is only called
        once, as part of the decoration process! You can only give
        it a single argument, which is the function object.
        """
        # Assume it is an instance method if the first parameter of the
        # function is called 'self'
        # "I would rely on the convention that functions that will become
        # methods have a first argument named self, and other functions don't.
        # Fragile, but then, there's no really solid way."
        self.spec_args = inspect.getargspec(f)
        # print("self.spec_args: ", self.spec_args)
        if self.spec_args and len(self.spec_args[0]) and self.spec_args[0][0] == 'self':
            self.is_instance = True
        if self.kwargs['returns']:
            self.spec_args[0].append('compss_retvalue')

        # Get module (for invocation purposes in the worker)
        mod = inspect.getmodule(f)
        self.module = mod.__name__

        if(self.module == '__main__' or
           self.module == 'pycompss.runtime.launch'):
            # the module where the function is defined was run as __main__,
            # we need to find out the real module name

            # path = mod.__file__
            # dirs = mod.__file__.split(os.sep)
            # file_name = os.path.splitext(os.path.basename(mod.__file__))[0]

            # get the real module name from our launch.py variable
            path = getattr(mod, "app_path")
            dirs = path.split(os.path.sep)
            file_name = os.path.splitext(os.path.basename(path))[0]
            mod_name = file_name
            i = -1

            while True:
                new_l = len(path) - (len(dirs[i]) + 1)
                path = path[0:new_l]
                if "__init__.py" in os.listdir(path):
                    # directory is a package
                    i -= 1
                    mod_name = dirs[i] + '.' + mod_name
                else:
                    break
            self.module = mod_name

        logger.debug("Decorating function %s in module %s" % (f.__name__, self.module))

        @wraps(f)
        def wrapped_f(*args, **kwargs):
            # Check if this call is nested using the launch_pycompss_module
            # function from launch.py.
            is_nested = False
            istack = inspect.stack()
            for i_s in istack:
                if i_s[3] == 'launch_pycompss_module':
                    is_nested = True
                if i_s[3] == 'launch_pycompss_application':
                    is_nested = True
            
            if (inspect.stack()[-2][3] == 'compss_worker' or inspect.stack()[-2][3] == 'compss_persistent_worker') and (not is_nested):
                # Called from worker code, run the method
                from pycompss.util.serializer import serialize_objects

                returns = self.kwargs['returns']

                # Discover hidden objects passed as files
                real_values, to_serialize = reveal_objects(args,
                                                           self.spec_args[0],
                                                           self.kwargs,
                                                           kwargs['compss_types'],
                                                           returns)

                ret = f(*real_values)  # Llamada real de la funcion f
                # f(*args, **kwargs)

                if returns:
                    ret_filename = args[-1]
                    to_serialize.append((ret, ret_filename))

                if len(to_serialize) > 0:
                    serialize_objects(to_serialize)
            else:
                # called from the master code
                from pycompss.runtime.binding import process_task
                from pycompss.runtime.binding import Function_Type

                # Check the type of the function called.
                # inspect.ismethod(f) does not work here,
                # for methods python hasn't wrapped the function as a method yet
                # Everything is still a function here, can't distinguish yet 
                # with inspect.ismethod or isfunction
                ftype = Function_Type.FUNCTION
                class_name = ''
                if self.is_instance:
                    ftype = Function_Type.INSTANCE_METHOD
                    class_name = type(args[0]).__name__
                elif args and inspect.isclass(args[0]):
                    for n, _ in inspect.getmembers(args[0], inspect.ismethod):
                        if n == f.__name__:
                            ftype = Function_Type.CLASS_METHOD
                            class_name = args[0].__name__

                # Check the parameters in order to allow default and specific 
                # parameter values.
                # Be very careful with parameter position.
                # The included are sorted by position. The rest may not.
                num_params = len(self.spec_args[0])
                if 'compss_retvalue' in self.spec_args[0]:
                    # if the task returns a value, appears as an argument
                    num_params -= 1

                if num_params > len(args):
                    # There are default parameters
                    # Get the variable names and values that have been
                    # defined by default (get_default_args(f)).
                    default_params = get_default_args(f)
                    # dif = num_params - len(args)
                    # check_specified_params = False
                    # if len(kwargs) > 0:
                    #     # the user has specified a particular parameter
                    #     check_specified_params = True
                    argsl = list(args)  # given values

                    # Parameter Sorting
                    for p in self.spec_args[0][len(args):num_params]:
                        if p in kwargs:
                            #argsl.append(kwargs[p[0]])
                            argsl.append(kwargs[p])
                        else:
                            for dp in default_params:
                                if p in dp[0]:
                                    argsl.append(dp[1])

                    args = tuple(argsl)

                return process_task(f, ftype, self.spec_args[0], class_name,
                                    self.module, args, kwargs, self.kwargs)
                # Inicio de la creacion asincrona de la tarea.
                # Libreria de pycompss y luego c.
                # Retorna al terminar esto.

        return wrapped_f


def get_default_args(f):
    """
    Returns a dictionary of arg_name:default_values for the input function
    @param f: Function to inspect for default parameters.
    """
    a = inspect.getargspec(f)
    return zip(a.args[-len(a.defaults):], a.defaults)


def reveal_objects(values, spec_args, deco_kwargs, compss_types, returns):
    """
    Function that goes through all parameterss in order to
    find and open the files.
    @param values: The value of each parameter. Type = List
    @param spec_args: Specific arguments. Type = List
    @param deco_kwargs: The decoratos. Type = List
    @param compss_types: The types of the values. Type = List
    @param returns: If the function returns a value. Type = Boolean.
    @return: a list with the real values
    """
    from pycompss.api.parameter import Parameter, Type, Direction
    from pycompss.util.serializer import deserialize_from_file
    # from cPickle import load

    num_pars = len(spec_args)
    real_values = []
    to_serialize = []

    if returns:
        num_pars -= 1    # return value must not be passed to the function call

    for i in range(num_pars):
        spec_arg = spec_args[i]
        compss_type = compss_types[i]
        value = values[i]
        if i == 0:
            if spec_arg == 'self':  # callee object
                if deco_kwargs['isModifier']:
                    d = Direction.INOUT
                else:
                    d = Direction.IN
                deco_kwargs[spec_arg] = Parameter(p_type=Type.OBJECT, p_direction=d)
            elif inspect.isclass(value):  # class (it's a class method)
                real_values.append(value)
                continue

        p = deco_kwargs.get(spec_arg)
        if p == None:  # decoration not present, using default
            p = Parameter()
            # deco_kwargs[spec_arg] = p
        
        if compss_type == Type.FILE and p.type != Type.FILE:
            # For COMPSs it is a file, but it is actually a Python object
            logger.debug("Processing a hidden object in parameter %d", i)
            obj = deserialize_from_file(value)

            real_values.append(obj)
            if p.direction != Direction.IN:
                to_serialize.append((obj, value))
        else:
            real_values.append(value)
        '''
        # - Experimental -
        # Avoid the confusion beteen future objects and param=FILE_IN with returns=FILE_OUT
        elif compss_type == Type.FILE and p.type == Type.FILE:
            # For COMPSs it is a file, but it is actually file within the serialized file.
            logger.debug("Processing a hidden file in parameter %d", i)
            print("Processing a hidden file in parameter %d", i)
            try:
                obj = deserialize_from_file(value)
                real_values.append(obj)
                if p.direction != Direction.IN:
                    to_serialize.append((obj, value))
            except:
                # if any exception arised, then it has to be a simple value
                real_values.append(value)
        '''
            
    return real_values, to_serialize
