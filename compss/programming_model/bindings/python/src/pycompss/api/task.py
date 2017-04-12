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
@author: srodrig1

PyCOMPSs API - Task
===================
    This file contains the class task, needed for the task definition and the
    reveal_objects function.
"""
import inspect
import os
import logging
import pycompss.runtime.binding as binding
from pycompss.util.serializer import serialize_objects, deserialize_from_file, deserialize_from_string
from pycompss.util.interactiveHelpers import updateTasksCodeFile
from pycompss.util.location import i_am_at_master
from functools import wraps


# Tracing Events and Codes -> Should be equal to Tracer.java definitions
SYNC_EVENTS = 8000666
TASK_EVENTS = 8000010
TASK_EXECUTION = 120
SERIALIZATION = 121


logger = logging.getLogger('pycompss.api.task')


class task(object):

    def __init__(self, *args, **kwargs):
        """
        If there are decorator arguments, the function to be decorated is
        not passed to the constructor!
        """
        logger.debug("Init @task decorator...")

        # Defaults
        self.args = args          # Not used
        self.kwargs = kwargs      # The only ones actually used: (decorators)
        self.is_instance = False

        # Reserved PyCOMPSs keywords and default values
        if 'isModifier' not in self.kwargs:
            self.kwargs['isModifier'] = True
        if 'returns' not in self.kwargs:
            self.kwargs['returns'] = False
        if 'priority' not in self.kwargs:
            self.kwargs['priority'] = False

        # Pre-process decorator arguments
        from pycompss.api.parameter import Parameter, Type, Direction
        import copy

        if i_am_at_master():
            for arg_name in self.kwargs.keys():
                if arg_name not in ['isModifier', 'returns', 'priority']:
                    # Prevent p.value from being overwritten later by ensuring
                    # each Parameter is a separate object
                    p = self.kwargs[arg_name]
                    pcopy = copy.copy(p)  # shallow copy
                    self.kwargs[arg_name] = pcopy

        if self.kwargs['isModifier']:
            direction = Direction.INOUT
        else:
            direction = Direction.IN

        # Add callee object parameter
        self.kwargs['self'] = Parameter(p_type=Type.OBJECT, p_direction=direction)

        # Check the return type:
        if self.kwargs['returns']:
            # This condition is interesting, because a user can write returns=list
            # However, a list has the attribute __len__ but raises an exception.
            # Since the user does not indicate the length, it will be managed as a single return.
            # When the user specifies the length, it is possible to manage the elements independently.
            if not hasattr(self.kwargs['returns'], '__len__') or type(self.kwargs['returns']) is type:
                # Simple return
                retType = getCOMPSsType(self.kwargs['returns'])
                self.kwargs['compss_retvalue'] = Parameter(p_type=retType, p_direction=Direction.OUT)
            else:
                # Multi return
                i = 0
                returns = []
                for r in self.kwargs['returns']:  # This adds only one? - yep
                    retType = getCOMPSsType(r)
                    returns.append(Parameter(p_type=retType, p_direction=Direction.OUT))
                self.kwargs['compss_retvalue'] = tuple(returns)

        logger.debug("Init @task decorator finished.")

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

        # Set default booleans
        self.is_instance = False
        self.has_varargs = False
        self.has_keywords = False
        self.has_defaults = False
        self.has_return = False

        # Question: Will the first condition evaluate to false? spec_args will always be a named tuple, so
        # it will always return true if evaluated as a bool
        # Answer: The first condition evaluates if args exists (a list) and is not empty in the spec_args.
        # The second checks if the first argument in that list is 'self'. In case that the args list exists and its
        # first element is self, then the function is considered as an instance function (task defined within a class).
        if self.spec_args.args and self.spec_args.args[0] == 'self':
            self.is_instance = True

        # Check if contains *args
        if self.spec_args.varargs is not None:
            self.has_varargs = True

        # Check if contains **kwargs
        if self.spec_args.keywords is not None:
            self.has_keywords = True

        # Check if has default values
        if self.spec_args.defaults is not None:
            self.has_defaults = True

        # Check if the keyword returns has been specified by the user.
        if self.kwargs['returns']:
            self.has_return = True
            self.spec_args.args.append('compss_retvalue')

        # Get module (for invocation purposes in the worker)
        mod = inspect.getmodule(f)
        self.module = mod.__name__

        if(self.module == '__main__' or self.module == 'pycompss.runtime.launch'):
            # the module where the function is defined was run as __main__,
            # we need to find out the real module name

            # Get the real module name from our launch.py app_path global variable
            try:
                path = getattr(mod, "app_path")
            except AttributeError:
                # This exception is raised when the runtime is not running and the @task decorator is used.
                print("ERROR!!! The runtime has not been started yet. The function will be ignored.")
                print("Please, start the runtime before using task decorated functions in order to avoid this error.")
                print("Suggestion: Use the 'runcompss' command or the 'start' function from pycompss.interactive module depending on your needs.")
                return

            # Get the file name
            file_name = os.path.splitext(os.path.basename(path))[0]

            # Do any necessary preprocessing action before executing any code
            if file_name.startswith('InteractiveMode'):
                # If the file_name starts with 'InteractiveMode' means that the user is using PyCOMPSs
                # from jupyter-notebook. Convention between this file and interactive.py
                # In this case it is necessary to do a pre-processing step that consists
                # of putting all user code that may be executed in the worker on a file.
                # This file has to be visible for all workers.
                updateTasksCodeFile(f, path)
            else:
                # work as always
                pass

            # Get the module
            self.module = getModuleName(path, file_name)

        # The registration needs to be done only in the master node
        if i_am_at_master():
            logger.debug("Registering function %s in module %s" % (f.__name__, self.module))
            registerTask(f, self.module)

        # Modified variables until now that will be used later:
        #   - self.spec_args    : Function argspect (Named tuple)
        #                         e.g. ArgSpec(args=['a', 'b', 'compss_retvalue'], varargs=None, keywords=None, defaults=None)
        #   - self.is_instance  : Boolean - if the function is an instance (contains self in the spec_args)
        #   - self.has_varargs  : Boolean - if the function has *args
        #   - self.has_keywords : Boolean - if the function has **kwargs
        #   - self.has_defaults : Boolean - if the function has default values
        #   - self.has_return   : Boolean - if the function has return
        #   - self.module       : module as string (e.g. test.kmeans)
        # Other variables that will be used:
        #   - f                 : Decorated function
        #   - self.args         : Decorator args tuple (usually empty)
        #   - self.kwargs       : Decorator keywords dictionary.
        #                         e.g. {'priority': True, 'isModifier': True, 'returns': <type 'dict'>,
        #                               'self': <pycompss.api.parameter.Parameter instance at 0xXXXXXXXXX>,
        #                               'compss_retvalue': <pycompss.api.parameter.Parameter instance at 0xXXXXXXXX>}

        @wraps(f)
        def wrapped_f(*args, **kwargs):
            # args   - <Tuple>      - Contains the objects that the function has been called with (positional).
            # kwargs - <Dictionary> - Contains the named objects that the function has been called with.

            # Check if this call is nested using the launch_pycompss_module function from launch.py.
            is_nested = False
            istack = inspect.stack()
            for i_s in istack:
                if i_s[3] == 'launch_pycompss_module':
                    is_nested = True
                if i_s[3] == 'launch_pycompss_application':
                    is_nested = True

            if not i_am_at_master() and (not is_nested):
                # Task decorator worker body code.
                workerCode(f,
                           self.is_instance,
                           self.has_varargs,
                           self.has_keywords,
                           self.has_defaults,
                           self.has_return,
                           args,
                           kwargs,
                           self.kwargs,
                           self.spec_args)
            else:
                # Task decorator master body code.
                # Returns the future object that will be used instead of the
                # actual function return.
                fo = masterCode(f,
                                self.module,
                                self.is_instance,
                                self.has_varargs,
                                self.has_keywords,
                                self.has_defaults,
                                self.has_return,
                                args,
                                self.args,
                                kwargs,
                                self.kwargs,
                                self.spec_args)
                return fo
        return wrapped_f


###############################################################################
##################### TASK DECORATOR FUNCTIONS ################################
###############################################################################


def getModuleName(path, file_name):
    """
    Get the module name considering its path and filename.
    Example: runcompss -d src/kmeans.py
             path = "test/kmeans.py"
             file_name = "kmeans" (without py extension)
             return mod_name = "test.kmeans"
    :param path: relative path until the file.py from where the runcompss has been executed
    :param file_name: python file to be executed name (without the py extension)
    :return: the module name
    """
    dirs = path.split(os.path.sep)
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
    return mod_name


def registerTask(f, module):
    """
    This function is used to register the task in the runtime.
    This registration must be done only once on the task decorator initialization.
    :param f: Function to be registered
    :param module: Module that the function belongs to.
    """
    # Look for the decorator that has to do the registration
    # Since the __init__ of the decorators is independent, there is no way to pass information through them.
    # However, the __call__ method of the decorators can be used. The way that they are called is from bottom
    # to top. So, the first one to call its __call__ method will always be @task.
    # Consequently, the @task decorator __call__ method can detect the top decorator and pass a hint to order
    # that decorator that has to do the registration (not the others).
    gotFuncCode = False
    func = f
    while not gotFuncCode:
        try:
            funcCode = inspect.getsourcelines(func)
            gotFuncCode = True
        except IOError:
            # There is one or more decorators below the @task --> undecorate until possible to get the func code.
            # Example of this case: test 19: @timeit decorator below the @task decorator.
            func = func.__wrapped__
    topDecorator = getTopDecorator(funcCode)
    logger.debug("[@TASK] Top decorator of function %s in module %s: %s" % (f.__name__, module, str(topDecorator)))
    f.__who_registers__ = topDecorator
    # Include the registering info related to @task
    f.__to_register__ = {__name__: "@taskStuff"}
    # Do the task register if I am the top decorator
    if f.__who_registers__ == __name__:
        logger.debug("[@TASK] I have to do the register of function %s in module %s" % (f.__name__, module))
        logger.debug("[@TASK] %s" % str(f.__to_register__))


def workerCode(f, is_instance, has_varargs, has_keywords, has_defaults, has_return,
               args, kwargs, self_kwargs, self_spec_args):
    """
    Task decorator body executed in the workers.
    Its main function is to execute to execute the function decorated as task.
    Prior to the execution, the worker needs to retrieve the necessary parameters.
    These parameters will be used for the invocation of the function.
    When the function has been executed, stores in a file the result and finishes the worker execution.
    Then, the runtime grabs this files and does all management among tasks that involve them.
    :param f: <Function> - Function to execute
    :param is_instance: <Boolean> - If the function is an instance function.
    :param has_varargs: <Boolean> - If the function has *args
    :param has_keywords: <Boolean> - If the function has **kwargs
    :param has_defaults: <Boolean> - If the function has default parameter values
    :param has_return: <Boolean> - If the function has return
    :param args: <Tuple> - Contains the objects that the function has been called with (positional).
    :param kwargs: <Dictionary> - Contains the named objects that the function has been called with.
    :param self_kwargs: <Dictionary> - Decorator keywords dictionary.
    :param self_spec_args: <Named Tuple> - Function argspect
    """
    # Retrieve internal parameters from worker.py.
    tracing = kwargs.get('compss_tracing')
    cache_queue = kwargs.get('compss_cache_queue')
    cache_pipe = kwargs.get('compss_cache_pipe')
    process_name = kwargs.get('compss_process_name')
    local_cache = kwargs.get('compss_local_cache')

    if tracing:
        import pyextrae
        pyextrae.eventandcounters(TASK_EVENTS, 0)
        pyextrae.eventandcounters(TASK_EVENTS, SERIALIZATION)

    spec_args = self_spec_args.args

    toadd = []
    # Check if there is *arg parameter in the task
    if has_varargs:
        if binding.aargs_as_tuple:
            # If the *args are expected to be managed as a tuple:
            toadd.append(self_spec_args.varargs)
        else:
            # If the *args are expected to be managed as individual elements:
            num_aargs = len(args) - len(spec_args)
            if has_keywords:
                num_aargs -= 1
            for i in range(num_aargs):
                toadd.append('*' + self_spec_args.varargs + str(i))
    # Check if there is **kwarg parameters in the task
    if has_keywords:
        toadd.append(self_spec_args.keywords)

    returns = self_kwargs['returns']
    if has_return:
        spec_args = spec_args[:-1] + toadd + [spec_args[-1]]
    else:
        spec_args = spec_args + toadd

    # Discover hidden objects passed as files
    real_values, to_serialize = reveal_objects(args,
                                               spec_args,
                                               self_kwargs,
                                               kwargs['compss_types'],
                                               cache_queue,
                                               cache_pipe,
                                               local_cache,
                                               process_name,
                                               returns)

    if binding.aargs_as_tuple:
        # Check if there is *arg parameter in the task, so the last element (*arg tuple) has to be flattened
        if has_varargs:
            if has_keywords:
                real_values = real_values[:-2] + list(real_values[-2]) + [real_values[-1]]
            else:
                real_values = real_values[:-1] + list(real_values[-1])
    else:
        pass
    kargs = {}
    # Check if there is **kwarg parameter in the task, so the last element (kwarg dict) has to be flattened
    if has_keywords:
        kargs = real_values[-1]         # kwargs dict
        real_values = real_values[:-1]  # remove kwargs from real_values

    if tracing:
        pyextrae.eventandcounters(TASK_EVENTS, 0)
        pyextrae.eventandcounters(TASK_EVENTS, TASK_EXECUTION)

    ret = f(*real_values, **kargs)  # Real call to f function

    if tracing:
        pyextrae.eventandcounters(TASK_EVENTS, 0)
        pyextrae.eventandcounters(TASK_EVENTS, SERIALIZATION)

    if returns:
        # If there is multireturn then serialize each one on a different file
        # Multireturn example: a,b,c = fun() , where fun() has a return x,y,z
        if isinstance(returns, list) or isinstance(returns, tuple):
            num_ret = len(returns)
            total_rets = len(args) - num_ret
            rets = args[total_rets:]
            i = 0
            for ret_filename in rets:
                to_serialize.append((ret[i], ret_filename))
                i += 1
        else:  # simple return
            ret_filename = args[-1]
            to_serialize.append((ret, ret_filename))
    if len(to_serialize) > 0:
        serialize_objects(to_serialize)


def masterCode(f, self_module, is_instance, has_varargs, has_keywords, has_defaults, has_return,
               args, self_args, kwargs, self_kwargs, self_spec_args):
    """
    Task decorator body executed in the master
    :param f: <Function> - Function to execute
    :param self_module: <String> - Function module
    :param is_instance: <Boolean> - If the function belongs to an instance
    :param has_varargs: <Boolean> - If the function has *args
    :param has_keywords: <Boolean> - If the function has **kwargs
    :param has_defaults: <Boolean> - If the function has default values
    :param has_return: <Boolean> - If the function has return
    :param args: <Tuple> - Contains the objects that the function has been called with (positional).
    :param self_args: <Tuple> - Decorator args (usually empty)
    :param kwargs: <Dictionary> - Contains the named objects that the function has been called with.
    :param self_kwargs: <Dictionary> - Decorator keywords dictionary.
    :param self_spec_args: <Named Tuple> - Function argspect
    :return: Future object that fakes the real return of the task (for its delegated execution)
    """
    from pycompss.runtime.binding import process_task
    from pycompss.runtime.binding import Function_Type

    # Check the type of the function called.
    # inspect.ismethod(f) does not work here,
    # for methods python hasn't wrapped the function as a method yet
    # Everything is still a function here, can't distinguish yet
    # with inspect.ismethod or isfunction
    ftype = Function_Type.FUNCTION
    class_name = ''
    if is_instance:
        ftype = Function_Type.INSTANCE_METHOD
        class_name = type(args[0]).__name__
    elif args and inspect.isclass(args[0]):
        for n, _ in inspect.getmembers(args[0], inspect.ismethod):
            if n == f.__name__:
                ftype = Function_Type.CLASS_METHOD
                class_name = args[0].__name__

    # Build the arguments list
    # Be very careful with parameter position.
    # The included are sorted by position. The rest may not.

    # Check how many parameters are defined in the function
    num_params = len(self_spec_args.args)
    if has_return:
        num_params -= 1

    # Check if the user has defined default values and include them
    argsList = []
    if has_defaults:
        # There are default parameters
        # Get the variable names and values that have been defined by default (get_default_args(f)).
        # default_params will have a list of pairs of the form (argument, default_value)
        # Default values have to be always defined after undefined value parameters.
        default_params = get_default_args(f)
        argsList = list(args)  # Given values
        # Default parameter addition
        for p in self_spec_args.args[len(args):num_params]:
            if p in kwargs:
                argsList.append(kwargs[p])
            else:
                for dp in default_params:
                    if p in dp[0]:
                        argsList.append(dp[1])
        args = tuple(argsList)

    # List of parameter names
    vals_names = list(self_spec_args.args[:num_params])
    # List of values of each parameter
    vals = list(args[:num_params])  # first values of args are the parameters

    # Check if there are *args or **kwargs
    args_names = []
    args_vals = []
    if has_varargs:   # *args
        aargs = '*' + self_spec_args.varargs
        if binding.aargs_as_tuple:
            # If the *args are expected to be managed as a tuple:
            args_names.append(aargs)  # Name used for the *args
            args_vals.append(args[num_params:])  # last values will compose the *args parameter
        else:
            # If the *args are expected to be managed as individual elements:
            pos = 0
            for i in range(len(args[num_params:])):
                args_names.append(aargs + str(pos))  # Name used for the *args
                pos += 1
            args_vals = args_vals + list(args[num_params:])
    if has_keywords:  # **kwargs
        aakwargs = '**' + self_spec_args.keywords  # Name used for the **kwargs
        args_names.append(aakwargs)
        # The **kwargs dictionary is considered as a single dictionary object.
        args_vals.append(kwargs)

    # Build the final list of parameter names
    spec_args = vals_names + args_names
    if has_return: # 'compss_retvalue' in self_spec_args.args:
        spec_args += ['compss_retvalue']
    # Build the final list of values for each parameter
    values = tuple(vals + args_vals)

    fo = process_task(f, self_module, class_name, ftype, has_return, spec_args, values, kwargs, self_kwargs)
    # Starts the asynchronous creation of the task.
    # First calling the PyCOMPSs library and then C library (bindings-commons).
    return fo


###############################################################################
######################## AUXILIARY FUNCTIONS ##################################
###############################################################################


def getTopDecorator(code):
    """
    Retrieves the decorator which is on top of the current task decorators stack.
    :param code: Tuple which contains the task code to analyse and the number of lines of the code.
    :return: the decorator name in the form "pycompss.api.__name__"
    """
    # Code has two fields:
    # code[0] = the entire function code.
    # code[1] = the number of lines of the function code.
    funcCode = code[0]
    decoratorKeys = ("implement", "constraint", "mpi", "task")
    decorators = [l for l in funcCode if l.strip().startswith('@')]  # Could be improved if it stops when the first line without @ is found.
                                                                     # but we have to be care if a decorator is commented (# before @)
                                                                     # The strip is due to the spaces that appear before functions definitions,
                                                                     # such as class methods.
    for dk in decoratorKeys:
        for d in decorators:
            if dk in d:
                return "pycompss.api." + dk  # each decorator __name__


def getCOMPSsType(value):
    """
    Retrieve the value type mapped to COMPSs types.
    :param value: Value to analyse
    :return: The Type of the value
    """
    from pycompss.api.parameter import Type
    if type(value) is bool:
        return Type.BOOLEAN
    elif type(value) is str and len(value) == 1:
        return Type.CHAR           # Char does not exist as char. Only for strings of length 1.
    elif type(value) is str and len(value) > 1:
        return Type.STRING
    #elif type(value) is byte:     # byte does not exist in python (instead bytes is an str alias)
    #    return Type.BYTE
    # elif type(value) is short:   # short does not exist in python... they are integers.
    #    return Type.SHORT
    elif type(value) is int:
        return Type.INT
    elif type(value) is long:
        return Type.LONG
    elif type(value) is float:
        return Type.DOUBLE
    # elif type(value) is double:  # In python, floats are doubles.
    #     return Type.DOUBLE
    elif type(value) is str:
        return Type.STRING
    # elif type(value) is :       # Unavailable
    #     return Type.OBJECT
    # elif type(value) is :       # Unavailable # TODO: THIS TYPE WILL HAVE TO BE USED INSTEAD OF EXTERNAL
    #     return Type.PSCO
    # elif 'getID' in dir(value):
    #     # It is a storage object, but at this point we do not know if its going to be persistent or not.
    #     return Type.EXTERNAL_PSCO
    else:
        # Default type
        return Type.FILE


def get_default_args(f):
    """
    Returns a dictionary of arg_name:default_values for the input function
    @param f: Function to inspect for default parameters.
    """
    a = inspect.getargspec(f)
    num_params = len(a.args) - len(a.defaults)
    return zip(a.args[num_params:], a.defaults)


def reveal_objects(values,
                   spec_args, deco_kwargs, compss_types,
                   cache_queue, cache_pipe, local_cache,
                   process_name, returns):
    """
    Function that goes through all parameters in order to
    find and open the files.
    :param values: <List> - The value of each parameter.
    :param spec_args: <List> - Specific arguments.
    :param deco_kwargs: <List> - The decoratos.
    :param compss_types: <List> - The types of the values.
    :param cache_queue: <Queue> - Global cache queue.
    :param cache_pipe: <Pipe> - Global cache pipe.
    :param local_cache: <Dictionary> - Local cache dictionary.
    :param process_name: <String> - Process name (id).
    :param returns: If the function returns a value. Type = Boolean.
    :return: a list with the real values
    """
    from pycompss.api.parameter import Parameter, Type, Direction
    try:
        # Import storage libraries if possible
        from storage.api import getByID
    except ImportError:
        # If not present, import dummy functions
        from pycompss.storage.api import getByID

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
        if p is None:  # decoration not present, using default
            p = Parameter()

        import time
        def elapsed(start):
            return time.time() - start

        if compss_type == Type.FILE and p.type != Type.FILE:
            # For COMPSs it is a file, but it is actually a Python object
            logger.debug("Processing a hidden object in parameter %d", i)
            if cache_queue is not None:
                # ask the cache for the object
                t_cache_interaction_start = time.time()
                suffix_name = os.path.split(value)[-1]
                if local_cache.has_object(suffix_name):
                    obj = local_cache.get(suffix_name)
                    local_cache.hit(suffix_name)
                    logger.debug('Time to hit and assign on local cache: %.08fs'%elapsed(t_cache_interaction_start))
                else:
                    cache_queue.put((process_name.split('-')[-1], value))
                    answer = cache_pipe.recv()
                    # have we received an answer of the form (key, bytes) ?
                    # if yes, read from the indicated SHM
                    #obj = deserialize_from_file(value)
                    if isinstance(answer, tuple):
                        from shm_manager import shm_manager as SHM
                        manager = SHM(answer[0], answer[1], 0600)
                        cache_pipe.send('D') # DONE!
                        obj = deserialize_from_string(manager.read_object())
                        del manager
                        logger.debug('Time to miss on local and hit and assign in global cache: %.08fs'%elapsed(t_cache_interaction_start))
                    else:
                        obj = deserialize_from_file(value)
                        logger.debug('Time to miss on both local and global and read from disk %.08fs'%elapsed(t_cache_interaction_start))
                    t_local_cache_addition = time.time()
                    file_size = os.path.getsize(value)
                    local_cache.add(suffix_name, obj, file_size)
                    logger.debug('Time to add an object on the local cache: %.08fs'%elapsed(t_local_cache_addition))
            else:
                obj = deserialize_from_file(value)

            #if 'getID' in dir(obj) and obj.getID() is not None:   # dirty fix
            #    obj = getByID(obj.getID())
            real_values.append(obj)
            if p.direction != Direction.IN:
                to_serialize.append((obj, value))
        else:
            real_values.append(value)
    return real_values, to_serialize
