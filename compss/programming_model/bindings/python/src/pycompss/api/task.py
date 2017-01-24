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
from functools import wraps
from pycompss.util.serializer import serialize_objects, deserialize_from_file


logger = logging.getLogger('pycompss.api.task')


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

        if (not inspect.stack()[-2][3] == 'compss_worker') and \
           (not inspect.stack()[-2][3] == 'compss_persistent_worker'):
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
            # check the return type:
            retType = getReturnType(self.kwargs['returns'])
            self.kwargs['compss_retvalue'] = Parameter(p_type=retType, p_direction=Direction.OUT)
            #self.kwargs['compss_retvalue'] = Parameter(p_type=Type.FILE, p_direction=Direction.OUT)
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
        # print("self.kwargs   : ", self.kwargs)
        # will the first condition evaluate to false? spec_args will always be a named tuple, so
        # it will always return true if evaluated as a bool
        if self.spec_args.args and self.spec_args.args[0] == 'self':
            self.is_instance = True
        if self.kwargs['returns']:
            self.spec_args.args.append('compss_retvalue')

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

            if file_name.startswith('InteractiveMode'):
                # put code into file
                updateTasksCodeFile(f, path)
            else:
                # work as always
                pass

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

            if (inspect.stack()[-2][3] == 'compss_worker' or inspect.stack()[-2][3] == 'compss_persistent_worker') \
                    and (not is_nested):
                # Called from worker code, run the method
                returns = self.kwargs['returns']

                spec_args = self.spec_args.args
                # *args
                aargs = self.spec_args.varargs
                # **kwargs
                aakwargs = self.spec_args.keywords
                toadd = []
                # Check if there is *arg parameter in the task
                if aargs is not None:
                    toadd.append(aargs)
                # Check if there is **kwarg parameters in the task
                if aakwargs is not None:
                    toadd.append(aakwargs)
                if returns is not None:
                    spec_args = spec_args[:-1] + toadd + [spec_args[-1]]
                else:
                    spec_args = spec_args[:-1] + toadd

                # Discover hidden objects passed as files
                real_values, to_serialize = reveal_objects(args,
                                                           spec_args,
                                                           self.kwargs,
                                                           kwargs['compss_types'],
                                                           returns)
                kargs = {}
                # Check if there is *arg parameter in the task, so the last element (*arg tuple) has to be flattened
                if aargs is not None:
                    if aakwargs is not None:
                        real_values = real_values[:-2] + list(real_values[-2]) + [real_values[-1]]
                    else:
                        real_values = real_values[:-1] + list(real_values[-1])
                # Check if there is **kwarg parameter in the task, so the last element (kwarg dict) has to be flattened
                if aakwargs is not None:
                    kargs = real_values[-1]         # kwargs dict
                    real_values = real_values[:-1]  # remove kwargs from real_values
                    if returns is not None:
                        kargs.pop('compss_retvalue')
                    #real_values = real_values[:-1] + [kargs]
                ret = f(*real_values, **kargs)  # Llamada real de la funcion f

                if returns:
                    if isinstance(returns, list) or isinstance(returns, tuple): # multireturn
                        num_ret = len(returns)
                        total_rets = len(args) - num_ret
                        rets = args[total_rets:]
                        i = 0
                        for ret_filename in rets:
                            # print ret[i]
                            # print ret_filename
                            to_serialize.append((ret[i], ret_filename))
                            i += 1
                    else:                         # simple return
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
                num_params = len(self.spec_args.args)
                if 'compss_retvalue' in self.spec_args.args:
                    # if the task returns a value, appears as an argument
                    num_params -= 1

                #if num_params > len(args):
                a = inspect.getargspec(f)
                if a.defaults is not None:
                    # There are default parameters
                    # Get the variable names and values that have been
                    # defined by default (get_default_args(f)).
                    # default_params will have a list of pairs of the form
                    # (argument, default_value)
                    default_params = get_default_args(f)
                    # dif = num_params - len(args)
                    # check_specified_params = False
                    # if len(kwargs) > 0:
                    #     # the user has specified a particular parameter
                    #     check_specified_params = True
                    argsl = list(args)  # given values

                    # Parameter Sorting
                    for p in self.spec_args.args[len(args):num_params]:
                        if p in kwargs:
                            argsl.append(kwargs[p])
                        else:
                            for dp in default_params:
                                if p in dp[0]:
                                    argsl.append(dp[1])

                    args = tuple(argsl)

                spec_args = self.spec_args.args
                values = args
                # *args
                aargs = self.spec_args.varargs
                # **kwargs
                aakwargs = self.spec_args.keywords
                num_args = len(args) - num_params  # # args
                vals_names = list(spec_args[:num_params])
                vals = list(args[:num_params])  # first values of args are the parameters
                arg_name = []
                arg_vals = []
                # if user uses *args
                if aargs is not None:
                    arg_name.append(aargs)              # Name used for the *args
                    arg_vals.append(args[num_params:])  # last values will compose the *args parameter
                # if user uses **kwargs
                if aakwargs is not None:
                    arg_name.append(aakwargs)
                    arg_vals.append(kwargs)

                spec_args = vals_names + arg_name
                if 'compss_retvalue' in self.spec_args.args:
                    spec_args += ['compss_retvalue']
                values = tuple(vals + arg_vals)

                # print "f: ", f
                # print "ftype: ", ftype
                # print "self.spec_args[0]: ", self.spec_args[0]
                # print "class_name: ", class_name
                # print "self.module: ", self.module
                # print "args: ", args
                # print "kwargs: ", kwargs
                # print "self.kwargs: ", self.kwargs
                # print "spec_args: ", spec_args
                # print "values: ", values

                return process_task(f, ftype, spec_args, class_name, self.module, values, kwargs, self.kwargs)
                # Starts the asyncrhonous creation of the task.
                # First calling the pycompss library and then C library (bindings-commons).

        return wrapped_f


def getReturnType(value):
    from pycompss.api.parameter import Type
    # # Always file
    return Type.FILE
'''
    # Return the correct type of the value returned (that will be within a file)
    if type(value) is bool:
        return Type.BOOLEAN
    elif type(value) is str and len(value) == 1:
        return Type.CHAR           # Char does not exist as char. Only for strings of length 1.
    # elif type(value) is bytes:
    #     return Type.STRING       # The 2.x bytes built-in is an alias to the str type.
    # elif type(value) is short:   # short does not exist in python... they are integers.
    #     return Type.SHORT
    elif type(value) is int:
        return Type.INT
    elif type(value) is long:
        return Type.LONG
    elif type(value) is float:
        return Type.FLOAT
    # elif type(value) is double:  # In python, floats are doubles.
    #     return Type.DOUBLE
    elif type(value) is str:
        return Type.STRING
    # elif type(value) is :     # Unavailable
    #     return Type.OBJECT
    # elif type(value) is :     # Unavailable
    #     return Type.PSCO
    elif 'getID' in dir(value):
        # It is a storage object, but at this point we do not know if its going to be persistent or not.
        return Type.EXTERNAL_PSCO
    else:
        # Default type
        return Type.FILE
'''

def get_default_args(f):
    """
    Returns a dictionary of arg_name:default_values for the input function
    @param f: Function to inspect for default parameters.
    """
    a = inspect.getargspec(f)
    num_params = len(a.args) - len(a.defaults)
    return zip(a.args[num_params:], a.defaults)


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
    try:
        # Import storage libraries if possible
        from storage.api import getByID
    except ImportError:
        # If not present, import dummy functions
        from pycompss.storage.api import getByID

    # print "-----------------------------------"
    # print "values: ", values
    # print "spec_args: ", spec_args
    # print "deco_kwargs: ", deco_kwargs
    # print "deco_kwargs[compss_retvalue]: ", deco_kwargs['compss_retvalue']
    # # print "deco_kwargs[compss_retvalue].type: ", deco_kwargs['compss_retvalue'].type
    # # print "deco_kwargs[compss_retvalue].value: ", deco_kwargs['compss_retvalue'].value
    # # print "deco_kwargs[compss_retvalue].direction: ", deco_kwargs['compss_retvalue'].direction
    # print "compss_types: ", compss_types
    # print "returns: ", returns
    # print "-----------------------------------"

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
            # deco_kwargs[spec_arg] = p

        if compss_type == Type.FILE and p.type != Type.FILE:
            # For COMPSs it is a file, but it is actually a Python object
            logger.debug("Processing a hidden object in parameter %d", i)
            obj = deserialize_from_file(value)
            #if 'getID' in dir(obj) and obj.getID() is not None:   # dirty fix
            #    obj = getByID(obj.getID())
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


###########################################################################
############### INTERACTIVE MODE AUXILIAR METHODS #########################
###########################################################################

def updateTasksCodeFile(f, filePath):

    if not os.path.exists(filePath):
        createTasksCodeFile(filePath)
    imports = getIPythonImports()     ######## [import\n, import\n, ...]
    #print imports
    taskCode = getTaskCode(f)         ######## {'name': str(line\nline\n...)}
    #print taskCode
    oldCode = getOldCode(filePath)    ######## {'imports':[import\n, import\n, ...], 'tasks':{'name':str(line\nline\n...), 'name':str(line\nline\n...), ...}}
    #print oldCode

    newImports = updateImports(imports, oldCode['imports'])
    newCode = updateTasks(taskCode, oldCode['tasks'])

    updateCodeFile(newImports, newCode, filePath)

    #print "Task appended to: ", filePath
    print "Task appended."


def createTasksCodeFile(filePath):
    file = open(filePath, 'a')
    file.write('\n')
    file.write("##########\n") # separator between imports and tasks 10x#
    file.close()


def getIPythonImports():
    # print globals()['In'] # is not in this scope
    ipython = globals()['__builtins__']['get_ipython']()  # retrieve the self of ipython where to look
    # If you want to show the contents of the ipython object for analysis
    # file.write(str(ipython.__dict__))
    # import pprint
    # pprint.pprint(ipython.__dict__, width=1)
    raw_code = ipython.__dict__['user_ns']['In']
    imports = []
    for i in raw_code:
        if i.startswith("from") or i.startswith("import"):
            imports.append(i+'\n')
    return imports


def getTaskCode(f):
    import inspect
    taskCode = inspect.getsource(f)
    name = ''
    lines = taskCode.split('\n')
    for line in lines:
        if line.startswith('def'):
            name = line.replace('(', ' (').split(' ')[1]
    return {name:taskCode}


def clean(linesList):
    result = []
    for l in linesList:
        if l != '\n':
            result.append(l)
    return result


def reduce(linesList):
    result = []
    jump = False
    for l in linesList:
        if l == '\n' and not jump:
            result.append(l)
            jump = True
        if l != '\n':
            result.append(l)
            jump = False
    return result


def getOldCode(filePath):
    # Read the entire file
    file = open(filePath, 'r')
    contents = file.readlines()
    file.close()
    # Separate imports from tasks
    fileImports = []
    fileTasks = []
    foundSeparator = False
    for line in contents:
        if line == '##########\n':
            foundSeparator = True
        else:
            if not foundSeparator:
                fileImports.append(line)
            else:
                fileTasks.append(line)

    fileImports = clean(fileImports)
    fileTasks = reduce(fileTasks)

    # Process tasks
    # get indices for splitting ('\n')
    ind = []
    for i in range(len(fileTasks)):
        if fileTasks[i] == '\n':
            ind.append(i)
    # split into tasks
    parts = []
    for i in range(len(ind)):
        if i == 0 and ind[i] != 0:
            parts.append(fileTasks[0:ind[i]])
        elif i == len(ind) - 1:
            parts.append(fileTasks[ind[i]:])
        else:
            parts.append(fileTasks[ind[i]:ind[i+1]])
    # Add tasks to dictionary by function name:
    tasks = {}
    for task in parts:
        code = ''
        name = ''
        for line in task:
            code += line
            if line.startswith('def'):
                name = line.replace('(',' (').split(' ')[1]
        tasks[name] = code
    return {'imports':fileImports, 'tasks':tasks}


def updateImports(newImports, oldImports):
    notInImports = []
    for i in newImports:
        already = False
        for j in oldImports:
            if i == j:
                already = True
        if not already:
            notInImports.append(i)
    # Merge the minimum imports
    imports = oldImports + notInImports
    return imports


def updateTasks(newTask, tasks):
    newTaskName = newTask.keys()[0]
    if tasks.has_key(newTaskName):
        print "WARNING! A task with the same name already exists (the previous will be deprecated)."
    tasks[newTaskName] = newTask[newTaskName]
    return tasks


def updateCodeFile(newImports, newCode, filePath):
    file = open(filePath, 'w')
    for i in newImports:
        file.write(i)
    file.write("##########\n")  # separator between imports and tasks 10x#
    file.write('\n')
    for k,v in newCode.iteritems():
        for line in v:
            file.write(line)
        file.write('\n')
    file.close()




'''
# old way to update the tasks code:

def updateTasksCodeFile(f, filePath):

    imports = getImports()
    taskCode = getTaskCode(f)

    if not os.path.exists(filePath):
        createTasksCodeFile(filePath)
    updateImports(imports, filePath)
    addNewTaskCode(taskCode, filePath)

    print "Task appended to: ", filePath


def getImports():
    # print globals()['In'] # is not in this scope
    ipython = globals()['__builtins__']['get_ipython']()  # retrieve the self of ipython where to look
    # If you want to show the contents of the ipython object for analysis
    # file.write(str(ipython.__dict__))
    # import pprint
    # pprint.pprint(ipython.__dict__, width=1)
    raw_code = ipython.__dict__['user_ns']['In']
    imports = []
    for i in raw_code:
        if i.startswith("from") or i.startswith("import"):
            imports.append(i+'\n')
    return imports


def getTaskCode(f):
    import inspect
    taskCode = inspect.getsource(f)
    return taskCode


def createTasksCodeFile(filePath):
    file = open(filePath, 'a')
    file.write('\n')
    file.write("##########\n") # separator between imports and tasks 10x#
    file.close()


def updateImports(imports, filePath):
    # Read the entire file
    file = open(filePath, 'r')
    contents = file.readlines()
    file.close()
    # Separate imports from tasks
    fileImports = []
    fileTasks = []
    foundSeparator = False
    for line in contents:
        if line == '##########\n':
            foundSeparator = True
        if not foundSeparator:
            fileImports.append(line)
        else:
            fileTasks.append(line)
    # Check the imports that are not already defined
    notInImports = []
    for i in imports:
        already = False
        for j in fileImports:
            if i == j:
                already = True
        if not already:
            notInImports.append(i)
    # Write all contents
    result = fileImports + notInImports + fileTasks
    file = open(filePath, 'w')
    for i in result:
        file.write(i)
    file.close()


def addNewTaskCode(taskCode, filePath):
    file = open(filePath, 'a')
    file.write('\n')
    file.write(taskCode)
    file.close()
'''
