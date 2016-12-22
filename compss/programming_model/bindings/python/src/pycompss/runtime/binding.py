"""
@author: etejedor
@author: fconejer

PyCOMPSs Binding - Binding
==========================
    This file contains the Python binding auxiliary classes and methods.
"""

from pycompss.api.parameter import *
from pycompss.util.serializer import *
from pycompss.util.sizer import total_sizeof

#from tempfile import mkdtemp
from shutil import rmtree

import types
import os
import re
import inspect
import logging
import traceback
from collections import *

import compss

from cPickle import dumps
from cPickle import PicklingError


python_to_compss = {types.IntType: Type.INT,          # int
                    types.LongType: Type.LONG,        # long
                    types.FloatType: Type.DOUBLE,      # float
                    types.BooleanType: Type.BOOLEAN,  # bool
                    types.StringType: Type.STRING,    # str
                    # The type of instances of user-defined classes
                    # types.InstanceType: Type.OBJECT,
                    # The type of methods of user-defined class instances
                    # types.MethodType: Type.OBJECT,
                    # The type of user-defined old-style classes
                    # types.ClassType: Type.OBJECT,
                    # The type of modules
                    # types.ModuleType: Type.OBJECT,
                    # The type of tuples (e.g. (1, 2, 3, 'Spam'))
                    types.TupleType: Type.OBJECT,
                    # The type of lists (e.g. [0, 1, 2, 3])
                    types.ListType: Type.OBJECT,
                    # The type of dictionaries (e.g. {'Bacon': 1, 'Ham': 0})
                    types.DictType: Type.OBJECT
                    }

# temp_dir = mkdtemp(prefix='pycompss', dir=os.getcwd())
temp_dir = ''
temp_obj_prefix = "/compss-serialized-obj_"

objid_to_filename = {}

task_objects = {}

# Objects that have been accessed by the main program
objs_written_by_mp = {}  # obj_id -> compss_file_name

# 1.3:
# init_logging(os.getenv('IT_HOME') + '/../Bindings/python/log/logging.json')
logger = logging.getLogger(__name__)

# Enable or disable small objects conversion to strings (using cPickle)
# cross-module variable (set/modified from launch.py)
object_conversion = False


class Function_Type:
    FUNCTION = 1
    INSTANCE_METHOD = 2
    CLASS_METHOD = 3


class Future(object):
    """
    Future object class definition (iterable).
    """
    pass
    '''
    def __init__(self, num_fos):
        self.list = [FO() for _ in xrange(num_fos)]

    def __iter__(self):
        return iter(self.list)
    '''

def start_runtime():
    """
    Starts the runtime by calling the external python library that calls the bindings-common.
    """
    logger.info("Starting COMPSs...")
    compss.start_runtime()
    logger.info("COMPSs started")


def stop_runtime():
    """
    Stops the runtime by calling the external python library that calls the bindings-common.
    Also cleans objects and temporary files created during runtime.
    """
    clean_objects()
    logger.info("Stopping COMPSs...")
    compss.stop_runtime()
    logger.info("Cleaning...")
    clean_temps()
    logger.info("COMPSs stopped")


def get_file(file_name, mode):
    """
    Calls the external python library (that calls the bindings-common) in order to request a file.
    :return: The current name of the file requested (that may have been renamed during runtime).
    """
    logger.debug("Getting file %s with mode %s" % (file_name, mode))
    compss_name = compss.get_file(file_name, mode)
    logger.debug("COMPSs file name is %s" % compss_name)
    return compss_name


def delete_file(file_name):
    """
    Calls the external python library (that calls the bindings-common) in order to request a file removal.
    :param file_name: File name to remove
    """
    logger.debug("Deleting file %s" %(file_name))
    result = compss.delete_file(file_name)
    if result == "true":
        logger.debug("File %s successfully deleted." % (file_name))
    else:
        logger.error("Failed to remove file %s." % (file_name))


def barrier():
    """
    Calls the external python library (that calls the bindings-common) in order to request a barrier.
    Wait for all tasks.
    """
    logger.debug("Wait for all tasks.")
    compss.waitForAllTasks(0) # Always 0 (not needed for the signature)


def get_logPath():
    """
    Requests the logging path to the external python library (that calls the bindings-common).
    :return: The path where to store the logs.
    """
    logger.debug("Requesting log path")
    logPath = compss.get_logging_path()
    logger.debug("Log path received: %s" % logPath)
    return logPath


def set_constraints(func_name, func_module, constraints):
    """
    Calls the external python library (that calls the bindings-common) in order to notify the runtime
    about a constraint that a function will have.
    :param func_name: Function name.
    :param func_module: Module where the function is.
    :param constraints: List of constraints.
    """
    logger.debug("Setting constraints for function %s of module %s." % (func_name, func_module))
    for key, value in constraints.iteritems():
        logger.debug("\t - %s -> %s" % (key, value))

    # Build constraints string from constraints dictionary
    constraints_string = ''
    for key, value in constraints.iteritems():
        constraints_string += key + ":" + str(value) + ";"
    logger.debug("constraints_string: %s" % constraints_string)

    # Call runtime with the name, module and constraints.
    app_id = 0          # Always 0 (not needed for the signature)
    values = []         # Not needed in python (required for C binding)
    parameterCount = 0  # Not needed in python (required for C binding)
    compss.set_constraints(app_id,
                           func_module,
                           func_name,
                           False,           # not necessary for python apps
                           False,           # not necessary for python apps
                           constraints_string,
                           parameterCount,  # not necessary for python apps
                           values)          # not necessary for python apps
    logger.debug("Constraints successfully set.")


def get_task_objects():
    """
    This method retrieves the dictionary that contains the objects used as parameters for the tasks.
    Used within the API in order to check if waiting for an INOUT object.
    :return: Dictionary containing the ids of the objects used as task parameter.
    """
    return task_objects


def synchronize(obj, mode):
    """
    Synchronization function.
    This method retrieves the value of a future object.
    Calls the runtime in order to wait for the value and when received, returns it.
    :param obj: Object to syncrhorinze.
    :param mode: Direction of the object to synchronize.
    :return: The value of the object requested.
    """
    # TODO - CUANDO SE LLAME A compss.get_file, anadir un booleano diferenciando si es fichero u objeto
    # Objetivo: mejorar el detalle de las trazas. Esto se tiene que implementar primero en el runtime, despues
    # adaptar el api de C, y finalmente anadir el booleano aqui.
    logger.debug("Synchronizing object %d with mode %s" % (id(obj), mode))

    obj_id = id(obj)
    if obj_id not in task_objects:
        return obj

    file_name = objid_to_filename[obj_id]
    compss_file = compss.get_file(file_name, mode)

    new_obj = deserialize_from_file(compss_file)
    new_obj_id = id(new_obj)

    # The main program won't work with the old object anymore, update mapping
    objid_to_filename[new_obj_id] = file_name
    task_objects[new_obj_id] = new_obj
    # Do not let python free old objects until compss_stop, otherwise python could reuse object ids.
    # del objid_to_filename[obj_id]
    # del task_objects[obj_id]

    logger.debug("Now object with id %d and %s has mapping %s" % (new_obj_id, type(new_obj), file_name))

    if mode != Direction.IN:
        objs_written_by_mp[new_obj_id] = compss_file

    return new_obj


def process_task(f, ftype, spec_args, class_name, module_name, task_args, task_kwargs, deco_kwargs):
    """
    Function that submits a task to the runtime.
    :param f: Function or method
    :param ftype: Function type
    :param spec_args: Names of the task arguments
    :param class_name: Name of the class (if method)
    :param module_name: Name of the module containing the function/method (including packages, if any)
    :param task_args: Unnamed arguments
    :param task_kwargs: Named arguments
    :param deco_kwargs: Decorator arguments
    :return: The future object related to the task return
    """
    logger.debug("TASK: %s of type %s, in module %s, in class %s" % (f.__name__, ftype, module_name, class_name))

    first_par = 0
    if ftype == Function_Type.INSTANCE_METHOD:
        has_target = True
    else:
        has_target = False
        if ftype == Function_Type.CLASS_METHOD:
            first_par = 1  # skip class parameter

    ret_type = deco_kwargs['returns']
    fu = []
    if ret_type:
        # Create future for return value
        if isinstance(ret_type, list) or isinstance(ret_type, tuple): # MULTIRETURN
            logger.debug("Multiple objects return found.")
            pos = 0
            firstTime = False
            if 'compss_retvalue' in spec_args:
                spec_args.remove('compss_retvalue') # remove single return... it contains more than one
                del deco_kwargs['compss_retvalue']  # remove single return... it contains more than one
                firstTime = True
            for i in ret_type:
                if i in python_to_compss:  # primitives, string, dic, list, tuple
                    fue = Future()
                else:
                    fue = Future()  # modules, functions, methods
                fu.append(fue)
                logger.debug("Setting object %d of %s as a future" % (id(fue), type(fue)))
                obj_id = id(fue)
                ret_filename = temp_dir + temp_obj_prefix + str(obj_id)
                objid_to_filename[obj_id] = ret_filename
                task_objects[obj_id] = fue
                task_kwargs['compss_retvalue' + str(pos)] = ret_filename
                deco_kwargs['compss_retvalue' + str(pos)] = Parameter(p_type=Type.FILE, p_direction=Direction.OUT)
                if firstTime:
                    spec_args.append('compss_retvalue'+str(pos))
                pos += 1
        else: # SIMPLE RETURN
            if ret_type in python_to_compss:  # primitives, string, dic, list, tuple
                fu = Future()
            elif inspect.isclass(ret_type):
                # For objects:
                # type of future has to be specified to allow o = func; o.func
                try:
                    fu = ret_type()
                except TypeError:
                    logger.warning("Type %s does not have an empty constructor, building generic future object" % ret_type)
                    fu = Future()
            else:
                fu = Future()  # modules, functions, methods
            logger.debug("Setting object %d of %s as a future" % (id(fu), type(fu)))
            obj_id = id(fu)
            ret_filename = temp_dir + temp_obj_prefix + str(obj_id)
            objid_to_filename[obj_id] = ret_filename
            task_objects[obj_id] = fu
            task_kwargs['compss_retvalue'] = ret_filename
    else:
        fu = None

    app_id = 0

    if class_name == '':
        path = module_name
    else:
        path = module_name + '.' + class_name

    # Infer COMPSs types from real types, except for files
    num_pars = len(spec_args)
    is_future = {}
    max_obj_arg_size = 320000
    for i in range(first_par, num_pars):
        spec_arg = spec_args[i]
        p = deco_kwargs.get(spec_arg)
        if p is None:
            logger.debug("Adding default decoration for param %s" % spec_arg)
            p = Parameter()
            deco_kwargs[spec_arg] = p
        if spec_args[0] != 'self':
            # It is a function
            if i < len(task_args):
                p.value = task_args[i]
            else:
                p.value = task_kwargs[spec_arg]
        else:
            # It is a class function
            if spec_arg == 'self':
                p.value = task_args[0]
            elif spec_arg.startswith('compss_retvalue'):
                p.value = task_kwargs[spec_arg]
            else:
                p.value = task_args[i]

        val_type = type(p.value)
        is_future[i] = (val_type == Future)
        logger.debug("Parameter " + spec_arg + "\n" +
                     "\t- Value type: " + str(val_type) + "\n" +
                     "\t- User-defined type: " + str(p.type))

        # Infer type if necessary
        if p.type is None:
            p.type = python_to_compss.get(val_type)
            if p.type is None:
                if 'getID' in dir(p.value):  # criteria for persistent object
                    p.type = Type.EXTERNAL_PSCO
                else:
                    p.type = Type.OBJECT
            logger.debug("\n\t- Inferred type: %d" % p.type)

        # Convert small objects to string if object_conversion enabled
        # Check if the object is small in order not to serialize it.
        # Evaluates the size before serializing it
        if object_conversion:
            if (p.type == Type.OBJECT or p.type == Type.STRING) and \
               not is_future.get(i) and p.direction == Direction.IN:
                if not isinstance(p.value, basestring) and \
                   isinstance(p.value, (list, dict, tuple, deque, set, frozenset)):
                    # check object size
                    # bytes = sys.getsizeof(p.value)  # does not work properly with recursive object
                    bytes = total_sizeof(p.value)
                    megabytes = bytes / 1000000  # truncate
                    logger.debug("Object size %d bytes (%d Mb)." % (bytes, megabytes))

                    if bytes < max_obj_arg_size:  # be careful... more than this value produces:
                        # Cannot run program "/bin/bash"...: error=7, La lista de argumentos es demasiado larga
                        logger.debug("The object size is less than 320 kb.")
                        real_value = p.value
                        try:
                            v = dumps(p.value)  # can not use protocol=HIGHEST_PROTOCOL due to it is sent as a parameter
                            v = '\"' + v + '\"'
                            p.value = v
                            p.type = Type.STRING
                            logger.debug("Inferred type modified (Object converted to String).")
                            # more than one object converted to string may appear
                            max_obj_arg_size -= bytes
                        except PicklingError:
                            p.value = real_value
                            p.type = Type.OBJECT
                            logger.debug("The object cannot be converted due to: not serializable.")
                    else:
                        p.type = Type.OBJECT
                        logger.debug("Inferred type reestablished to Object.")
                        # if the parameter converts to an object, release the size to be used for converted objects?
                        # No more objects can be converted
                        # max_obj_arg_size += bytes
                        # if max_obj_arg_size > 320000:
                        #     max_obj_arg_size = 320000
            '''
            ############################################################
            # Check if the object is small in order not to serialize it.
            # THIS ALTERNATIVE EVALUATES THE SIZE AFTER SERIALIZING THE PARAMETER
            if (p.type == Type.OBJECT or p.type == Type.STRING) and not is_future.get(i)
                                                                and p.direction == Direction.IN:
                if not isinstance(p.value, basestring):
                    real_value = p.value
                    try:
                        v = dumps(p.value) # can not use protocol=HIGHEST_PROTOCOL due to it is passed as a parameter
                        v = '\"' + v + '\"'
                        # check object size
                        bytes = sys.getsizeof(v)
                        megabytes = bytes / 1000000 # truncate
                        logger.debug("Object size %d bytes (%d Mb)." % (bytes, megabytes))
                        if bytes < max_obj_arg_size: # be careful... more than this value produces:
                            # Cannot run program "/bin/bash"...: error=7, La lista de argumentos es demasiado larga
                            logger.debug("The object size is less than 320 kb.")
                            p.value = v
                            p.type = Type.STRING
                            logger.debug("Inferred type modified (Object converted to String).")
                            # more than one object converted to string may appear
                            max_obj_arg_size -= bytes
                        else:
                            p.value = real_value
                            p.type = Type.OBJECT
                            logger.debug("Inferred type reestablished to Object.")
                            # if the parameter converts to an object, release the size to be used for converted objects?
                            # No more objects can be converted
                            #max_obj_arg_size += bytes
                            #if max_obj_arg_size > 320000:
                            #    max_obj_arg_size = 320000
                    except PicklingError:
                        p.value = real_value
                        p.type = Type.OBJECT
                        logger.debug("The object cannot be converted due to: not serializable.")
            ############################################################
            '''

        # Serialize objects into files
        if p.type == Type.OBJECT or is_future.get(i):
            # 2nd condition: real type can be primitive,
            # but now it's acting as a future (object)
            try:
                # Check if the Object is a list composed by future objects
                # This could be delegated to the runtime.
                # Will have to be discussed.
                if(val_type == type(list())):
                    # Is there a future object within the list?
                    if any(isinstance(v, (Future)) for v in p.value):
                        logger.debug("Found a list that contains future objects - synchronizing...")
                        mode = get_compss_mode('in')
                        p.value = map(synchronize, p.value, [mode] * len(p.value))
                turn_into_file(p)
            except PicklingError:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.exception("Pickling error exception: non-serializable object found as a parameter.")
                logger.exception(''.join(line for line in lines))
                print("[ ERROR ]: Non serializable objects can not be used as parameters (e.g. methods).")
                print("[ ERROR ]: Task: %s --> Parameter: %s" % (f.__name__, spec_arg))
                print("[ ERROR ]: Value: %s" % p.value)
                raise       # raise the exception up tu launch.py in order to point where the error is in the user code.
                # return fu  # the execution continues, but without processing this task
        elif p.type == Type.EXTERNAL_PSCO:
            manage_persistent(p)
        elif p.type == Type.INT:
            if p.value > JAVA_MAX_INT or p.value < JAVA_MIN_INT:
                # This must go through Java as a long to prevent overflow
                # with Java int
                p.type = Type.LONG
        elif p.type == Type.LONG:
            if p.value > JAVA_MAX_LONG or p.value < JAVA_MIN_LONG:
                # This must be serialized to prevent overflow with Java long
                p.type = Type.OBJECT
                turn_into_file(p)

        logger.debug("Final type for parameter %s: %d" % (spec_arg, p.type))

    # Build values and COMPSs types and directions
    values = []
    compss_types = []
    compss_directions = []
    if ftype == Function_Type.INSTANCE_METHOD:
        ra = range(1, num_pars)
        ra.append(0)  # callee is the last
    else:
        ra = range(first_par, num_pars)
    for i in ra:
        spec_arg = spec_args[i]
        p = deco_kwargs[spec_arg]
        values.append(p.value)
        if p.type == Type.OBJECT or is_future.get(i):
            compss_types.append(Type.FILE)
        else:
            compss_types.append(p.type)
        compss_directions.append(p.direction)

    # Priority
    has_priority = deco_kwargs['priority']

    if logger.isEnabledFor(logging.DEBUG):
        values_str = ''
        types_str = ''
        direct_str = ''
        for v in values:
            values_str += str(v) + " "
        for t in compss_types:
            types_str += str(t) + " "
        for d in compss_directions:
            direct_str += str(d) + " "
        logger.debug("Processing task:\n" +
                     "\t- App id: " + str(app_id) + "\n" +
                     "\t- Path: " + path + "\n" +
                     "\t- Function name: " + f.__name__ + "\n" +
                     "\t- Priority: " + str(has_priority) + "\n" +
                     "\t- Has target: " + str(has_target) + "\n" +
                     "\t- Num params: " + str(num_pars) + "\n" +
                     "\t- Values: " + values_str + "\n" +
                     "\t- COMPSs types: " + types_str + "\n" +
                     "\t- COMPSs directions: " + direct_str)

    compss.process_task(app_id,
                        path,
                        f.__name__,
                        has_priority,
                        has_target,
                        values, compss_types, compss_directions)

    return fu


def manage_persistent(p):
    p.type = Type.EXTERNAL_PSCO
    obj_id = p.value.getID()
    task_objects[obj_id] = obj_id 
    p.value = obj_id


def turn_into_file(p):
    obj_id = id(p.value)
    file_name = objid_to_filename.get(obj_id)
    if file_name is None:
        # This is the first time a task accesses this object
        task_objects[obj_id] = p.value
        file_name = temp_dir + temp_obj_prefix + str(obj_id)
        objid_to_filename[obj_id] = file_name
        logger.debug("Mapping object %d to file %s" % (obj_id, file_name))
        serialize_to_file(p.value, file_name)
    elif obj_id in objs_written_by_mp:
        # Main program generated the last version
        compss_file = objs_written_by_mp.pop(obj_id)
        logger.debug("Serializing object %d to file %s" % (obj_id, compss_file))
        serialize_to_file(p.value, compss_file, True)
    p.value = file_name


def get_compss_mode(pymode):
    if pymode.startswith('w'):
        return Direction.OUT
    elif pymode.startswith('r+') or pymode.startswith('a'):
        return Direction.INOUT
    else:
        return Direction.IN


def clean_objects():
    task_objects.clear()
    objid_to_filename.clear()
    objs_written_by_mp.clear()


def clean_temps():
    rmtree(temp_dir, True)
    cwd = os.getcwd()
    for f in os.listdir(cwd):
        if re.search("d\d+v\d+_\d+\.IT", f):
            os.remove(os.path.join(cwd, f))
