### 
# Recipe source:  http://en.sharejs.com/python/12922
# Improved with my sugar.
###

###
#
#  W A R N I N G
#
#  This recipe is obsolete!
#
#  When you are looking for copying and pickling functionality for generators
#  implemented in pure Python download the
#
#             generator_tools  for python 2.5, 2.6 or 3.2+
#
# package at the cheeseshop or at www.fiber-space.de
#
###

import cPickle as pickle
import marshal
import types
import new
import copy
import sys
import copy_reg
from opcode import*
import threading

#########################################################################
########################## GENERATORS ###################################
#########################################################################

def copy_generator(f_gen):
    '''
    Function used to copy a generator object.

    @param f_gen: generator object.
    @return: pair (g_gen, g) where g_gen is a new generator object and g a generator
             function g producing g_gen. The function g is created from f_gen.gi_frame.

    Usage: function copies a running generator.

        def inc(start, step = 1):
            i = start
            while True:
                yield i
                i+= step

        # >>> inc_gen = inc(3)
        # >>> inc_gen.next()
        # 3
        # >>> inc_gen.next()
        # 4
        # >>> inc_gen_c, inc_c = copy_generator(inc_gen)
        # >>> inc_gen_c.next() == inc_gen.next()
        # True
        # >>> inc_gen_c.next()
        # 6

    Implementation strategy:

        Inspecting the frame of a running generator object f provides following important
        information about the state of the generator:

           - the values of bound locals inside the generator object
           - the last bytecode being executed

        This state information of f is restored in a new function generator g in the following way:

           - the signature of g is defined by the locals of f ( co_varnames of f ). So we can pass the
             locals to g inspected from the current frame of running f. Yet unbound locals are assigned
             to None.

             All locals will be deepcopied. If one of the locals is a generator object it will be copied
             using copy_generator. If a local is not copyable it will be assigned directly. Shared state
             is therefore possible.

           - bytecode hack. A JUMP_ABSOLUTE bytecode instruction is prepended to the bytecode of f with
             an offset pointing to the next unevaluated bytecode instruction of f.

    Corner cases:

        - an unstarted generator ( last instruction = -1 ) will be just cloned.

        - if a generator has been already closed ( gi_frame = None ) a ValueError exception
          is raised.
    '''
    if not f_gen.gi_frame:
        raise ValueError("Can't copy closed generator")
    f_code = f_gen.gi_frame.f_code
    offset = f_gen.gi_frame.f_lasti
    locals = f_gen.gi_frame.f_locals

    if offset == -1:  # clone the generator
        argcount = f_code.co_argcount
    else:
        # bytecode hack - insert jump to current offset
        # the offset depends on the version of the Python interpreter
        if sys.version_info[:2] == (2, 4):
            offset += 4
        elif sys.version_info[:2] == (2, 5):
            offset += 5
        start_sequence = (opmap["JUMP_ABSOLUTE"],)+divmod(offset, 256)[::-1]
        modified_code = "".join([chr(op) for op in start_sequence])+f_code.co_code
        argcount = f_code.co_nlocals

    varnames = list(f_code.co_varnames)
    for i, name in enumerate(varnames):
        loc = locals.get(name)
        if isinstance(loc, types.GeneratorType):
            varnames[i] = copy_generator(loc)[0]
        else:
            try:
                varnames[i] = copy.deepcopy(loc)
            except TypeError:
                varnames[i] = loc

    new_code = new.code(argcount,
                        f_code.co_nlocals,
                        f_code.co_stacksize,
                        f_code.co_flags,
                        modified_code,
                        f_code.co_consts,
                        f_code.co_names,
                        f_code.co_varnames,
                        f_code.co_filename,
                        f_code.co_name,
                        f_code.co_firstlineno,
                        f_code.co_lnotab)
    g = new.function(new_code, globals(),)
    g_gen = g(*varnames)
    return g_gen, g


class any_obj:
    "Used to create objects for spawning arbitrary attributes by assignment"


class GeneratorSnapshot(object):
    '''
    Object used to hold data for living generators.
    '''
    def __init__(self, f_gen):
        f_code = f_gen.gi_frame.f_code
        self.gi_frame = any_obj()
        self.gi_frame.f_code = any_obj()
        self.gi_frame.f_code.__dict__.update((key, getattr(f_code, key))
                                             for key in dir(f_code) if key.startswith("co_"))
        self.gi_frame.f_lasti = f_gen.gi_frame.f_lasti
        self.gi_frame.f_locals = {}
        for key, value in f_gen.gi_frame.f_locals.items():
            if isinstance(value, types.GeneratorType):
                self.gi_frame.f_locals[key] = GeneratorSnapshot(value)
            else:
                self.gi_frame.f_locals[key] = value


def pickle_generator_filename(f_gen, filename):
    '''
    @param f_gen: generator object
    @param filename: destination file for pickling generator
    '''
    output_pkl = open(filename, "wb")
    f_gen.next()  # jump one ahead for not to repeat when unpickling
    pickle.dump(GeneratorSnapshot(f_gen), output_pkl)


def unpickle_generator_filename(filename):
    '''
    @param filename: source file of pickled generator
    '''
    input_pkl = open(filename, "rb")
    gen_snapshot = pickle.load(input_pkl)
    return copy_generator(gen_snapshot)[0]


#########################################################################
######################## MODULE OBJECTS #################################
#########################################################################

def reduce_mod(m):
    assert sys.modules[m.__name__] is m
    return rebuild_mod, (m.__name__,)


def rebuild_mod(name):
    __import__(name)
    return sys.modules[name]


#########################################################################
######################## THREAD EVENTS ##################################
#########################################################################

def unserialize_event(isset):
    e = threading.Event()
    if isset:
        e.set()
    return e
                    
def serialize_event(e):
   return unserialize_event, (e.isSet(),)
                        

#########################################################################
######################### THREAD LOCK ###################################
#########################################################################

def unserialize_lock(locked):
    from threading import Lock
    lock = Lock()
    if locked:
        if not lock.acquire(False):
            raise UnpicklingError("Cannot acquire lock")
    return lock

def serialize_lock(l):
   return unserialize_lock, (l.locked(),)


#########################################################################
########################### ELLIPSIS ####################################
#########################################################################

def unserialize_ellipsis(el):
    return eval(el) 
                                    
def serialize_ellipsis(e):
    return unserialize_ellipsis, (e.__repr__(),)


#########################################################################
######################### QUIT - EXIT ###################################
#########################################################################

def unserialize_quit(q):
    return eval(q)
    
def serialize_quit(q):
    return unserialize_quit, (q.__repr__(),)
     
#########################################################################
####################### sys.FunctionType ################################
#########################################################################

def reducefunction(func):
    return (restorefunction, (func.func_name, marshal.dumps(func.func_code)))

def restorefunction(func_name, dump):
    return types.FunctionType(marshal.loads(dump), globals(), func_name)


##############
# Interfaces #
##############

# ----------------- GENERATORS --------------------

def pickle_generator(f_gen, f, serializer):
    '''
    Pickle a generator and store the serialization result in a file.
    :param f_gen: generator object.
    :param f: destination file for pickling generator.
    '''
    f_gen.next()  # jump one ahead for not to repeat when unpickling
    serializer.dump(GeneratorSnapshot(f_gen), f)


def unpickle_generator(f):
    '''
    Unpickle a generator from a file.
    :param f: source file of pickled generator.
    :return: the generator from file.
    '''
    gen_snapshot = pickle.load(f)
    return copy_generator(gen_snapshot)[0]


def getPickled_generator(f_gen):
    '''
    Retrieve a generator that has been pickled.
    :param f_gen: generator object.
    :param f: destination file for pickling generator.
    :return: a generator.
    '''
    f_gen.next()  # jump one ahead for not to repeat when unpickling
    return GeneratorSnapshot(f_gen)


# ----------------- MODULE OBJECTS --------------------

def pickle_module_object(mo, f, protocol):
    '''
    Pickle a module object and store the serialization result in a file.
    :param mo: module object
    :param f: destination file for pickling the module object
    :param protocol: pickling protocol
    '''
    copy_reg.pickle(type(sys), reduce_mod)
    pickle.dump(mo, f, protocol)


def unpickle_module_object(f):
    '''
    Unpickle a module object from a file.
    :param f: source file of pickled module object.
    :return: a module object.
    '''
    return pickle.load(f)
    
    
# ----------------- OTHERS --------------------
    