
from exaqute.common import ExaquteException
import traceback


_exaqute_inited = False
_temp_objects = None
_accessed_objs = []

def _traceback():
    stack = traceback.extract_stack(None, 6)
    stack.pop()  # Last entry is not usefull, it is actually line above
    return "".join(traceback.format_list(stack))


class ValueWrapper:

    def __init__(self, value, keep):
        self.value = value
        self.keep = keep
        self.deleted = False
        self.traceback = _traceback()
        if not keep:
            _register_temp_object(self)

    def unwrap_value(self):
        if self.deleted:
            raise ExaquteException("Using deleted object")
        if not self.keep and self not in _temp_objects:
            raise ExaquteException("Using temporary object after submit point, object created at {}", self.traceback)
        return self.value


def _obj_to_value(obj):
    if isinstance(obj, ValueWrapper):
        return obj.unwrap_value()
    else:
        if not obj in _accessed_objs:
            _accessed_objs.append(obj)
    return obj

def _check_accessed(obj):
    return obj in _accessed_objs

def _delete_accessed(obj):
    _accessed_objs.remove(obj)

def _init():
    global _exaqute_inited
    global _temp_objects
    if _exaqute_inited:
        raise ExaquteException("Init called twice")
    _exaqute_inited = True
    _temp_objects = set()


def _reset():
    # For testing purpose
    global _exaqute_inited
    global _temp_objects
    _exaqute_inited = False
    _temp_objects = None


def _register_temp_object(obj):
    _temp_objects.add(obj)


def _check_init():
    if not _exaqute_inited:
        raise ExaquteException("Exaqute call before init")


def _submit_point():
    global _temp_objects
    _temp_objects = set()
