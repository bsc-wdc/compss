
from .internals import _init, _check_init, _submit_point, ValueWrapper, _check_accessed, _delete_accessed
from exaqute.common import ExaquteException


def init():
    _init()


def get_value_from_remote(obj):
    _check_init()
    _submit_point()
    if isinstance(obj, list):
        return [get_value_from_remote(o) for o in obj]
    if not isinstance(obj, ValueWrapper):
        if not _check_accessed(obj):
            raise ExaquteException("get_value_from_remote called on non-task value, got {!r}".format(obj))
        else:
            _delete_accessed(obj)
        return obj
    else:
        if not obj.keep:
            raise ExaquteException("get_value_from_remote called on not keeped object, object created at {}".format(obj.traceback))
        return obj.unwrap_value()


def barrier():
    _check_init()
    _submit_point()


def delete_object(obj):
    _check_init()
    if not isinstance(obj, ValueWrapper):
        if not _check_accessed(obj):
            raise ExaquteException("delete_object called on non-task value, got {!r}".format(obj))
        else:
            _delete_accessed(obj)
    else:
        if not obj.keep:
            raise ExaquteException("Deleting non-keeped object, object created at {}".format(obj.traceback))
        if obj.deleted:
            raise ExaquteException("Deleting already deleted object, object created at {}".format(obj.traceback))
        obj.deleted = True
