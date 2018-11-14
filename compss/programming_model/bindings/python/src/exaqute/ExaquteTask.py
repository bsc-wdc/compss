from exaqute.ExaquteParameter import *


class ExaquteTask(object):

    def __init__(self, *args, **kwargs):
        raise Exception(
            "Exaqute task decorator not implemented in the current scheduler")

    def __call__(self, f):
        raise Exception(
            "Exaqute task call code not implemented in the current scheduler")


def get_value_from_remote(obj):
    raise Exception("Get value not implemented in the current scheduler")


def barrier():
    raise Exception("Barrier not implemented in the current scheduler")


def delete_object(obj):
    raise Exception("Delete object not implemented in the current scheduler")


def compute(obj):
    raise Exception("Compute not implemented in the current scheduler")
