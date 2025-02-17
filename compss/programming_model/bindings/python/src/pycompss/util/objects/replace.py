#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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

# -*- coding: utf-8 -*-

"""
SOURCE CODE TAKEN FROM BEN KURTOVIC'S GITHUB REPO replace.py FILE.

https://gist.github.com/earwig/28a64ffb94d51a608e3d

+ Added typing
+ pydocstyle
"""

import ctypes
import sys
from ctypes import pythonapi as api
from types import (
    BuiltinFunctionType,
    GetSetDescriptorType,
    MemberDescriptorType,
    MethodType,
)
from pycompss.util.typing_helper import typing

try:
    import guppy
    from guppy.heapy import Path
except Exception:
    raise ImportError("Cannot use local decorator without guppy!")

hp = guppy.hpy()


def _w(x: int):  # NOSONAR
    def f():  # NOSONAR
        x  # NOSONAR

    return f


if sys.version_info < (3, 0):
    CellType = type(_w(0).func_closure[0])
else:
    CellType = type(_w(0).__closure__[0])

del _w


# -----------------------------------------------------------------------------
def _write_struct_attr(addr: int, value: typing.Any, add_offset: int) -> None:
    ptr_size = ctypes.sizeof(ctypes.py_object)
    ptrs_in_struct = (3 if hasattr(sys, "getobjects") else 1) + add_offset
    offset = ptrs_in_struct * ptr_size + ctypes.sizeof(ctypes.c_ssize_t)
    ref = ctypes.byref(ctypes.py_object(value))
    ctypes.memmove(addr + offset, ref, ptr_size)


def _replace_attribute(
    source: typing.Any,
    rel: str,
    new: typing.Any,
) -> None:
    if isinstance(source, (MethodType, BuiltinFunctionType)):
        if rel == "__self__":
            # Note: PyMethodObject->im_self and PyCFunctionObject->m_self
            # have the same offset
            _write_struct_attr(id(source), new, 1)
            return
        if rel == "im_self":
            return  # Updated via __self__
    if isinstance(source, type):
        if rel == "__base__":
            return  # Updated via __bases__
        if rel == "__mro__":
            return  # Updated via __bases__ when important, otherwise futile
    if isinstance(source, (GetSetDescriptorType, MemberDescriptorType)):
        if rel == "__objclass__":  # NOSONAR
            _write_struct_attr(id(source), new, 0)
            return
    try:
        setattr(source, rel, new)
    except TypeError:
        print("Unknown R_ATTRIBUTE (read-only):", rel, type(source))
    except AttributeError:
        print("Unknown R_ATTRIBUTE (read-only):", rel, type(source))


def _replace_indexval(
    source: typing.Any, rel: typing.Any, new: typing.Any
) -> None:
    if isinstance(source, tuple):
        temp = list(source)
        temp[rel] = new
        replace(source, tuple(temp))
        return
    source[rel] = new


def _replace_indexkey(
    source: typing.Any, rel: typing.Any, new: typing.Any
) -> None:
    source[new] = source.pop(list(source.keys())[rel])


def _replace_interattr(source: typing.Any, rel: str, new: typing.Any) -> None:
    if isinstance(source, CellType):
        api.PyCell_Set(ctypes.py_object(source), ctypes.py_object(new))
        return
    if rel == "ob_type":
        source.__class__ = new
        return
    print("Unknown R_INTERATTR:", rel, type(source))


def _replace_local_var(source: typing.Any, rel: str, new: typing.Any) -> None:
    source.f_locals[rel] = new
    api.PyFrame_LocalsToFast(ctypes.py_object(source), ctypes.c_int(0))


_RELATIONS = {
    Path.R_ATTRIBUTE: _replace_attribute,
    Path.R_INDEXVAL: _replace_indexval,
    Path.R_INDEXKEY: _replace_indexkey,
    Path.R_INTERATTR: _replace_interattr,
    Path.R_LOCAL_VAR: _replace_local_var,
}


def _path_key_func(path: Path) -> int:
    reltype = type(path.path[1]).__bases__[0]
    return 1 if reltype is Path.R_ATTRIBUTE else 0


def replace(old: typing.Any, new: typing.Any) -> None:
    """Replace the old object with the new object.

    :param old: Old object.
    :param new: New object.
    :returns: None.
    """
    for path in sorted(hp.iso(old).pathsin, key=_path_key_func):
        relation = path.path[1]
        try:
            func = _RELATIONS[type(relation).__bases__[0]]
        except KeyError:
            print("Unknown relation:", relation, type(path.src.theone))
            continue
        func(path.src.theone, relation.r, new)


# Commented out due to fails with mypy.
# # ---------------------------------------------------------------------------
# class A(object):     # NOSONAR
#     def func(self):  # NOSONAR
#         return self  # NOSONAR
#
#
# class B(object):  # NOSONAR
#     pass          # NOSONAR
#
#
# class X(object):  # NOSONAR
#     cattr = None  # type: typing.Any
#     iattr = None  # type: typing.Any
#
#
# def sure(obj):
#     def inner():
#         return obj
#
#     return inner
#
#
# def gen(obj):
#     while 1:
#         yield obj
#
#
# class S(object):              # NOSONAR
#     # __slots__ = ("p", "q")  # NOSONAR
#
#     def __init__(self):
#         self.p = None  # type: typing.Any
#         self.q = None  # type: typing.Any
#
#
# class T(object):              # NOSONAR
#     # __slots__ = ("p", "q")  # NOSONAR
#
#     def __init__(self):
#         self.p = None  # type: typing.Any
#         self.q = None  # type: typing.Any
#
#
# class U(object):  # NOSONAR
#     pass          # NOSONAR
#
#
# class V(object):  # NOSONAR
#     pass          # NOSONAR
#
#
# class W(U):  # NOSONAR
#     pass     # NOSONAR
#
#
# # ---------------------------------------------------------------------------
# a = A()
# b = B()
#
# X.cattr = a
# x = X()
# x.iattr = a
# d = {a: a}
# L = [a]
# t = (a,)
# f = a.func
# meth = a.__sizeof__
# clo = sure(a)
# g = gen(a)
# s = S()
# s.p = a
# u = U()
# ud = U.__dict__["__dict__"]
# s.q = S
# sd = S.q
#
#
# # ---------------------------------------------------------------------------
# def examine_vars(id1, id2, id3):
#     def ex(v, id_):  # NOSONAR
#         return str(v) + ("" if id(v) == id_ else " - ERROR!")
#     print("dict (local var):  ", ex(a, id1))
#     print("dict (class attr): ", ex(X.cattr, id1))
#     print("dict (inst attr):  ", ex(x.iattr, id1))
#     print("dict (key):        ", ex(list(d.keys())[0], id1))
#     print("dict (value):      ", ex(list(d.values())[0], id1))
#     print("list:              ", ex(L[0], id1))
#     print("tuple:             ", ex(t[0], id1))
#     print("method (instance): ", ex(f(), id1))
#     print("method (builtin):  ", ex(meth.__self__, id1))
#     print("closure:           ", ex(clo(), id1))
#     print("frame (generator): ", ex(next(g), id1))
#     print("slots:             ", ex(s.p, id1))
#     print("class (instance):  ", ex(type(u), id2))
#     print("class (subclass):  ", ex(W.__bases__[0], id2))
#     print("class (g/s descr): ", ex(ud.__get__(u, U) or type(u), id2))
#     print("class (mem descr): ", ex(sd.__get__(s, S), id3))
#
#
# # For testing purposes:
# # if __name__ == "__main__":
# #     examine_vars(id(a), id(U), id(S))
# #     print("-" * 35)
# #     replace(a, b)
# #     replace(U, V)
# #     replace(S, T)
# #     print("-" * 35)
# # examine_vars(id(b), id(V), id(T))
