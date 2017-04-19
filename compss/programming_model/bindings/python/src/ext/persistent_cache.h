/*
  Copyright 2017 Barcelona Supercomputing Center (www.bsc.es)

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

/*
  An efficient object cache.
  @author: srodrig1 (sergio.rodriguez at bsc.es)
*/
#pragma once
#include <Python.h>
#include <set>
#include <map>
#include <cassert>
#include <iostream>
#include <structmember.h>

/*
  Cache objects are wrapped into cached_objects.
  A cached_object is a tuple that contains some useful fields
  as hit count, last hit time and similar (apart from the object itself).

  A Cache object stores and sorts cached_objects
  Note that cached_object is visible from Python. This allows us to define
  custom comparison functions with no need to recompile the C++ module
*/
typedef struct {
  PyObject_HEAD
  // Identifier (in PyCOMPSs it is a file name)
  std::string id;
  // Object size (in bytes).
  long long object_size;
  // The object itself
  PyObject* obj;
  // Hit count
  long long hit_count;
  // Last hit time
  long long last_hit_time;
  // Pointer to the comparison function of the "mother" cache
  // object
  PyObject* comparison_function;
} cached_object;

static char* cached_object_member_names[] = {
  (char*)"object_size",
  (char*)"hit_count",
  (char*)"last_hit_time"
};

static char* cached_object_member_descriptions[] = {
  (char*)"Object size (in bytes)",
  (char*)"Hit count of this object",
  (char*)"Last access time on this object"
};

static PyMemberDef cached_object_members[] = {
  {cached_object_member_names[0], T_LONG,
  offsetof(cached_object, object_size), 0, cached_object_member_descriptions[0]},
  {cached_object_member_names[1], T_LONG,
  offsetof(cached_object, hit_count), 0, cached_object_member_descriptions[1]},
  {cached_object_member_names[2], T_LONG,
  offsetof(cached_object, last_hit_time), 0,
  cached_object_member_descriptions[2]},
  {NULL} /* Sentinel */
};

/*
  Empty, returns zero at the moment.
*/
static int cached_object_init(cached_object* self,
  PyObject *args, PyObject *kwds);

/*
  Equivalent to the destructor method.
  It decreases the wrapped object's refcount.
*/
static void cached_object_dealloc(cached_object* self);

/*
  Constructor
*/
static PyObject* cached_object_new(PyTypeObject* type,
                                   PyObject* args,
                                   PyObject* kwds);

/*
  Getter of the object's id.
  Its only purpose is to make visible the object's identifier but
  it should be never used
*/
static PyObject* cached_object_get_id(PyObject* self, PyObject* args);

/*
  Getter of the object's reference.
  Same comments as cached_object_get_id
*/
static PyObject* cached_object_get_obj(PyObject* self, PyObject* args);

static PyMethodDef cached_object_methods[] = {
  {"get_id", cached_object_get_id, METH_VARARGS,
   "Returns the wrapped object's identifier"},
  {"get_obj", cached_object_get_obj, METH_VARARGS,
   "Returns a reference to the wrapped object"},
   {NULL} /* Sentinel */
};

static char* cached_object_doc = (char*)
"\nA wrapper that is used by the persistent_cache class.\n\
It's purpose is to store additional information on objects as\n\
number of acceses, last access time and similar stuff.\n";

static PyTypeObject cached_objectType = {
  PyObject_HEAD_INIT(NULL)
  0,                                          /*ob_size*/
  "persistent_cache.cached_object",           /*tp_name*/
  sizeof(cached_object),                      /*tp_basicsize*/
  0,                                          /*tp_itemsize*/
  (destructor)cached_object_dealloc,          /*tp_dealloc*/
  0,                                          /*tp_print*/
  0,                                          /*tp_getattr*/
  0,                                          /*tp_setattr*/
  0,                                          /*tp_compare*/
  0,                                          /*tp_repr*/
  0,                                          /*tp_as_number*/
  0,                                          /*tp_as_sequence*/
  0,                                          /*tp_as_mapping*/
  0,                                          /*tp_hash */
  0,                                          /*tp_call*/
  0,                                          /*tp_str*/
  0,                                          /*tp_getattro*/
  0,                                          /*tp_setattro*/
  0,                                          /*tp_as_buffer*/
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,   /*tp_flags*/
  cached_object_doc,                          /* tp_doc */
  0,                                          /* tp_traverse */
  0,                                          /* tp_clear */
  0,                                          /* tp_richcompare */
  0,                                          /* tp_weaklistoffset */
  0,                                          /* tp_iter */
  0,                                          /* tp_iternext */
  cached_object_methods,                      /* tp_methods */
  cached_object_members,                      /* tp_members */
  0,                                          /* tp_getset */
  0,                                          /* tp_base */
  0,                                          /* tp_dict */
  0,                                          /* tp_descr_get */
  0,                                          /* tp_descr_set */
  0,                                          /* tp_dictoffset */
  (initproc)cached_object_init,               /* tp_init */
  0,                                          /* tp_alloc */
  cached_object_new,                          /* tp_new */
};

/*
  Default comparison function between cached objects
*/
static PyObject* default_comparison_function(PyObject* self, PyObject* args);

bool _default_comparison_function(const cached_object& a, const cached_object& b);

/*
  STL containers that accept custom comparators wants them in some of
  the following ways:
  1) Implicit in overloaded (or implemented) operators
  2) As functions of the form struct {bool op()(const obj1, const obj2)}
  Our chosen alternative is 2), so here is the comparator's signature.
  In order to support all kind of comparison functions, this function is
  only a wrapper to the actual comparison function, which is an attribute
  of the cached objects.
*/
struct cached_object_comparator {
  bool operator()(const cached_object& a, const cached_object& b) {
    PyObject* _a = (PyObject*)&a;
    PyObject* _b = (PyObject*)&b;
    if(a.comparison_function == NULL) {
      return _default_comparison_function(a, b);
    }
    // It makes no sense (since it defines no valid ordering) to compare two
    // cached objects that have different ordering criteria
    assert(a.comparison_function == b.comparison_function);
    PyObject* comparator = a.comparison_function;
    Py_INCREF(_a); Py_INCREF(_a);
    Py_INCREF(_b); Py_INCREF(_b);
    Py_INCREF(comparator);
    PyObject* function_args = PyTuple_Pack(2, _a, _b);
    PyObject* call_result = PyObject_CallObject(comparator, function_args);
    Py_DECREF(function_args);
    Py_DECREF(_a);
    Py_DECREF(_b);
    Py_DECREF(comparator);
    return call_result == Py_True;
  }
};
/*
    Module methods
*/
static PyMethodDef module_methods[] = {
    {"default_comparison_function", default_comparison_function, METH_VARARGS,
     (char*)"Default comparison function"},
    {NULL, NULL, 0, NULL} /* Sentinel */
};

/*
  The cache itself
*/
typedef struct {
  PyObject_HEAD
  // This set allows us to keep the cached objects sorted by their priority
  std::set< cached_object, cached_object_comparator >* S;
  // With this trick we can also retrieve and query random objects in O(|s|*logn)
  // time. In C++11 this could be an unordered_map (HashMap), which would lead
  // to constant-time queries. However, I don't think that this difference
  // will ever be significant enough to consider switching to C++11
  std::map< std::string, cached_object >* H;
  // Internal cache time
  long long current_time;
  // Maximum byte size
  long long size_limit;
  // Current sum of cached objects size
  long long current_size;
  // Comparison function (see cached_object_comparator)
  PyObject* comparator;
} Cache;

static char* Cache_doc = (char*)
"Object cache (pending to document!)";


static PyMemberDef Cache_members[] = {
  {NULL} /* Sentinel */
};

static void Cache_dealloc(Cache* self);

/*
  Empty, returns zero at the moment
*/
static int Cache_init(Cache* self, PyObject* args, PyObject* kwds);

/*
  ALL arguments must be keyword args. They are all optional and they all have
  default values. The arguments are the following:
  - size_limit = size in bytes
  - comparison_function = function that compares cached objects, see
    default_comparison_function as example
*/
static PyObject* Cache_new(PyTypeObject* type, PyObject* args, PyObject* kwds);

/*
  Adds an element to the Cache. The element should not exist.
  Params: (self), identifier, obj, [size]
*/
static PyObject* Cache_add(PyObject* self, PyObject* args);

/*
  Checks if the cache has some given object
*/
static PyObject* Cache_has_object(PyObject* self, PyObject* args);

/*
  Adds a hit to an object and updates its last_hit_time
*/
static PyObject* Cache_hit(PyObject* self, PyObject* args);

/*
  Given an identifier, returns its corresponding object (if any)
*/
static PyObject* Cache_get(PyObject* self, PyObject* args);

/*
  Delete an object
*/
static PyObject* Cache_delete(PyObject* self, PyObject* args);


/*
  Get object with least priority
*/
static PyObject* Cache_get_last(PyObject* self, PyObject* args);

/*
  Checks if cache is empty
*/
static PyObject* Cache_is_empty(PyObject* self, PyObject* args);

/*
  Modifies the wrapped object of a cached object to a new one.
  This new object will have the same hitcount and last_hit_time as
  the previous one
*/
static PyObject* Cache_set_object(PyObject* self, PyObject* args);

static PyMethodDef Cache_methods[] = {
  {"add", Cache_add, METH_VARARGS,
   "Adds an object to the cache"},
  {"has_object", Cache_has_object, METH_VARARGS,
   "Checks if an object is inside the cache or not"},
  {"hit", Cache_hit, METH_VARARGS,
   "Hits a cached object"},
  {"get", Cache_get, METH_VARARGS,
   "Given a id, returns its object"},
  {"delete", Cache_delete, METH_VARARGS,
   "Given a id, deletes its associate object"},
  {"get_last", Cache_get_last, METH_VARARGS,
   "Returns the object with least priority"},
  {"is_empty", Cache_is_empty, METH_VARARGS,
   "Checks if cache is empty"},
  {"set_object", Cache_set_object, METH_VARARGS,
   "Given an id and some object obj, make obj the new wrapped object\
   by the cached_object with identifier id"},
    {NULL} /* Sentinel */
};


static PyTypeObject CacheType = {
  PyObject_HEAD_INIT(NULL)
  0,                                          /*ob_size*/
  "persistent_cache.Cache",                   /*tp_name*/
  sizeof(Cache),                              /*tp_basicsize*/
  0,                                          /*tp_itemsize*/
  (destructor)Cache_dealloc,                  /*tp_dealloc*/
  0,                                          /*tp_print*/
  0,                                          /*tp_getattr*/
  0,                                          /*tp_setattr*/
  0,                                          /*tp_compare*/
  0,                                          /*tp_repr*/
  0,                                          /*tp_as_number*/
  0,                                          /*tp_as_sequence*/
  0,                                          /*tp_as_mapping*/
  0,                                          /*tp_hash */
  0,                                          /*tp_call*/
  0,                                          /*tp_str*/
  0,                                          /*tp_getattro*/
  0,                                          /*tp_setattro*/
  0,                                          /*tp_as_buffer*/
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,   /*tp_flags*/
  Cache_doc,                                  /* tp_doc */
  0,                                          /* tp_traverse */
  0,                                          /* tp_clear */
  0,                                          /* tp_richcompare */
  0,                                          /* tp_weaklistoffset */
  0,                                          /* tp_iter */
  0,                                          /* tp_iternext */
  Cache_methods,                              /* tp_methods */
  Cache_members,                              /* tp_members */
  0,                                          /* tp_getset */
  0,                                          /* tp_base */
  0,                                          /* tp_dict */
  0,                                          /* tp_descr_get */
  0,                                          /* tp_descr_set */
  0,                                          /* tp_dictoffset */
  (initproc)Cache_init,                       /* tp_init */
  0,                                          /* tp_alloc */
  Cache_new,                                  /* tp_new */
};


#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC initpersistent_cache(void);
