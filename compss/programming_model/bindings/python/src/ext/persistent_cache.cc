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
#include "persistent_cache.h"

static int cached_object_init(cached_object* self,
  PyObject *args, PyObject *kwds) {
    return 0;
}

static void cached_object_dealloc(cached_object* self) {
  Py_DECREF(self->obj);
  Py_XDECREF(self->comparison_function);
}

bool _default_comparison_function(const cached_object& a,  const cached_object& b) {
  if(a.hit_count != b.hit_count) {
    return a.hit_count < b.hit_count;
  }
  if(a.last_hit_time != b.last_hit_time) {
    return a.last_hit_time < b.last_hit_time;
  }
  return a.id < b.id;
}

static PyObject* default_comparison_function(PyObject* self, PyObject* args) {
  PyObject* a;
  PyObject* b;
  if(!Py_BuildValue("OO", &a, &b)) {
    return NULL;
  }
  cached_object* _a = (cached_object*)a;
  cached_object* _b = (cached_object*)b;
  return PyBool_FromLong(_default_comparison_function(*_a, *_b));
}

static PyObject*
cached_object_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
  cached_object* self = (cached_object*)type->tp_alloc(type, 0);
  const char* object_id;

  if(!PyArg_ParseTuple(args, "sOlOl", &object_id, &self->obj, &self->object_size,
  &self->comparison_function, &self->last_hit_time)) {
    return NULL;
  }
  Py_INCREF(self->obj);
  Py_XINCREF(self->comparison_function);
  self->id = std::string(object_id);
  return (PyObject*)self;
}

static PyObject* cached_object_get_id(PyObject* self, PyObject* args) {
  cached_object* _self = (cached_object*)self;
  return PyString_FromStringAndSize(_self->id.c_str(), _self->id.size());
}

static PyObject* cached_object_get_obj(PyObject* self, PyObject* args) {
  cached_object* _self = (cached_object*)self;
  return _self->obj;
}

void _delete_element(Cache* c, cached_object* obj) {
  c->current_size -= obj->object_size;
  c->H->erase(obj->id);
  Py_DECREF(obj->obj);
  Py_XDECREF(obj->comparison_function);
  c->S->erase(*obj);
}

void _delete_first_element(Cache* c) {
  cached_object* first_element = (cached_object*)&*(c->S->begin());
  _delete_element(c, first_element);
}

static void Cache_dealloc(Cache* self) {
  while(!self->S->empty()) _delete_first_element(self);
  delete(self->S);
  delete(self->H);
  Py_XDECREF(self->comparator);
}

static int Cache_init(Cache* self, PyObject* args, PyObject* kwds) {
  return 0;
}

static PyObject* Cache_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
  Cache* self = (Cache*)type->tp_alloc(type, 0);
  self->S = new std::set< cached_object, cached_object_comparator >();
  self->H = new std::map< std::string, cached_object >();
  self->current_time = 0ll;
  self->size_limit = 1024*1024*1024ll;
  static char* kwlist[] = {(char*)"size_limit", (char*)"comparison_function"};
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|lO", kwlist, &self->size_limit,
  &self->comparator)) {
         return NULL;
  }
  Py_XINCREF(self->comparator);
  return (PyObject*)self;
}

static PyObject* Cache_add(PyObject* self, PyObject* args) {
  Cache* _self = (Cache*)self;
  const char* object_id;
  PyObject* obj_to_add;
  long long object_size = -1ll;
  if(!PyArg_ParseTuple(args, "sO|l", &object_id, &obj_to_add, &object_size)) {
    return NULL;
  }
  cached_object to_add = *(new cached_object());
  to_add.id = std::string(object_id);
  if(_self->H->count(to_add.id)) {
    std::string err_msg = "Object with id " + to_add.id + " is\
    already cached!";
    PyErr_SetString(PyExc_KeyError, err_msg.c_str());
    return NULL;
  }
  to_add.obj = obj_to_add;
  if(object_size == -1ll) {
    // rough approximation to the object's size (only invoked when obj size is
    // not given by the user)
    object_size = sizeof(*obj_to_add);
  }
  // Delete the object with least priority until cache is empty or
  // the total used space is less than the limit
  // Note that this operation can delete the most recent object
  // depending on the comparison function
  while(!_self->S->empty() && _self->current_size >= _self->size_limit) {
    _delete_first_element(_self);
  }
  // Does this object fit on cache?
  // If yes, add it and then update the total used cache space
  if(_self->current_size + object_size <= _self->size_limit) {
    to_add.object_size = object_size;
    to_add.hit_count = 1;
    to_add.last_hit_time = _self->current_time;
    to_add.comparison_function = _self->comparator;
    Py_XINCREF(_self->comparator);
    Py_INCREF(to_add.obj);
    _self->current_size += to_add.object_size;
    std::map< std::string, cached_object >& map_ref = *_self->H;
    map_ref[to_add.id] = to_add;
    _self->S->insert(map_ref[to_add.id]);
  }
  ++_self->current_time;
  Py_RETURN_NONE;
}

bool _has_object(Cache* self, std::string id) {
  return self->H->count(id);
}

static PyObject* Cache_has_object(PyObject* self, PyObject* args) {
  Cache* _self = (Cache*)self;
  const char* to_query;
  if(!PyArg_ParseTuple(args, "s", &to_query)) {
    return NULL;
  }
  return PyBool_FromLong(_has_object(_self, std::string(to_query)));
}

static PyObject* Cache_hit(PyObject* self, PyObject* args) {
  Cache* _self = (Cache*) self;
  const char* to_query;
  if(!PyArg_ParseTuple(args, "s", &to_query)) {
    return NULL;
  }
  std::string object_id(to_query);
  if(!_has_object(_self, object_id)) {
    std::string err_msg = "Cache does not contain object with id " +
    object_id;
    PyErr_SetString(PyExc_KeyError, err_msg.c_str());
    return NULL;
  }
  std::map< std::string, cached_object >& map_ref = *_self->H;
  cached_object& cached_friend = map_ref[object_id];
  // As a remark: an std::set cannot know when some of its objects changes in
  // such a way that its ordering also does, so it must be removed and then
  // re-added in order to ensure that it is in a proper place inside the set
  _self->S->erase(cached_friend);
  ++cached_friend.hit_count;
  cached_friend.last_hit_time = _self->current_time;
  _self->S->insert(cached_friend);
  ++_self->current_time;
  Py_RETURN_NONE;
}

static PyObject* Cache_get(PyObject* self, PyObject* args) {
  Cache* _self = (Cache*)self;
  const char* to_query;
  if(!PyArg_ParseTuple(args, "s", &to_query)) {
    return NULL;
  }
  std::string id(to_query);
  if(!_has_object(_self, id)) {
    std::string err_msg = "Cache does not contain object with id " +
    id;
    PyErr_SetString(PyExc_KeyError, err_msg.c_str());
    return NULL;
  }
  std::map< std::string, cached_object >& map_ref = *_self->H;
  PyObject* ret = map_ref[id].obj;
  Py_INCREF(ret);
  return ret;
}

static PyObject* Cache_delete(PyObject* self, PyObject* args) {
  Cache* _self = (Cache*) self;
  const char* to_query;
  if(!PyArg_ParseTuple(args, "s", &to_query)) {
    return NULL;
  }
  std::string id(to_query);
  if(!_has_object(_self, id)) {
    std::string err_msg = "Cache does not contain object with id " +
    id;
    PyErr_SetString(PyExc_KeyError, err_msg.c_str());
    return NULL;
  }
  std::map< std::string, cached_object >& map_ref = *_self->H;
  cached_object& cached_friend = map_ref[id];
  _delete_element(_self, &cached_friend);
  Py_RETURN_NONE;
}

static PyObject* Cache_get_last(PyObject* self, PyObject* args) {
  Cache* _self = (Cache*)self;
  cached_object ret = *_self->S->begin();
  Py_INCREF(ret.obj);
  return ret.obj;
}

static PyObject* Cache_is_empty(PyObject* self, PyObject* args) {
  Cache* _self = (Cache*)self;
  return PyBool_FromLong(_self->S->empty());
}

static PyObject* Cache_set_object(PyObject* self, PyObject* args) {
  Cache* _self = (Cache*)self;
  const char* to_query;
  PyObject* new_obj;
  if(!PyArg_ParseTuple(args, "sO", &to_query, &new_obj)) {
    return NULL;
  }
  std::string id(to_query);
  std::map< std::string, cached_object >& map_ref = *_self->H;
  cached_object& cached_friend = map_ref[id];
  _self->S->erase(cached_friend);
  Py_DECREF(cached_friend.obj);
  cached_friend.obj = new_obj;
  Py_INCREF(cached_friend.obj);
  _self->S->insert(cached_friend);
  Py_RETURN_NONE;
}

PyMODINIT_FUNC initpersistent_cache(void) {
    PyObject* m;
    m = Py_InitModule("persistent_cache", module_methods);

    if (PyType_Ready(&cached_objectType) < 0)
        return;
    Py_INCREF(&cached_objectType);
    PyModule_AddObject(m, "cached_object", (PyObject*)&cached_objectType);

    if (PyType_Ready(&CacheType) < 0)
        return;
    Py_INCREF(&CacheType);
    PyModule_AddObject(m, "Cache", (PyObject*)&CacheType);
}
