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
#include "shm_manager.h"

static int shm_manager_init(shm_manager *self, PyObject *args, PyObject *kwds) {
  return 0;
}

static void shm_manager_dealloc(shm_manager* self) {
  // python (or the user) wants to delete this object, let's deattach the
  // segment from the process (which at the same
  // time decreases the segment's natcch value)
  shmdt(self->base_address);
  // did this deattachment set our segment natcch to zero? then let's also
  // delete it from memory
  shmid_ds ds;
  shmctl(self->id, IPC_STAT, &ds);
  if(ds.shm_nattch == 0) {
    shmctl(self->id, IPC_RMID, NULL);
  }
}

// std::to_string belongs to c++11 standard, so let's avoid to use it
std::string _to_string(unsigned long long x) {
  std::ostringstream s;
  s << x;
  return s.str();
}

static PyObject* shm_manager_new(PyTypeObject* type,
                                 PyObject* args,
                                 PyObject* kwds) {
  shm_manager* self;
  self = (shm_manager*)type->tp_alloc(type, 0);
  // these are the default arg values, note that the pipe specifier is set at
  // PyArg_ParseTuple (all values after pipe are optional but, if given,
  // format must be respected)
  self->key = 1337;
  self->byte_amount = 0;
  // IPC_CREAT | IPC_EXCL means "i want to CREATe an SHM and i want to ensure
  // that my key is EXCLusive"
  self->flags = 0600 | IPC_CREAT | IPC_EXCL | SHM_NORESERVE | SHM_LOCKED;
  self->offset = 0;
  // try avoid as many collisions as possible
  if(FIRST_TIME) {
    std::srand(time(NULL) + getpid() + std::clock());
    FIRST_TIME = false;
  }

  if(!PyArg_ParseTuple(
        //read |lli as optional args (but in this order): two longs and an int
  args, "|lli", &self->key, &self->byte_amount, &self->flags)) {
    PyErr_SetString(PyExc_TypeError, "Wrong argspec");
    return NULL;
  }

  // add this offset only when we are creating the object
  // use shm.bytes to create new managers
  if(self->flags&IPC_CREAT) {
    self->byte_amount += SPARE_OBJECT_BYTES;
  }
  // if we are creating a segment then the key does not really matter, so we
  // can try a few random keys before deciding that there is an error
  // if we are getting a segment then the key must be the one that is specified
  int attempt_count = self->flags&IPC_CREAT ?
                        0 : MAX_SHMGET_ATTEMPTS-1;
  while(
  attempt_count++ < MAX_SHMGET_ATTEMPTS &&
  (self->id = shmget(self->key, self->byte_amount, self->flags)) == -1) {
    do {
      self->key = (key_t)std::rand();
    } while(!self->key);
  }
  // we tried to create or get an shm with no success? throw an error
  if(self->id == -1) {
    if(self->flags&IPC_CREAT) {
      PyErr_SetString(PyExc_RuntimeError, "Could not create SHM");
    }
    else {
      std::string _err_msg = "Could not get SHM with key "
      + _to_string(self->key) + " (are you sure that exists?)";
      PyErr_SetString(PyExc_RuntimeError, _err_msg.c_str());
    }
    return NULL;
  }                                               // or attach flags !!!
  if( (self->base_address = (char*)shmat(self->id, NULL, 0)) == (char*) -1) {
    PyErr_SetString(PyExc_RuntimeError, "Could not attach the SHM to the\
    process memory");
    return NULL;
  }
  return (PyObject*) self;
}

static PyObject* shm_manager_write_pyobject(PyObject* self, PyObject* args) {
  shm_manager* _self = (shm_manager*)self;
  PyObject* content_object;
  if(!PyArg_ParseTuple(args, "O", &content_object)) {
    PyErr_SetString(PyExc_TypeError, "Wrong argspec");
    return NULL;
  }
  // since the memory region of the PyObject is totally controlled by the SHM
  // manager, it is preferrable to avoid Python calling _Py_dealloc
  // a simple way to do it is to always have a "ghost" reference and have all
  // reads increase the refcount by one, ensuring that we never will
  // dealloc the object
  // so, even if ob_refcnt is not null at copy time, this value will not be
  // reset because it is convenient to have it at > 0
  memcpy(_self->base_address, content_object, _self->byte_amount);
  Py_RETURN_NONE;
}

static PyObject* shm_manager_write(PyObject* self, PyObject* args) {
  shm_manager* _self = (shm_manager*)self;
  const char* to_write;
  int byte_amount;
  if(!PyArg_ParseTuple(args, "s#", &to_write, &byte_amount)) {
    PyErr_SetString(PyExc_TypeError, "Wrong argspec");
    return NULL;
  }
  // are we going to write in a legal location?
  if((size_t)byte_amount + _self->offset > _self->byte_amount) {
    PyErr_SetString(PyExc_IndexError, "OOB write operation detected");
    return NULL;
  }
  // this is the actual write to the manager
  memcpy(_self->base_address + _self->offset, to_write, byte_amount);
  _self->offset += byte_amount;
  Py_RETURN_NONE;
}

static PyObject* shm_manager_read(PyObject* self, PyObject* args) {
  shm_manager* _self = (shm_manager*)self;
  unsigned long long bytes_to_read = _self->byte_amount - _self->offset;
  if(!PyArg_ParseTuple(args, "|l", &bytes_to_read)) {
    PyErr_SetString(PyExc_TypeError, "Wrong argspec");
    return NULL;
  }
  _self->offset += bytes_to_read;
  return PyString_FromStringAndSize(
    _self->base_address + _self->offset - bytes_to_read,
    bytes_to_read);
}

static PyObject* shm_manager_read_pyobject(PyObject* self, PyObject* args) {
  shm_manager* _self = (shm_manager*)self;
  PyObject* ret = (PyObject*)_self->base_address;
  // note that this address (&PyString_Type) is only valid for the calling
  // process or processes that are either a fork or ancestors of the current one
  // if its wanted to share objects between non-related processes then some
  // lock system must be implemented in order to avoid potential segfaults
  // in the case of PyCOMPSs persistent worker this issue is not important
  // because the workers are direct children of the persistent worker
  // (multiprocessing.Process forks the caller, avoiding the need to launch a
  // Python interpreter from scratch)
  ret->ob_type = &PyString_Type;
  Py_INCREF(ret);
  return (PyObject*)ret;
}

static PyObject* shm_manager_readline(PyObject* self, PyObject* args) {
  shm_manager* _self = (shm_manager*)self;
  unsigned long long old_offset = _self->offset;
  bool done = false;
  while(!done && _self->offset < _self->byte_amount) {
      done |= _self->base_address[_self->offset] == '\n';
      ++_self->offset;
  }
  PyObject* ret = PyString_FromStringAndSize(_self->base_address + old_offset,
  _self->offset - old_offset);
  return ret;
}

static PyObject* shm_manager_seek(PyObject* self, PyObject* args) {
  shm_manager* _self = (shm_manager*)self;
  unsigned long long new_offset = 0;
  if(!PyArg_ParseTuple(args, "|l", &new_offset)) {
    PyErr_SetString(PyExc_TypeError, "Wrong argspec");
    return NULL;
  }
  if(new_offset >= _self->byte_amount) {
    PyErr_SetString(PyExc_IndexError, "Attempted to set SHM pointer to an OOB\
    position");
  }
  _self->offset = new_offset;
  Py_RETURN_NONE;
}

static PyObject* shm_manager_tell(PyObject* self, PyObject* args) {
  shm_manager* _self = (shm_manager*)self;
  return Py_BuildValue("l", _self->offset);
}

static PyObject* shm_manager_delete(PyObject* self, PyObject* args) {
  Py_RETURN_NONE;
}

PyMODINIT_FUNC initshm_manager(void) {
    PyObject* m;
    if (PyType_Ready(&shm_managerType) < 0)
        return;
    m = Py_InitModule("shm_manager", module_methods);
    Py_INCREF(&shm_managerType);
    PyModule_AddObject(m, "shm_manager", (PyObject *)&shm_managerType);
}
