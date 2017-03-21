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
  This is a System-V Shared Memory Segment API for Python.
  It allows to have a file-like object that points to a shared memory segment.
  Also, it allows to read and write, literally, objects to the memory segment.
  Literally means that the internal object representation is "pasted" to the
  segment, not only the object's contents. This method allows us to pass
  Python Strings between processes saving an additional copy. Also, if we put
  on this string a serialized representation of an object, we can pass any
  Python object without any kind of Disk I/O and with no need to explicitly
  copy the contents of a Python String.

  It must be noted that Python Strings are internally represented that way:

  typedef struct {
    PyObject_VAR_HEAD
    long ob_shash;
    int ob_sstate;
    char ob_sval[1];
   } PyStringObject;

  If a string of 32 characters is instanced, a
  malloc(sizeof(PyStringObject) + 32) will be cast. Since the type of the
  contents is a char array (and not a pointer), its address cannot be changed,
  leading to the need of an explicit copy of the contents. So, if we want to
  share an string between processes we must send it from the original process
  to its destiny, and then COPY the whole string in a new PyObject.

  If we write instead the PyObject onto the segment and we return it then we
  will have no need to allocate extra memory and perform a whole copy of the
  contents. Please, see comments at read_pyobject and write_pyobject for some
  recommendations and observations about these functionalities

  @author: srodrig1 (sergio.rodriguez at bsc.es)
*/
#pragma once
#include <Python.h>
#include <structmember.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <unistd.h>
#include <iostream>
#include <cassert>
#include <cstdlib>
#include <ctime>
// In System-V SHM syntax, SHM segments have a numerical key that, if shared,
// must be unique. A cheap way to ensure this is to try random keys
// and throw an error if we had no success after some attempts.
static const int MAX_SHMGET_ATTEMPTS = 100;
// Additional byte amount to reserve for our SHM
// Note that a PyStringObject size will always be greater that the length of
// the string that it contains
static const unsigned long long SPARE_OBJECT_BYTES = 128ll;

/*
  This struct contains all the necessary information in order to work
  with a Shared Memory Segment
*/
typedef struct {
  PyObject_HEAD
  // it is preferrable to use unsigned instead of key_t for its
  // generality
  unsigned key;
  int id;
  unsigned long long byte_amount;
  int flags;
  char* base_address;
  unsigned long long offset;
  int last_z;
} shm_manager;

static char* member_names[] = {
  (char*)"key",
  (char*)"id",
  (char*)"bytes",
  (char*)"flags",
  (char*)"base_address",
  (char*)"offset"
};

static char* member_descriptions[] = {
  (char*)"SHM key",
  (char*)"SHM identifier",
  (char*)"Byte length of the SHM",
  (char*)"Flag-set for this manager",
  (char*)"Base memory address of the mapped segment",
  (char*)"Current offset (aka pointer)"
};

static PyMemberDef shm_manager_members[] = {
  {member_names[0], T_INT, offsetof(shm_manager, key), 0,
  member_descriptions[0]},
  {member_names[1], T_INT, offsetof(shm_manager, id), 0,
  member_descriptions[1]},
  {member_names[2], T_LONG, offsetof(shm_manager, byte_amount), 0,
  member_descriptions[2]},
  {member_names[3], T_INT, offsetof(shm_manager, flags), 0,
  member_descriptions[3]},
  {member_names[4], T_LONG, offsetof(shm_manager, base_address), 0,
  member_descriptions[4]},
  {member_names[5], T_LONG, offsetof(shm_manager, offset), 0,
  member_descriptions[5]},
  {NULL}  /* Sentinel */
};

/*
  Empty, returns zero at the moment.
*/
static int shm_manager_init(shm_manager *self, PyObject *args, PyObject *kwds);

/*
  Equivalent to the destructor method.
  It marks the segment for deletion and decreases its natcch by one.
  When a segment marked for deletion reaches a natcch of zero the OS deletes
  it.
  If the program ends in an unexpected way that leaves no room for Python to
  finish the execution, it is possible that some segments will outlive the
  caller process. In this case, they must be deleted by hand via ipcs and ipcrm.
*/
static void shm_manager_dealloc(shm_manager* self);

/*
  Expected parameters (default value):
  - key (1337)
  - bytes (100)
  - flags(0600 | IPC_CREAT | IPC_EXCL)
  Gets (or creates) a shared memory segment with the specified key.
  If the specified key is not available, random keys will be attempted for
  a limited amount of times.
  It also attaches the SHM to the main process. Attaching means mapping the
  shm addresses to the process memory.

  If write_pyobject or read_pyobject are going to be used, it is MANDATORY
  to set permissions for any process using the SHM to, at least 0600

  Returns a shm_manager object if there was no problem.
  If some problem is found, a ShmException will be thrown.
*/
static PyObject* shm_manager_new(PyTypeObject* type,
                                 PyObject* args,
                                 PyObject* kwds);

/*
  Memcpys the, literally, PyObject passed as argument to the SHM
*/
static PyObject* shm_manager_write_pyobject(PyObject* self, PyObject* args);

/*
  Expected parameters (default value):
  - An shm object (implicit as self/this)
  - A bytearray
  Writes to the SHM the contents of the bytearray
*/
static PyObject* shm_manager_write(PyObject* self, PyObject* args);

/*
  Returns a PyObject from the SHM.
  Since the process that creates the PyObject is not necessarily the
  same that reads this object, its ob_type must be changed in order to
  ensure that its type is correctly recovered. Otherwise, this object
  may be interpreted as None or, even worse, a segfault can occur.
  Also, it must be noted that if processes are not related and pyobject reads
  are not controlled then some faulty situations as having a wrong ob_type
  pointer can happen.
*/
static PyObject* shm_manager_read_pyobject(PyObject* self, PyObject* args);

/*
  Reads N bytes from the shm. If no amount is specified, then the whole
  remaining unread segment will be returned.
*/
static PyObject* shm_manager_read(PyObject* self, PyObject* args);

/*
  Expected parameters (default value):
  - An shm object (implicit as self/this)
  Deletes the SHM (both as Linux SYSV SHM Segment and python object)
*/
static PyObject* shm_manager_delete(PyObject* self, PyObject* args);

/*
  Reads a line from the shm and returns it (\n char included)
*/
static PyObject* shm_manager_readline(PyObject* self, PyObject* args);

/*
  Sets the pointer to the specified position
*/
static PyObject* shm_manager_seek(PyObject* self, PyObject* args);

/*
  Returns the pointer current position
*/
static PyObject* shm_manager_tell(PyObject* self, PyObject* args);

/*
    Module methods
*/
static PyMethodDef module_methods[] = {
    {NULL, NULL, 0, NULL} /* Sentinel */
};

/*
    Class and instance methods
*/
static PyMethodDef shm_manager_methods[] = {
    {"write_object", shm_manager_write_pyobject, METH_VARARGS,
    "Writes the, literally, content of an Object"},
    {"write", shm_manager_write, METH_VARARGS,
    "Writes N bytes to the shared memory segment"},
    {"read_object", shm_manager_read_pyobject, METH_VARARGS,
    "Reads the content of the SHM and \"creates\" a PObj with it"},
    {"read", shm_manager_read, METH_VARARGS,
    "Reads N bytes from the shared memory segment"},
    {"readline", shm_manager_readline, METH_VARARGS,
    "Reads a whole line from the shared memory segment"},
    {"seek", shm_manager_seek, METH_VARARGS,
    "Sets the pointer position"},
    {"tell", shm_manager_tell, METH_VARARGS,
    "Returns the current pointer poisition"},
    {"delete", shm_manager_delete, METH_VARARGS,
    "Deletes a Shared Memory Segment"},
    {NULL}  /* Sentinel */
};

static char* shm_manager_doc = (char*)
"\nA System-V SHM interface for Python.\n\
Offers functionalities for shared memory segments\n\
creation and handling, file-like methods (i.e: read, readline, write)\n\
and the possibility to transfer PyStringObjects, saving time from\n\
memory copies.\n\n\
EXAMPLE:\n\
Let's suppose that we have two processes and that we want to share an String\n\
from A to B. Then, we can do the following thing:\n\
from shm_manager import shm_manager as SHM\n\n\
# Source code at A\n\
my_string = \"Hello World!\"\n\
manager = SHM(-1, len(my_string))\n\
manager.write_object(my_string)\n\
*send manager.key and manager.bytes via your favorite IPC method (e.g pipes)*\n\
# Source code at B\n\
(key, byte_len) = *key and bytes received from a, for example, pipe*\n\
manager = SHM(key, byte_len, 0600)\n\
print manager.read_object() # Should print \"Hello World!\"\n\n\
";

/*
    Python type definition.
    Look at initshm_manager's implementation to see how
    this struct is used.
*/
static PyTypeObject shm_managerType = {
    PyObject_HEAD_INIT(NULL)
    0,                                          /*ob_size*/
    "shm_manager.shm_manager",                  /*tp_name*/
    sizeof(shm_manager),                        /*tp_basicsize*/
    0,                                          /*tp_itemsize*/
    (destructor)shm_manager_dealloc,            /*tp_dealloc*/
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
    shm_manager_doc,                            /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    shm_manager_methods,                        /* tp_methods */
    shm_manager_members,                        /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc)shm_manager_init,                 /* tp_init */
    0,                                          /* tp_alloc */
    shm_manager_new,                            /* tp_new */
};


#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC initshm_manager(void);
