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
#include "thread_affinity.h"

static PyObject* pysched_setaffinity(PyObject* self, PyObject* args) {
  long long pid = 0LL;
  long long mask = 0LL;
  if(!PyArg_ParseTuple(args, "ll", &pid, &mask)) {
    return NULL;
  }
  if( pid == 0LL ) pid = getpid();
  if(sched_setaffinity(0, sizeof(long long), (cpu_set_t*)&mask) < 0) {
    PyErr_SetString(PyExc_RuntimeError, "Error during sched_setaffinity call!");
  }
  Py_RETURN_NONE;
}


static PyObject* pysched_getaffinity(PyObject* self, PyObject* args) {
  long long pid = 0LL;
  if(!PyArg_ParseTuple(args, "|l", &pid)) {
    return NULL;
  }
  if( pid == 0LL ) pid = getpid();
  long long ret_val;
  if(sched_getaffinity(pid, sizeof(long long), (cpu_set_t*)&ret_val) < 0) {
    PyErr_SetString(PyExc_RuntimeError, "Error during sched_getaffinity call!");
  }
  return Py_BuildValue("l", ret_val);
}


PyMODINIT_FUNC initthread_affinity(void) {
    PyObject* m;
    m = Py_InitModule("thread_affinity", module_methods);
}
