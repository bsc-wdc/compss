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
  PyObject* cpu_list;
  if(!PyArg_ParseTuple(args, "O|l", &cpu_list, &pid)) {
    return NULL;
  }
  cpu_set_t to_assign;
  CPU_ZERO(&to_assign);
  int num_params = PyList_Size(cpu_list);
  for(int i=0; i<num_params; ++i) {
    int cpu_id = PyInt_AsLong(PyList_GetItem(cpu_list, i));
    CPU_SET(cpu_id, &to_assign);
  }
  if(sched_setaffinity(pid, sizeof(cpu_set_t), &to_assign) < 0) {
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
  cpu_set_t set_cpus;
  if(sched_getaffinity(pid, sizeof(cpu_set_t), &set_cpus) < 0) {
    PyErr_SetString(PyExc_RuntimeError, "Error during sched_getaffinity call!");
  }
  std::vector<int> ret_val;
  for(int i=0; i<__CPU_SETSIZE; ++i) {
    if(CPU_ISSET(i, &set_cpus)) ret_val.push_back(i);
  }
  PyObject* py_ret = PyList_New(int(ret_val.size()));
  for(int i=0; i<int(ret_val.size()); ++i) {
    PyList_SetItem(py_ret, i, Py_BuildValue("i", ret_val[i]));
  }
  return py_ret;
}


PyMODINIT_FUNC initthread_affinity(void) {
    PyObject* m = Py_InitModule("thread_affinity", module_methods);
}
