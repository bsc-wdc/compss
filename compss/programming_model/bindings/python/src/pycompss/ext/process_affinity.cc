/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
#include "process_affinity.h"

struct module_state {
    PyObject *error;
};

#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))
#define PyInt_AsLong PyLong_AsLong

static PyObject * error_out(PyObject *m) {
    struct module_state *st = GETSTATE(m);
    PyErr_SetString(st->error, "Thread affinity extension module: Something bad happened");
    return NULL;
}

//////////////////////////////////////////////////////////////

cpu_set_t default_affinity;

static PyObject* pysched_setaffinity(PyObject* self, PyObject* args) {
    long long pid = 0ll;
    PyObject* cpu_list;
    if(!PyArg_ParseTuple(args, "O|l", &cpu_list, &pid)) {
        return NULL;
    }
    cpu_set_t to_assign;
    CPU_ZERO(&to_assign);
    int num_params = PyList_Size(cpu_list);
    for(int i = 0; i < num_params; ++i) {
        int cpu_id = PyInt_AsLong(PyList_GetItem(cpu_list, i));
        CPU_SET(cpu_id, &to_assign);
    }
    if(sched_setaffinity(pid, sizeof(cpu_set_t), &to_assign) < 0) {
        if(sched_setaffinity(pid, sizeof(cpu_set_t), &default_affinity) < 0) {
            PyErr_SetString(PyExc_RuntimeError, "Cannot set default affinity (!)");
        } else {
            PyErr_SetString(PyExc_RuntimeError, "setaffinity failed, setting default affinity");
        }
    }
    Py_RETURN_NONE;
}

static PyObject* pysched_getaffinity(PyObject* self, PyObject* args) {
    long long pid = 0ll;
    if(!PyArg_ParseTuple(args, "|l", &pid)) {
        return NULL;
    }
    if(pid == 0ll) pid = getpid();
    cpu_set_t set_cpus;
    if(sched_getaffinity(pid, sizeof(cpu_set_t), &set_cpus) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Error during sched_getaffinity call!");
        Py_RETURN_NONE;
    }
    std::vector< int > ret_val;
    for(int i = 0; i < __CPU_SETSIZE; ++i) {
        if(CPU_ISSET(i, &set_cpus)) ret_val.push_back(i);
    }
    PyObject* py_ret = PyList_New(int(ret_val.size()));
    for(int i = 0; i < int(ret_val.size()); ++i) {
        PyList_SetItem(py_ret, i, Py_BuildValue("i", ret_val[i]));
    }
    return py_ret;
}

//////////////////////////////////////////////////////////////


static PyMethodDef ThreadAffinityMethods[] = {
    { "error_out", (PyCFunction)error_out, METH_NOARGS, NULL},
    { "setaffinity", pysched_setaffinity, METH_VARARGS, "Args: (mask, [pid=0]) -> set the affinity for the thread with given pid to given mask. If pid equals zero, then the current thread's affinity will be changed." },
    { "getaffinity", pysched_getaffinity, METH_VARARGS, "Args: ([pid=0]) -> returns the affinity for the thread with given pid. If not specified, returns the affinity for the current thread."},
    { NULL, NULL } /* sentinel */
};


static int process_affinity_traverse(PyObject *m, visitproc visit, void *arg) {
    Py_VISIT(GETSTATE(m)->error);
    return 0;
}
static int process_affinity_clear(PyObject *m) {
    Py_CLEAR(GETSTATE(m)->error);
    return 0;
}
static struct PyModuleDef cModThAPy = {
    PyModuleDef_HEAD_INIT,
    "process_affinity",             /* name of module */
    NULL,                          /* module documentation, may be NULL */
    sizeof(struct module_state),   /* size of per-interpreter state of the module, or -1 if the module keeps state in global variables. */
    ThreadAffinityMethods,
    NULL,
    process_affinity_traverse,
    process_affinity_clear,
    NULL
};
#define INITERROR return NULL
PyMODINIT_FUNC PyInit_process_affinity(void) {
    PyObject *module = PyModule_Create(&cModThAPy);

    if (module == NULL)
        INITERROR;
    struct module_state *st = GETSTATE(module);

    st->error = PyErr_NewException("process_affinity.Error", NULL, NULL);
    if (st->error == NULL) {
        Py_DECREF(module);
        INITERROR;
    }

    sched_getaffinity(0, sizeof(cpu_set_t), &default_affinity);

    return module;
}
