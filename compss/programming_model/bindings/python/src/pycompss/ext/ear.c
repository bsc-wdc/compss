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
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <unistd.h>
#include <sched.h>
#include <earld.h>


static PyObject * ear_finalize(PyObject *self, PyObject *args) {
    printf("\nFinalizing EAR - Calling destructor\n");
    ear_destructor();
    Py_RETURN_NONE;
}


static PyMethodDef EARMethods[] = {
    {"finalize",        ear_finalize,        METH_NOARGS,  "Finalize EAR library and clean up all its data structures."},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

static struct PyModuleDef earmodule = {
    PyModuleDef_HEAD_INIT,
    "ear",  /* name of module */
    NULL,   /* module documentation, may be NULL */
    -1,     /* size of per-interpreter state of the module, or -1 if the module keeps state in global variables. */
    EARMethods
};

PyMODINIT_FUNC PyInit_ear(void)
{
    return PyModule_Create(&earmodule);
}
