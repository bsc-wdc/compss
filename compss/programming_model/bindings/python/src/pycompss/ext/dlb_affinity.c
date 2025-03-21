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
#include <dlb.h>
#include <dlb_drom.h>

static PyObject * dlb_init(PyObject *self, PyObject *args) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    int err = DLB_Init(0, &mask, 0);
    //int err = DLB_Init(0, NULL, "--drom --lewi --ompt");
    if (err == DLB_SUCCESS) printf("\nInitializing DLB DROM\n");
    else                    printf("\nError initializing DLB DROM: %d\n", err);
    Py_RETURN_NONE;
}

static PyObject * dlb_finalize(PyObject *self, PyObject *args) {
    printf("\nFinalizing DLB DROM\n");
    DLB_Finalize();
    Py_RETURN_NONE;
}

static PyObject * dlb_pollDROM_Update(PyObject *self, PyObject *args) {
    DLB_PollDROM_Update();
    Py_RETURN_NONE;
}

static PyObject * dlb_attach(PyObject *self, PyObject *args) {
    int err = DLB_DROM_Attach();
    Py_RETURN_NONE;
}

static PyObject * dlb_detach(PyObject *self, PyObject *args) {
    int err = DLB_DROM_Detach();
    if (err == DLB_SUCCESS) printf("\ndetached\n");
    else                    printf("\nerror ataching: %d\n", err);
    Py_RETURN_NONE;
}


/* Lend */

static PyObject * dlb_lend(PyObject *self, PyObject *args) {
    int err = DLB_Lend();
    if (err == DLB_SUCCESS) printf("\nlent\n");
    else                    printf("\nerror lending: %d\n", err);
    Py_RETURN_NONE;
}

static PyObject * dlb_lendCpu(PyObject *self, PyObject *args) {
    int cpuid = 0;
    if (!PyArg_ParseTuple(args, "i", &cpuid))
        return NULL;
    int err = DLB_LendCpu(cpuid);
    if (err == DLB_SUCCESS) printf("\nlent\n");
    else                    printf("\nerror lending: %d\n", err);
    Py_RETURN_NONE;
}

static PyObject * dlb_lendCpus(PyObject *self, PyObject *args) {
    int ncpus = 0;
    if (!PyArg_ParseTuple(args, "i", &ncpus))
        return NULL;
    int err = DLB_LendCpus(ncpus);
    if (err == DLB_SUCCESS) printf("\nlent\n");
    else                    printf("\nerror lending: %d\n", err);
    Py_RETURN_NONE;
}

static PyObject * dlb_lendCpuMask(PyObject *self, PyObject *args) {
    int i;
    PyObject* cpu_list;
    if (!PyArg_ParseTuple(args, "O", &cpu_list))
        return NULL;

    cpu_set_t mask;
    CPU_ZERO(&mask);
    int num_params = PyList_Size(cpu_list);
    for (i = 0; i < num_params; ++i) {
        int cpu_id = PyLong_AsLong(PyList_GetItem(cpu_list, i));
        CPU_SET(cpu_id, &mask);
    }
    int err = DLB_LendCpuMask(&mask);
    if (err == DLB_SUCCESS) printf("\nlent\n");
    else                    printf("\nerror lending: %d\n", err);
    Py_RETURN_NONE;
}


/* Acquire */

static PyObject * dlb_acquireCpu(PyObject *self, PyObject *args) {
    int cpuid = 0;
    if (!PyArg_ParseTuple(args, "i", &cpuid))
        return NULL;
    int err = DLB_AcquireCpu(cpuid);
    if (err == DLB_SUCCESS) printf("\nacquired\n");
    else                    printf("\nerror acquiring: %d\n", err);
    Py_RETURN_NONE;
}

static PyObject * dlb_acquireCpus(PyObject *self, PyObject *args) {
    int ncpus = 0;
    if (!PyArg_ParseTuple(args, "i", &ncpus))
        return NULL;
    int err = DLB_AcquireCpus(ncpus);
    if (err == DLB_SUCCESS) printf("\nacquired\n");
    else                    printf("\nerror acquiring: %d\n", err);
    Py_RETURN_NONE;
}

static PyObject * dlb_acquireCpuMask(PyObject *self, PyObject *args) {
    int i;
    PyObject* cpu_list;
    if (!PyArg_ParseTuple(args, "O", &cpu_list))
        return NULL;

    cpu_set_t mask;
    CPU_ZERO(&mask);
    int num_params = PyList_Size(cpu_list);
    for (i = 0; i < num_params; ++i) {
        int cpu_id = PyLong_AsLong(PyList_GetItem(cpu_list, i));
        CPU_SET(cpu_id, &mask);
    }
    int err = DLB_AcquireCpuMask(&mask);
    if (err == DLB_SUCCESS) printf("\nacquired\n");
    else                    printf("\nerror acquiring: %d\n", err);
    Py_RETURN_NONE;
}


/* Borrow */

static PyObject * dlb_borrow(PyObject *self, PyObject *args) {
    int err = DLB_Borrow();
    if (err == DLB_SUCCESS) printf("\nborrowed\n");
    else                    printf("\nerror borrowing: %d\n", err);
    Py_RETURN_NONE;
}

static PyObject * dlb_borrowCpu(PyObject *self, PyObject *args) {
    int cpuid = 0;
    if (!PyArg_ParseTuple(args, "i", &cpuid))
        return NULL;
    int err = DLB_BorrowCpu(cpuid);
    if (err == DLB_SUCCESS) printf("\nborrowed\n");
    else                    printf("\nerror borrowing: %d\n", err);
    Py_RETURN_NONE;
}

static PyObject * dlb_borrowCpus(PyObject *self, PyObject *args) {
    int ncpus = 0;
    if (!PyArg_ParseTuple(args, "i", &ncpus))
        return NULL;
    int err = DLB_BorrowCpus(ncpus);
    if (err == DLB_SUCCESS) printf("\nborrowed\n");
    else                    printf("\nerror borrowing: %d\n", err);
    Py_RETURN_NONE;
}

static PyObject * dlb_borrowCpuMask(PyObject *self, PyObject *args) {
    int i;
    PyObject* cpu_list;
    if (!PyArg_ParseTuple(args, "O", &cpu_list))
        return NULL;

    cpu_set_t mask;
    CPU_ZERO(&mask);
    int num_params = PyList_Size(cpu_list);
    for (i = 0; i < num_params; ++i) {
        int cpu_id = PyLong_AsLong(PyList_GetItem(cpu_list, i));
        CPU_SET(cpu_id, &mask);
    }
    int err = DLB_BorrowCpuMask(&mask);
    if (err == DLB_SUCCESS) printf("\nborrowed\n");
    else                    printf("\nerror borrowing: %d\n", err);
    Py_RETURN_NONE;
}


/* Get_Affinity */

static PyObject * dlb_getaffinity(PyObject *self, PyObject *args) {
    int i, ii;
    long long pid = 0ll;

    if (!PyArg_ParseTuple(args, "|l", &pid))
        return NULL;

    if (pid == 0ll) pid = getpid();
    cpu_set_t mask;
    CPU_ZERO(&mask);
    int err = DLB_DROM_GetProcessMask(pid, &mask, 0);
    if (err == DLB_SUCCESS)
        printf("\n(%d) getmask ok\n", pid);//, __CPU_SETSIZE);
    else
        printf("\n(%d) getmask fail: %d\n", pid, err);

    int size = 0;
    for (i = 0; i < __CPU_SETSIZE; ++i)
        if (CPU_ISSET(i, &mask)) size++;
    
    PyObject* py_ret = PyList_New(size);
    for (i = 0, ii = 0; i < __CPU_SETSIZE; ++i)
        if (CPU_ISSET(i, &mask))
            PyList_SetItem(py_ret, ii++, Py_BuildValue("i", i));

    return py_ret;
}


/* Set_Affinity */

static PyObject * dlb_setaffinity(PyObject *self, PyObject *args) {
    int i;
    long long pid = 0ll;
    PyObject* cpu_list;

    if (!PyArg_ParseTuple(args, "O|l", &cpu_list, &pid))
        return NULL;

    cpu_set_t mask;
    CPU_ZERO(&mask);
    int num_params = PyList_Size(cpu_list);
    for (i = 0; i < num_params; ++i) {
        int cpu_id = PyLong_AsLong(PyList_GetItem(cpu_list, i));
        CPU_SET(cpu_id, &mask);
    }
    int err = DLB_DROM_SetProcessMask(pid, &mask, 0);
    if (err == DLB_SUCCESS)
        printf("\n(%d) setmask ok\n", pid);
    else
        printf("\n(%d) setmask fail: %d\n", pid, err);

    Py_RETURN_NONE;
}

static PyMethodDef DlbAffinityMethods[] = {

    {"init",            dlb_init,            METH_NOARGS,  "Initialize DLB library and all its internal data structures."},

    {"finalize",        dlb_finalize,        METH_NOARGS,  "Finalize DLB library and clean up all its data structures."},

    {"pollDROM_Update", dlb_pollDROM_Update, METH_NOARGS,  "Poll DROM module to check if the process needs to adapt to a new mask or number of CPUs."},

    {"attach",          dlb_attach,          METH_NOARGS,  "Attach process to DLB as third party."},

    {"detach",          dlb_detach,          METH_NOARGS,  "Detach process from DLB."},

    {"lend",            dlb_lend,            METH_NOARGS,  "Lend all the current CPUs."},

    {"lendCpu",         dlb_lendCpu,         METH_VARARGS, "Args: (cpuid) -> Lend a specific CPU."},

    {"lendCpus",        dlb_lendCpus,        METH_VARARGS, "Args: (ncpus) -> Lend a specific amount of CPUs, only useful for systems that do not work with cpu masks."},

    {"lendCpuMask",     dlb_lendCpuMask,     METH_VARARGS, "Args: (mask) -> Lend a set of CPUs."},

    {"acquireCpu",      dlb_acquireCpu,      METH_VARARGS, "Args: (cpuid) -> Acquire a specific CPU."},

    {"acquireCpus",     dlb_acquireCpus,     METH_VARARGS, "Args: (ncpu) -> Acquire a specific amount of CPUs."},

    {"acquireCpuMask",  dlb_acquireCpuMask,  METH_VARARGS, "Args: (mask) -> Acquire a set of CPUs."},

    {"borrow",          dlb_borrow,          METH_NOARGS,  "Borrow all the possible CPUs registered on DLB."},

    {"borrowCpu",       dlb_borrowCpu,       METH_VARARGS, "Args: (cpuid) -> Borrow a specific CPU."},

    {"borrowCpus",      dlb_borrowCpus,      METH_VARARGS, "Args: (ncpus) -> Borrow a specific amount of CPUs."},

    {"borrowCpuMask",   dlb_borrowCpuMask,   METH_VARARGS, "Args: (mask) -> Borrow a set of CPUs."},

    {"getaffinity",     dlb_getaffinity,     METH_VARARGS, "Args: (mask, [pid=0]) -> Returns the process mask of the given PID."},

    {"setaffinity",     dlb_setaffinity,     METH_VARARGS, "Args: ([pid=0]) -> Set the process mask of the given PID."},

    {NULL, NULL, 0, NULL}        /* Sentinel */
};

static struct PyModuleDef dlbmodule = {
    PyModuleDef_HEAD_INIT,
    "dlb_thread_affinity", /* name of module */
    NULL,                  /* module documentation, may be NULL */
    -1,                    /* size of per-interpreter state of the module, or -1 if the module keeps state in global variables. */
    DlbAffinityMethods
};

PyMODINIT_FUNC PyInit_dlb_affinity(void)
{
    return PyModule_Create(&dlbmodule);
}
