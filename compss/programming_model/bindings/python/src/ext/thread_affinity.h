
/*
  Wrappers that make possible to call thread (set|get)affinity from Python2

  @author: srodrig1 (sergio.rodriguez at bsc.es)
*/
#pragma once
#include <Python.h>
#include <structmember.h>
#include <unistd.h>
#include <sched.h>
#include <vector>

/*
  Wrapper for sched_setaffinity.
  Arguments:
  - mask: a list of integers that denote the CPU identifiers (0-based) that we
          want to allow
  - pid: if zero, this will be transformed to the current pid
  Returns None
*/
static PyObject* pysched_setaffinity(PyObject* self, PyObject* args);


/*
  Wrapper for sched_getaffinity.
  Arguments:
  - pid (OPTIONAL): if zero or ommited, this will be transformed to the current pid
  Returns the list of allowed CPUs
*/
static PyObject* pysched_getaffinity(PyObject* self, PyObject* args);


extern "C" {
#if PY_MAJOR_VERSION >= 3
    PyMODINIT_FUNC
    PyInit_thread_affinity(void);
#else
    void initthread_affinity(void);
#endif
}