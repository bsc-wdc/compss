/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
#include <Python.h>
/* ****************************************************************** */
#include <param_metadata.h>
#include <GS_compss.h>
#include <stdio.h>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <cassert>
#include <cstddef>

// Uncomment this line to get debug prints
// #define DEBUG

// Basically, debug(args) is a macro that, depending on whether debug is
// defined or not, will translate to printf(args) + flush or to none
#ifdef DEBUG
#define debug(args...) printf(args); fflush(stdout);
#else
#define debug(args...)
#endif

struct module_state {
    PyObject *error;
};

/*
  This is simply a data container
  See process_task
*/
struct parameter {
    PyObject *value;
    int type;
    int direction;
    int stream;
    std::string prefix;
    int size;
    std::string name;

    parameter(PyObject *v, int t, int d, int s, std::string p, int sz, std::string n) {
        value = v;
        type = t;
        direction = d;
        stream = s;
        prefix = p;
        size = sz;
        name = n;
    }

    parameter() { }
    ~parameter() {
        // Nothing to do, as the PyObject pointed by value is owned and managed by
        // the Python interpreter (and therefore incorrect to free it)
    }

};

/*
  Python3 compatibility macros.
  See https://docs.python.org/3/howto/cporting.html
*/
#if PY_MAJOR_VERSION >= 3
#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))
#define PyInt_FromLong PyLong_FromLong
#define PyInt_AsLong PyLong_AsLong
#else
#define GETSTATE(m) (&_state)
static struct module_state _state;
#endif

static PyObject *
error_out(PyObject *m) {
    struct module_state *st = GETSTATE(m);
    PyErr_SetString(st->error, "Compss extension module: Something bad happened");
    return NULL;
}

//////////////////////////////////////////////////////////////

/*
  Start a COMPSs-Runtime instance
*/
static PyObject *
start_runtime(PyObject *self, PyObject *args) {
    debug("####C#### START\n");
    GS_On();
    Py_RETURN_NONE;
}

/*
  Stop the current COMPSs-Runtime instance
*/
static PyObject *
stop_runtime(PyObject *self, PyObject *args) {
    debug("####C#### STOP\n");
    GS_Off();
    Py_RETURN_NONE;
}

/*
  Auxiliary function that allows us to translate a PyStringObject
  (which is also a PyObject) to a char*
  As we can see, the functions body varies depending on the Python version,
  making it compatible for both Py2 and Py3

  W A R N I N G

  Do not EVER free a pointer obtained from here. It will eventually happen
  when the PyObject container gets refcd to 0

*/
static char *
_pystring_to_char(PyObject *c) {
    return
#if PY_MAJOR_VERSION >= 3
        PyBytes_AsString(PyUnicode_AsEncodedString(c, "utf-8", "Error ~"));
#else
        PyString_AsString(c);
#endif
}

static std::string
_pystring_to_string(PyObject *c) {
    return std::string(
#if PY_MAJOR_VERSION >= 3
    PyBytes_AsString(PyUnicode_AsEncodedString(c, "utf-8", "Error ~"))
#else
    PyString_AsString(c)
#endif
    );
}

/*
  Given an integer that can be translated to a type according to the
  datatype enum (see param_metadata.h), return its size.
  If compiled with debug, the name of the type will also appear
*/
static int
_get_type_size(int type) {
    switch ((enum datatype) type) {
    case file_dt:
        debug("#### file_dt\n");
        return sizeof(char*);
    case external_psco_dt:
        debug("#### external_psco_dt\n");
        return sizeof(char*);
    case string_dt:
        debug("#### string_dt\n");
        return sizeof(char*);
    case int_dt:
        debug("#### int_dt\n");
        return sizeof(int);
    case long_dt:
        debug("#### long_dt\n");
        return sizeof(long);
    case double_dt:
        debug("#### double_dt\n");
        return sizeof(double);
    case boolean_dt:
        debug("#### boolean_dt\n");
        return sizeof(int);
    default:
        debug("#### default\n");
        return 0;
    }
    // Here for anti-warning purposes, but this statement will never be reached
    return 0;
}

/*
  Writes the bit representation of the contents of a PyObject in the
  memory address indicated by the void*
*/
static void*
_get_void_pointer_to_content(PyObject *val, int type, int size) {
    // void is not a sizeable type, so we need something that allows us
    // to allocate the exact byte amount we want
    void *ret = new std::uint8_t[size];
    switch ((enum datatype) type) {
        case file_dt:
        case external_psco_dt:
        case string_dt:
            *(char**)ret = _pystring_to_char(val);
            break;
        case int_dt:
            *(int*)ret = int(PyInt_AsLong(val));
            break;
        case long_dt:
            *(long*)ret = PyLong_AsLong(val);
            break;
        case double_dt:
            *(double*)ret =  PyFloat_AsDouble(val);
            break;
        case boolean_dt:
            *(int*)ret = int(PyInt_AsLong(val));
            break;
        default:
            break;
    }
    return ret;
}

/*
  Deletes a void pointer by translating it to its actual type
*/
static void
_delete_void_pointer(void *p, int type) {
    switch ((enum datatype) type) {
    case file_dt:
    case external_psco_dt:
    case string_dt:
        delete (char*)p;
        break;
    default:
        break;
    }
}

/*
  A function that, given a task with its decorator parameters, translates these
  fields to a C-friendly format and sends them to the bindings_common part,
  which is responsible to send them to the COMPSs runtime via JNI.

  This function can be decomposed into three major parts: argument parsing,
  argument packing and conversion to bindings_common argument format
*/
static PyObject *
process_task(PyObject *self, PyObject *args) {
    /*
      Parse python object arguments and get pointers to them.
      lsiiiiiOOOOOO must be read as
      "long, string, integer, integer, integer, integer, integer,
       Object, Object, Object, Object, Object, Object"
    */
    debug("####C#### PROCESS TASK\n");
    long app_id;
    char *signature;
    int priority, num_nodes, replicated, distributed, has_target, num_returns;
    PyObject *values;
    PyObject *names;
    PyObject *compss_types;
    PyObject *compss_directions;
    PyObject *compss_streams;
    PyObject *compss_prefixes;
    //             See comment from above for the meaning of this "magic" string
    if(!PyArg_ParseTuple(args, "lsiiiiiiOOOOOO", &app_id, &signature, &priority,
                         &num_nodes, &replicated, &distributed, &has_target, &num_returns, &values, &names, &compss_types,
                         &compss_directions, &compss_streams, &compss_prefixes)) {
        // Return NULL after ParseTuple automatically translates to "wrong
        // arguments were passed, we expected an integer instead"-like errors
        return NULL;
    }
    debug("####C#### App id: %ld\n", app_id);
    debug("####C#### Signature: %s\n", signature);
    debug("####C#### Priority: %d\n", priority);
    debug("####C#### MPI Num nodes: %d\n", num_nodes);
    debug("####C#### Replicated: %d\n", replicated);
    debug("####C#### Distributed: %d\n", distributed);
    debug("####C#### Has target: %d\n", has_target);
    /*
      Obtain and set all parameter data, and pack it in a struct vector.
      See parameter and its field at the top of this source code file
    */
    Py_ssize_t num_pars = PyList_Size(values);
    debug("####C#### Num pars: %d\n", int(num_pars));
    std::vector< parameter > params(num_pars);
    for(int i = 0; i < num_pars; ++i) {
        debug("Processing parameter %d ...\n", i);
        PyObject *value      = PyList_GetItem(values, i);
        PyObject *type       = PyList_GetItem(compss_types, i);
        PyObject *direction  = PyList_GetItem(compss_directions, i);
        PyObject *stream     = PyList_GetItem(compss_streams, i);
        PyObject *prefix     = PyList_GetItem(compss_prefixes, i);
        PyObject *name       = PyList_GetItem(names, i);
        std::string received_prefix = _pystring_to_string(prefix);
        std::string received_name = _pystring_to_string(name);
        params[i] = parameter(
            value,
            int(PyInt_AsLong(type)),
            int(PyInt_AsLong(direction)),
            int(PyInt_AsLong(stream)),
            received_prefix,
            _get_type_size(int(PyInt_AsLong(type))),
            received_name
        );
        debug("----> Value is at %p\n", &params[i].value);
        debug("----> Type is %d\n", params[i].type);
        debug("----> Direction is %d\n", params[i].direction);
        debug("----> Stream is %d\n", params[i].stream);
        debug("----> Prefix is %s\n", params[i].prefix.c_str());
        debug("----> Size is %d\n", params[i].size);
        debug("----> Name is %s\n", params[i].name.c_str());
    }
    debug("####C#### Adapting C++ data to BC-JNI format...\n");
    /*
      Adapt the parsed data to a bindings-common friendly format.
      These pointers do NOT need to be freed, as the pointed contents
      are out of our control (will be scope-cleaned, or point to PyObject
      contents)
    */
    int num_fields = 6;
    std::vector< void* > unrolled_parameters(num_fields * num_pars, NULL);
    std::vector< char* > prefix_charp(num_pars);
    std::vector< char* > name_charp(num_pars);
    for(int i = 0; i < num_pars; ++i) {
        prefix_charp[i] = (char*)params[i].prefix.c_str();
        name_charp[i]   = (char*)params[i].name.c_str();
        debug("####C#### Processing parameter %d\n", i);
        unrolled_parameters[num_fields * i + 0] = _get_void_pointer_to_content(params[i].value, params[i].type, params[i].size);
        unrolled_parameters[num_fields * i + 1] = (void*)&params[i].type;
        unrolled_parameters[num_fields * i + 2] = (void*)&params[i].direction;
        unrolled_parameters[num_fields * i + 3] = (void*)&params[i].stream;
        unrolled_parameters[num_fields * i + 4] = (void*)&prefix_charp[i];
        unrolled_parameters[num_fields * i + 5] = (void*)&name_charp[i];
    }

    debug("####C#### Calling GS_ExecuteTaskNew...\n");
    /*
      Finally, call bindings common with all the computed parameters
    */
    GS_ExecuteTaskNew(
        app_id,
        signature,
        priority,
        num_nodes,
        replicated,
        distributed,
        has_target,
        num_returns,
        num_pars,
        &unrolled_parameters[0] // hide the fact that params is a std::vector
    );
    debug("####C#### Returning from process_task...\n");
    Py_RETURN_NONE;
}

/*
  Given a PyCOMPSs-id file, get its corresponding COMPSs-id file
*/
static PyObject *
get_file(PyObject *self, PyObject *args) {
    debug("####C#### GET FILE\n");
    char *file_name = _pystring_to_char(PyTuple_GetItem(args, 0));
    int mode = int(PyInt_AsLong(PyTuple_GetItem(args, 1)));
    char *compss_name;
    GS_Get_File(file_name, mode, &compss_name);
    debug("####C#### COMPSs file name %s\n", compss_name);
    PyObject *ret = Py_BuildValue("s", compss_name);
    // File_name must NOT be freed, as it points to a PyObject
    // The same applies to compss_name
    return ret;
}

/*
  Maybe the name is not the most appropriate one.
  This function notifies the runtime that the current file will have one less
  reader from now on.
  Deletion is only performed when this file has no readers and no writers
  AND someone asked to delete this file, so it may not happen
  immediately after calling this function.
*/
static PyObject *
delete_file(PyObject *self, PyObject *args) {
    debug("####C#### DELETE FILE\n");
    char *file_name = _pystring_to_char(PyTuple_GetItem(args, 0));
    debug("####C#### Calling Delete File with file %s\n", file_name);
    GS_Delete_File(file_name);
    debug("####C#### COMPSs delete file name %s with result %d \n", file_name, 0);
    Py_RETURN_NONE;
}

/*
  Closes the file, but this will never imply that this file will ever get deleted
  (not at least with this call).
  The difference in the meanings is the following: close_file only notifies
  that you want to close the file, but you have no idea if this file will be
  open again in the future, while delete_file implies knowledge that this
  file is obsolete and it can be safely deleted.
*/
static PyObject*
close_file(PyObject* self, PyObject* args) {
    char *file_name = _pystring_to_char(PyTuple_GetItem(args, 0));
    int mode = int(PyInt_AsLong(PyTuple_GetItem(args, 1)));
    GS_Close_File(file_name, mode);
    Py_RETURN_NONE;
}

/*
  Notify the runtime that our current application wants to "execute" a barrier.
  Program will be blocked in GS_BarrierNew until all running tasks have ended.
  Notifies the 'no more tasks' boolean value.
*/
static PyObject *
barrier(PyObject *self, PyObject *args) {
    debug("####C#### BARRIER\n");
    long app_id;
    PyObject *py_no_more_tasks;
    bool no_more_tasks;
    if(!PyArg_ParseTuple(args, "lO", &app_id, &py_no_more_tasks)) {
        return NULL;
    }
    no_more_tasks = PyObject_IsTrue(py_no_more_tasks);  // Verify that is a bool
    debug("####C#### COMPSs barrier for AppId: %ld \n", (app_id));
    debug("####C#### COMPSs barrier no more tasks?: %s \n", (no_more_tasks?"true":"false"));
    GS_BarrierNew(app_id, no_more_tasks);
    Py_RETURN_NONE;
}

/*
  Returns the logging path.
*/
static PyObject *
get_logging_path(PyObject *self, PyObject *args) {
    debug("####C#### GET LOG PATH\n");
    char *log_path;
    GS_Get_AppDir(&log_path);
    debug("####C#### COMPSs log path %s\n", log_path);
    // This makes log_path unallocatable
    PyObject *ret = Py_BuildValue("s", log_path);
    return ret;
}

static PyObject *
register_core_element(PyObject *self, PyObject *args) {
    debug("####C#### REGISTER CORE ELEMENT\n");
    char *CESignature,
         *ImplSignature,
         *ImplConstraints,
         *ImplType;
    PyObject *typeArgs;
    if(!PyArg_ParseTuple(args, "ssssO", &CESignature, &ImplSignature,
                         &ImplConstraints, &ImplType, &typeArgs)) {
        return NULL;
    }

    debug("####C#### Core Element Signature: %s\n", CESignature);
    debug("####C#### Implementation Signature: %s\n", ImplSignature);
    debug("####C#### Implementation Constraints: %s\n", ImplConstraints);
    debug("####C#### Implementation Type: %s\n", ImplType);
    int num_params = PyList_Size(typeArgs);
    debug("####C#### Implementation Type num args: %i\n", num_params);
    char **ImplTypeArgs = new char*[num_params];
    for(int i = 0; i < num_params; ++i) {
        ImplTypeArgs[i] = _pystring_to_char(PyList_GetItem(typeArgs, i));
        debug("####C#### Implementation Type Args: %s\n", ImplTypeArgs[i]);
    }
    // Invoke the C library
    GS_RegisterCE(CESignature,
                  ImplSignature,
                  ImplConstraints,
                  ImplType,
                  num_params,
                  ImplTypeArgs);
    debug("####C#### COMPSs ALREADY REGISTERED THE CORE ELEMENT\n");
    // Free all allocated memory
    delete ImplTypeArgs;
    Py_RETURN_NONE;
}

/*
  Method definition, generic argument speficication, and __doc__ field value
*/
static PyMethodDef CompssMethods[] = {
    { "error_out", (PyCFunction)error_out, METH_NOARGS, NULL},
    { "start_runtime", start_runtime, METH_VARARGS, "Start the COMPSs runtime." },
    { "stop_runtime", stop_runtime, METH_VARARGS, "Stop the COMPSs runtime." },
    { "process_task", process_task, METH_VARARGS, "Process a task call from the application." },
    { "get_file", get_file, METH_VARARGS, "Get a file for opening. The file can contain an object." },
    { "delete_file", delete_file, METH_VARARGS, "Delete a file." },
    { "close_file", close_file, METH_VARARGS, "Close a file." },
    { "barrier", barrier, METH_VARARGS, "Perform a barrier until the tasks already submitted have finished." },
    { "get_logging_path", get_logging_path, METH_VARARGS, "Requests the app log path." },
    { "register_core_element", register_core_element, METH_VARARGS, "Registers a task in the Runtime." },
    { NULL, NULL } /* sentinel */
};

extern "C" {

#if PY_MAJOR_VERSION >= 3
    static int compss_traverse(PyObject *m, visitproc visit, void *arg) {
        Py_VISIT(GETSTATE(m)->error);
        return 0;
    }
    static int compss_clear(PyObject *m) {
        Py_CLEAR(GETSTATE(m)->error);
        return 0;
    }
    static struct PyModuleDef cModPy = {
        PyModuleDef_HEAD_INIT,
        "compss",                      /* name of module */
        NULL,                          /* module documentation, may be NULL */
        sizeof(struct module_state),   /* size of per-interpreter state of the module, or -1 if the module keeps state in global variables. */
        CompssMethods,
        NULL,
        compss_traverse,
        compss_clear,
        NULL
    };
#define INITERROR return NULL
    PyMODINIT_FUNC
    PyInit_compss(void)
#else
#define INITERROR return
    void initcompss(void)
#endif
    {
#if PY_MAJOR_VERSION >= 3
        PyObject *module = PyModule_Create(&cModPy);
#else
        PyObject *module = Py_InitModule("compss", CompssMethods);
#endif

        if (module == NULL)
            INITERROR;
        struct module_state *st = GETSTATE(module);

        st->error = PyErr_NewException("compss.Error", NULL, NULL);
        if (st->error == NULL) {
            Py_DECREF(module);
            INITERROR;
        }

#if PY_MAJOR_VERSION >= 3
        return module;
#endif
    }

};
