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

bool DEBUG_MODE = false;
char* HEADER = "[PY-C EXTENSION]  -  ";

struct module_state {
    PyObject* error;
};

/*
  This is simply a data container
  See process_task
*/
struct parameter {
    PyObject* value;
    int type;
    int direction;
    int stream;
    std::string prefix;
    int size;
    std::string name;
    std::string c_type;
    std::string weight;
    int keep_rename;

    parameter(PyObject *v, int t, int d, int s, std::string p, int sz, std::string n, std::string ct, std::string w, int kr) {
        value = v;
        type = t;
        direction = d;
        stream = s;
        prefix = p;
        size = sz;
        name = n;
        c_type = ct;
        weight = w;
        keep_rename = kr;
    }

    parameter() { }
    ~parameter() {
        // Nothing to do, as the PyObject pointed by value is owned and managed by
        // the Python interpreter (and therefore incorrect to free it)
    }

};

/*
  Python3 compatibility only.
  See https://docs.python.org/3/howto/cporting.html
*/
#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))
#define PyInt_FromLong PyLong_FromLong
#define PyInt_AsLong PyLong_AsLong

//////////////////////////////////////////////////////////////
// Debugging functions
//////////////////////////////////////////////////////////////

static void debug(const char * format, ...){
    if (DEBUG_MODE == true) {
        va_list arg;
        va_start(arg, format);
        std::string message = HEADER;
        message += format;
        vprintf(message.c_str(), arg);
        va_end(arg);
        fflush(stdout);
    }
}

/*
  Set debug mode.
  Separate function since it can be deactivated and/or activated when
  necessary.
*/
static PyObject* set_debug(PyObject* self, PyObject* args) {
    bool debug_mode = PyObject_IsTrue(PyTuple_GetItem(args, 0));
    DEBUG_MODE = debug_mode;
    Py_RETURN_NONE;
}

//////////////////////////////////////////////////////////////
// Internal functions
//////////////////////////////////////////////////////////////

/*
   Auxiliary function to print errors.
*/
static PyObject* error_out(PyObject *m) {
    struct module_state* st = GETSTATE(m);
    PyErr_SetString(st->error, "Compss extension module: Something bad happened");
    return NULL;
}

/*
  Auxiliary function that allows us to translate a PyStringObject
  (which is also a PyObject) to a char*

  W A R N I N G

  Do not EVER free a pointer obtained from here. It will eventually happen
  when the PyObject container gets refcd to 0.
*/
static char* _pystring_to_char(PyObject* c) {
    return
        PyBytes_AsString(PyUnicode_AsEncodedString(c, "utf-8", "Error ~"));
}

/*
   Auxiliary functions to convert pystring to string.
*/
static std::string _pystring_to_string(PyObject* c) {
    return std::string(
        PyBytes_AsString(PyUnicode_AsEncodedString(c, "utf-8", "Error ~"))
    );
}

/*
  Given an integer that can be translated to a type according to the
  datatype enum (see param_metadata.h), return its size.
  If compiled with debug, the name of the type will also appear.
*/
static int _get_type_size(int type) {
    switch ((enum datatype) type) {
    case file_dt:
        debug("- Type: file_dt\n");
        return sizeof(char*);
    case external_stream_dt:
        debug("- Type: external_stream_dt\n");
        return sizeof(char*);
    case external_psco_dt:
        debug("- Type: external_psco_dt\n");
        return sizeof(char*);
    case string_dt:
        debug("- Type: string_dt\n");
        return sizeof(char*);
    case string_64_dt:
        debug("- Type: string_64_dt\n");
        return sizeof(char*);
    case int_dt:
        debug("- Type: int_dt\n");
        return sizeof(int);
    case long_dt:
        debug("- Type: long_dt\n");
        return sizeof(long);
    case double_dt:
        debug("- Type: double_dt\n");
        return sizeof(double);
    case boolean_dt:
        debug("- Type: boolean_dt\n");
        return sizeof(int);
    default:
        debug("- Type: default\n");
        return 0;
    }
    // Here for anti-warning purposes, but this statement will never be reached
    return 0;
}

/*
  Writes the bit representation of the contents of a PyObject in the
  memory address indicated by the void*.
*/
static void* _get_void_pointer_to_content(PyObject* val, int type, int size) {
    // void is not a sizeable type, so we need something that allows us
    // to allocate the exact byte amount we want
    void* ret = new std::uint8_t[size];
    switch ((enum datatype) type) {
        case file_dt:
        case directory_dt:
        case external_stream_dt:
        case external_psco_dt:
        case string_dt:
        case string_64_dt:
        case collection_dt:
        case dict_collection_dt:
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


//////////////////////////////////////////////////////////////
// API functions
//////////////////////////////////////////////////////////////

/*
  Start a COMPSs-Runtime instance.
*/
static PyObject* start_runtime(PyObject* self, PyObject* args) {
    debug("Start runtime\n");
    GS_On();
    Py_RETURN_NONE;
}

/*
  Stop the current COMPSs-Runtime instance.
*/
static PyObject* stop_runtime(PyObject* self, PyObject* args) {
    int code = int(PyInt_AsLong(PyTuple_GetItem(args, 0)));
    debug("Stop runtime with code: %i\n", (code));
    GS_Off(code);
    Py_RETURN_NONE;
}

/*
  Cancel all application tasks
*/
static PyObject* cancel_application_tasks(PyObject* self, PyObject* args){
    long app_id = long(PyInt_AsLong(PyTuple_GetItem(args, 0)));
    debug("COMPSs cancel application tasks for AppId: %ld\n", (app_id));
    GS_Cancel_Application_Tasks(app_id);
    Py_RETURN_NONE;
}

/*
  A function that initiatiates the pipe mechanism setting two pipes.
  First argument will be the command_pipe, where the binding_commons library
  writes the commands, and the second argument will be the result_pipe, where
  the binding_commons library expects the command results
*/
static PyObject* set_pipes(PyObject* self, PyObject* args){
	char* command_pipe = _pystring_to_char(PyTuple_GetItem(args, 0));
	char* result_pipe = _pystring_to_char(PyTuple_GetItem(args, 1));
	GS_set_pipes(command_pipe,result_pipe);
	Py_RETURN_NONE;
}

/*
  A function that reads a command from the pipe mechanism set with set_pipes 
  method. No arguments are used in the function. The result is a string containing
  the command read from the pipe.
*/
static PyObject* read_pipes(PyObject* self, PyObject* args){
    char* command;
    GS_read_pipes(&command);
    PyObject *ret = Py_BuildValue("s", command);
    return ret;
}

/*
  A function that, given a task with its decorator parameters, translates these
  fields to a C-friendly format and sends them to the bindings_common part,
  which is responsible to send them to the COMPSs runtime via JNI.

  This function can be decomposed into three major parts: argument parsing,
  argument packing and conversion to bindings_common argument format.
*/
static PyObject* process_task(PyObject* self, PyObject* args) {
    /*
      Parse python object arguments and get pointers to them.
      lsiiiiiiiOOOOOO must be read as
      "long, string, integer, integer, integer, integer, integer, integer,
       Object, Object, Object, Object, Object, Object"
    */
    debug("Process task:\n");
    long app_id;
    char* signature;
    char* on_failure;
    int priority, num_nodes, reduce, chunk_size, replicated, distributed, has_target, num_returns, time_out;
    PyObject *values;
    PyObject *names;
    PyObject *compss_types;
    PyObject *compss_directions;
    PyObject *compss_streams;
    PyObject *compss_prefixes;
    PyObject *content_types;
    PyObject *weights;
    PyObject *keep_renames;
    //             See comment from above for the meaning of this "magic" string
    if(!PyArg_ParseTuple(args, "lssiiiiiiiiiOOOOOOOOO", &app_id, &signature, &on_failure, &time_out, &priority,
                         &num_nodes, &reduce, &chunk_size, &replicated, &distributed, &has_target, &num_returns, &values, &names, &compss_types,
                         &compss_directions, &compss_streams, &compss_prefixes, &content_types, &weights, &keep_renames)) {
        // Return NULL after ParseTuple automatically translates to "wrong
        // arguments were passed, we expected an integer instead"-like errors
        return NULL;
    }
    debug("- App id: %ld\n", app_id);
    debug("- Signature: %s\n", signature);
    debug("- On Failure: %s\n", on_failure);
    debug("- Time Out: %d\n", time_out);
    debug("- Priority: %d\n", priority);
    debug("- Reduce: %d\n", reduce);
    debug("- Chunk size: %d\n", chunk_size);
    debug("- MPI Num nodes: %d\n", num_nodes);
    debug("- Replicated: %d\n", replicated);
    debug("- Distributed: %d\n", distributed);
    debug("- Has target: %d\n", has_target);
    /*
      Obtain and set all parameter data, and pack it in a struct vector.

      See parameter and its field at the top of this source code file
    */
    Py_ssize_t num_pars = PyList_Size(values);
    debug("Num pars: %d\n", int(num_pars));
    std::vector< parameter > params(num_pars);
    std::vector< char* > prefix_charp(num_pars);
    std::vector< char* > name_charp(num_pars);
    std::vector< char* > c_type_charp(num_pars);
    std::vector< char* > weight_charp(num_pars);
    int num_fields = 9;
    std::vector< void* > unrolled_parameters(num_fields * num_pars, NULL);

    for(int i = 0; i < num_pars; ++i) {
        debug("Processing parameter %d ...\n", i);
        PyObject *value      = PyList_GetItem(values, i);
        PyObject *type       = PyList_GetItem(compss_types, i);
        PyObject *direction  = PyList_GetItem(compss_directions, i);
        PyObject *stream     = PyList_GetItem(compss_streams, i);
        PyObject *prefix     = PyList_GetItem(compss_prefixes, i);
        PyObject *name       = PyList_GetItem(names, i);
        PyObject *c_type     = PyList_GetItem(content_types, i);
        PyObject *weight	 = PyList_GetItem(weights, i);
        PyObject *k_rename   = PyList_GetItem(keep_renames, i);
        std::string received_prefix = _pystring_to_string(prefix);
        std::string received_name = _pystring_to_string(name);
        std::string received_c_type = _pystring_to_string(c_type);
        std::string received_weight = _pystring_to_string(weight);

        params[i] = parameter(
            value,
            int(PyInt_AsLong(type)),
            int(PyInt_AsLong(direction)),
            int(PyInt_AsLong(stream)),
            received_prefix,
            _get_type_size(int(PyInt_AsLong(type))),
            received_name,
            received_c_type,
			received_weight,
			int(PyInt_AsLong(k_rename))
        );

        debug("Adapting C++ data to BC-JNI format...\n");
        /*
          Adapt the parsed data to a bindings-common friendly format.
          These pointers do NOT need to be freed, as the pointed contents
          are out of our control (will be scope-cleaned, or point to PyObject
          contents)
        */
		prefix_charp[i] = (char*) params[i].prefix.c_str();
		name_charp[i] = (char*) params[i].name.c_str();
		c_type_charp[i] = (char*) params[i].c_type.c_str();
		weight_charp[i] = (char*) params[i].weight.c_str();

		debug("Processing parameter %d\n", i);
	    /*
	      Adapt the parsed data to a bindings-common friendly format.
	      These pointers do NOT need to be freed, as the pointed contents
	      are out of our control (will be scope-cleaned, or point to PyObject
	      contents)
	    */
		unrolled_parameters[num_fields * i + 0] = _get_void_pointer_to_content(params[i].value, params[i].type, params[i].size);
		unrolled_parameters[num_fields * i + 1] = (void*) &params[i].type;
		unrolled_parameters[num_fields * i + 2] = (void*) &params[i].direction;
		unrolled_parameters[num_fields * i + 3] = (void*) &params[i].stream;
		unrolled_parameters[num_fields * i + 4] = (void*) &prefix_charp[i];
		unrolled_parameters[num_fields * i + 5] = (void*) &name_charp[i];
		unrolled_parameters[num_fields * i + 6] = (void*) &c_type_charp[i];
		unrolled_parameters[num_fields * i + 7] = (void*) &weight_charp[i];
		unrolled_parameters[num_fields * i + 8] = (void*) &params[i].keep_rename;

        debug("----> Value is at %p\n", &params[i].value);
        debug("----> Type: %d\n", params[i].type);
        debug("----> Direction: %d\n", params[i].direction);
        debug("----> Stream: %d\n", params[i].stream);
        debug("----> Prefix: %s\n", prefix_charp[i]);
        debug("----> Size: %d\n", params[i].size);
        debug("----> Name: %s\n", name_charp[i]);
        debug("----> Content: %s\n", c_type_charp[i]);
        debug("----> Weight: %s\n", weight_charp[i]);
        debug("----> Keep rename: %d\n", params[i].keep_rename);

    }

    debug("Calling GS_ExecuteTaskNew...\n");
    /*
      Finally, call bindings common with all the computed parameters
    */
    GS_ExecuteTaskNew(
        app_id,
        signature,
        on_failure,
        time_out,
        priority,
        num_nodes,
        reduce,
        chunk_size,
        replicated,
        distributed,
        has_target,
        num_returns,
        num_pars,
        &unrolled_parameters[0] // hide the fact that params is a std::vector
    );
    debug("Returning from process_task...\n");
    Py_RETURN_NONE;
}

/*
  process http task
*/
static PyObject* process_http_task(PyObject* self, PyObject* args) {
    /*
      Parse python object arguments and get pointers to them.
      lsiiiiiiiOOOOOO must be read as
      "long, string, integer, integer, integer, integer, integer, integer,
       Object, Object, Object, Object, Object, Object"
    */
    debug("Process http task:\n");
    long app_id;
    char* signature;
    char* on_failure;
    int priority, num_nodes, reduce, chunk_size, replicated, distributed, has_target, num_returns, time_out;
    PyObject *values;
    PyObject *names;
    PyObject *compss_types;
    PyObject *compss_directions;
    PyObject *compss_streams;
    PyObject *compss_prefixes;
    PyObject *content_types;
    PyObject *weights;
    PyObject *keep_renames;
    //             See comment from above for the meaning of this "magic" string
    if(!PyArg_ParseTuple(args, "lssiiiiiiiiiOOOOOOOOO", &app_id, &signature, &on_failure, &time_out, &priority, &num_nodes, &reduce,
                                &chunk_size, &replicated, &distributed, &has_target, &num_returns, &values, &names, &compss_types,
                                &compss_directions, &compss_streams, &compss_prefixes, &content_types, &weights, &keep_renames)) {
        // Return NULL after ParseTuple automatically translates to "wrong
        // arguments were passed, we expected an integer instead"-like errors
        return NULL;
    }
    debug("- App id: %ld\n", app_id);
    debug("- Signature: %s\n", signature);
    debug("- On Failure: %s\n", on_failure);
    debug("- Time Out: %d\n", time_out);
    debug("- Priority: %d\n", priority);
    debug("- Reduce: %d\n", reduce);
    debug("- Chunk size: %d\n", chunk_size);
    debug("- MPI Num nodes: %d\n", num_nodes);
    debug("- Replicated: %d\n", replicated);
    debug("- Distributed: %d\n", distributed);
    debug("- Has target: %d\n", has_target);
    /*
      Obtain and set all parameter data, and pack it in a struct vector.

      See parameter and its field at the top of this source code file
    */
    Py_ssize_t num_pars = PyList_Size(values);
    debug("Num pars: %d\n", int(num_pars));
    std::vector< parameter > params(num_pars);
    std::vector< char* > prefix_charp(num_pars);
    std::vector< char* > name_charp(num_pars);
    std::vector< char* > c_type_charp(num_pars);
    std::vector< char* > weight_charp(num_pars);
    int num_fields = 9;
    std::vector< void* > unrolled_parameters(num_fields * num_pars, NULL);

    for(int i = 0; i < num_pars; ++i) {
        debug("Processing parameter %d ...\n", i);
        PyObject *value      = PyList_GetItem(values, i);
        PyObject *type       = PyList_GetItem(compss_types, i);
        PyObject *direction  = PyList_GetItem(compss_directions, i);
        PyObject *stream     = PyList_GetItem(compss_streams, i);
        PyObject *prefix     = PyList_GetItem(compss_prefixes, i);
        PyObject *name       = PyList_GetItem(names, i);
        PyObject *c_type     = PyList_GetItem(content_types, i);
        PyObject *weight	 = PyList_GetItem(weights, i);
        PyObject *k_rename   = PyList_GetItem(keep_renames, i);
        std::string received_prefix = _pystring_to_string(prefix);
        std::string received_name = _pystring_to_string(name);
        std::string received_c_type = _pystring_to_string(c_type);
        std::string received_weight = _pystring_to_string(weight);

        params[i] = parameter(
            value,
            int(PyInt_AsLong(type)),
            int(PyInt_AsLong(direction)),
            int(PyInt_AsLong(stream)),
            received_prefix,
            _get_type_size(int(PyInt_AsLong(type))),
            received_name,
            received_c_type,
			received_weight,
			int(PyInt_AsLong(k_rename))
        );

        debug("Adapting C++ data to BC-JNI format...\n");
        /*
          Adapt the parsed data to a bindings-common friendly format.
          These pointers do NOT need to be freed, as the pointed contents
          are out of our control (will be scope-cleaned, or point to PyObject
          contents)
        */
		prefix_charp[i] = (char*) params[i].prefix.c_str();
		name_charp[i] = (char*) params[i].name.c_str();
		c_type_charp[i] = (char*) params[i].c_type.c_str();
		weight_charp[i] = (char*) params[i].weight.c_str();

		debug("Processing parameter %d\n", i);
	    /*
	      Adapt the parsed data to a bindings-common friendly format.
	      These pointers do NOT need to be freed, as the pointed contents
	      are out of our control (will be scope-cleaned, or point to PyObject
	      contents)
	    */
		unrolled_parameters[num_fields * i + 0] = _get_void_pointer_to_content(params[i].value, params[i].type, params[i].size);
		unrolled_parameters[num_fields * i + 1] = (void*) &params[i].type;
		unrolled_parameters[num_fields * i + 2] = (void*) &params[i].direction;
		unrolled_parameters[num_fields * i + 3] = (void*) &params[i].stream;
		unrolled_parameters[num_fields * i + 4] = (void*) &prefix_charp[i];
		unrolled_parameters[num_fields * i + 5] = (void*) &name_charp[i];
		unrolled_parameters[num_fields * i + 6] = (void*) &c_type_charp[i];
		unrolled_parameters[num_fields * i + 7] = (void*) &weight_charp[i];
		unrolled_parameters[num_fields * i + 8] = (void*) &params[i].keep_rename;

        debug("----> Value is at %p\n", &params[i].value);
        debug("----> Type: %d\n", params[i].type);
        debug("----> Direction: %d\n", params[i].direction);
        debug("----> Stream: %d\n", params[i].stream);
        debug("----> Prefix: %s\n", prefix_charp[i]);
        debug("----> Size: %d\n", params[i].size);
        debug("----> Name: %s\n", name_charp[i]);
        debug("----> Content: %s\n", c_type_charp[i]);
        debug("----> Weight: %s\n", weight_charp[i]);
        debug("----> Keep rename: %d\n", params[i].keep_rename);

    }

    debug("Calling GS_ExecuteHttpTask...\n");
    /*
      Finally, call bindings common with all the computed parameters
    */
    GS_ExecuteHttpTask(
        app_id,
        signature,
        on_failure,
        time_out,
        priority,
        num_nodes,
        reduce,
        chunk_size,
        replicated,
        distributed,
        has_target,
        num_returns,
        num_pars,
        &unrolled_parameters[0] // hide the fact that params is a std::vector
    );
    debug("Returning from process_http_task...\n");
    Py_RETURN_NONE;
}

/*
  Given a PyCOMPSs-id file, check if it has been accessed before
*/
static PyObject* accessed_file(PyObject* self, PyObject* args) {
    debug("####C#### ACCESSED FILE\n");
    long app_id = int(PyInt_AsLong(PyTuple_GetItem(args, 0)));
    char* file_name = _pystring_to_char(PyTuple_GetItem(args, 1));
    debug("Accessed file? %s\n", (file_name));
    if (GS_Accessed_File(app_id, file_name) == 0) {
        debug("- False\n");
    	Py_RETURN_FALSE;
    } else {
        debug("- True\n");
    	Py_RETURN_TRUE;
    }
}

/*
  Given a PyCOMPSs-id file, get its corresponding COMPSs-id file.
*/
static PyObject* open_file(PyObject* self, PyObject* args) {
    debug("####C#### GET FILE\n");
    long app_id = int(PyInt_AsLong(PyTuple_GetItem(args, 0)));
    char* file_name = _pystring_to_char(PyTuple_GetItem(args, 1));
    debug("Open file %s\n", (file_name));
    int mode = int(PyInt_AsLong(PyTuple_GetItem(args, 2)));
    debug("- mode: %i\n", (mode));
    char* compss_name;
    GS_Open_File(app_id, file_name, mode, &compss_name);
    debug("COMPSs file to open: %s\n", (compss_name));
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
static PyObject* delete_file(PyObject* self, PyObject* args) {
    debug("####C#### DELETE FILE\n");
    long app_id = int(PyInt_AsLong(PyTuple_GetItem(args, 0)));
    char* file_name = _pystring_to_char(PyTuple_GetItem(args, 1));
    debug("Delete file: %s\n", (file_name));
    bool wait = PyObject_IsTrue(PyTuple_GetItem(args, 2));
    debug("- Wait: %s\n", (wait ? "true" : "false"));
    bool applicationDelete = PyObject_IsTrue(PyTuple_GetItem(args, 3));
    debug("- applicationDelete: %s\n", (applicationDelete ? "true" : "false"));
    GS_Delete_File(app_id, file_name, wait, applicationDelete);
    debug("COMPSs file deleted: %s\n", (file_name));
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
static PyObject* close_file(PyObject* self, PyObject* args) {
    long app_id = int(PyInt_AsLong(PyTuple_GetItem(args, 0)));
    char* file_name = _pystring_to_char(PyTuple_GetItem(args, 1));
    debug("Close file: %s\n", (file_name));
    int mode = int(PyInt_AsLong(PyTuple_GetItem(args, 2)));
    debug("- Mode: %i\n", (mode));
    GS_Close_File(app_id, file_name, mode);
    debug("File closed\n");
    Py_RETURN_NONE;
}

/*
  Given a PyCOMPSs-id file, get its corresponding last version COMPSs-id file
*/
static PyObject* get_file(PyObject* self, PyObject* args) {
    char* file_name = _pystring_to_char(PyTuple_GetItem(args, 1));
    debug("Get file: %s\n", (file_name));
    long app_id = long(PyInt_AsLong(PyTuple_GetItem(args, 0)));
    debug("- App id: %ld\n", (app_id));
    GS_Get_File(app_id, file_name);
    debug("COMPSs file already get: %s\n", (file_name));
    Py_RETURN_NONE;
}

/*
  Given a PyCOMPSs-id directory, get its corresponding last version
*/
static PyObject* get_directory(PyObject *self, PyObject *args) {
    char* dir_name = _pystring_to_char(PyTuple_GetItem(args, 1));
    debug("Get directory: %s\n", (dir_name));
    long app_id = long(PyInt_AsLong(PyTuple_GetItem(args, 0)));
    debug("- App id: %ld\n", (app_id));
    GS_Get_Directory(app_id, dir_name);
    debug("COMPSs directory already get: %s\n", (dir_name));
    Py_RETURN_NONE;
}

/*
  Notify the runtime that our current application wants to "execute" a barrier.
  Program will be blocked in GS_BarrierNew until all running tasks have ended.
  Notifies the 'no more tasks' boolean value.
*/
static PyObject* barrier(PyObject* self, PyObject* args) {
    debug("Barrier\n");
    long app_id;
    PyObject* py_no_more_tasks;
    bool no_more_tasks;
    if(!PyArg_ParseTuple(args, "lO", &app_id, &py_no_more_tasks)) {
        return NULL;
    }
    no_more_tasks = PyObject_IsTrue(py_no_more_tasks);  // Verify that is a bool
    debug("- App id: %ld \n", (app_id));
    debug("- No more tasks?: %s \n", (no_more_tasks ? "true" : "false"));
    GS_BarrierNew(app_id, no_more_tasks);
    debug("Barrier end\n");
    Py_RETURN_NONE;
}

/*
  Notify the runtime that our current application wants to "execute" a barrier for a group.
  Program will be blocked in GS_BarrierGroup until all running tasks part of the group have ended.
*/
static PyObject* barrier_group(PyObject* self, PyObject* args) {
    char* group_name = _pystring_to_char(PyTuple_GetItem(args, 1));
    debug("Barrier group: %s\n", (group_name));
    long app_id = long(PyInt_AsLong(PyTuple_GetItem(args, 0)));
    debug("- App id: %ld \n", (app_id));
    char* exception_message = NULL;
    GS_BarrierGroup(app_id, group_name, &exception_message);
    debug("Barrier group end: %s\n", (group_name));
    if (exception_message != NULL){
        PyObject* message_object = Py_BuildValue("s", exception_message);
        debug("- COMPSs exception raised : %s \n", (exception_message));
        return message_object;
    } else {
        Py_RETURN_NONE;
    }
}

/*
  Creates a new task group that will include all the subsequent tasks.
*/
static PyObject* open_task_group(PyObject* self, PyObject* args) {
    char *group_name = _pystring_to_char(PyTuple_GetItem(args, 0));
    debug("Open task group: %s\n", (group_name));
    bool implicit_barrier = PyObject_IsTrue(PyTuple_GetItem(args, 1));
    debug("- Implicit barrier?: %s \n", (implicit_barrier ? "true" : "false"));
    long app_id = long(PyInt_AsLong(PyTuple_GetItem(args, 2)));
    debug("- App id: %ld \n", (app_id));
    GS_OpenTaskGroup(group_name, implicit_barrier, app_id);
    debug("Task group: %s created\n", (group_name));
    Py_RETURN_NONE;
}

/*
  Closes the opened task group.
*/
static PyObject* close_task_group(PyObject* self, PyObject* args) {
    char* group_name = _pystring_to_char(PyTuple_GetItem(args, 0));
    debug("Close task group: %s\n", (group_name));
    long app_id = long(PyInt_AsLong(PyTuple_GetItem(args, 1)));
    debug("- App id: %ld \n", (app_id));
    GS_CloseTaskGroup(group_name, app_id);
    debug("Task group: %s closed\n", (group_name));
    Py_RETURN_NONE;
}

/*
  Cancels a task group.
*/
static PyObject* cancel_task_group(PyObject* self, PyObject* args) {
    char* group_name = _pystring_to_char(PyTuple_GetItem(args, 0));
    debug("Cancel task group: %s\n", (group_name));
    long app_id = long(PyInt_AsLong(PyTuple_GetItem(args, 1)));
    debug("- App id: %ld \n", (app_id));
    char* exception_message = NULL;
    GS_CancelTaskGroup(group_name, app_id, &exception_message);
	debug("Task group: %s cancelled\n", (group_name));
	debug("Barrier group end: %s\n", (group_name));
	if (exception_message != NULL) {
		PyObject* message_object = Py_BuildValue("s", exception_message);
		debug("- COMPSs exception raised : %s \n", (exception_message));
		return message_object;
	} else {
		Py_RETURN_NONE;
	}

}

/*
  Notify the runtime that our current application wants to "execute" a snapshot.
*/
static PyObject* snapshot(PyObject* self, PyObject* args) {
    debug("Snapshot\n");
    long app_id;
    if(!PyArg_ParseTuple(args, "l", &app_id)) {
        return NULL;
    }
    debug("- App id: %ld \n", (app_id));
    GS_Snapshot(app_id);
    debug("Snapshot end\n");
    Py_RETURN_NONE;
}

/*
  Notify the event emission.
*/
static PyObject* emit_event(PyObject *self, PyObject *args) {
    int type = int(PyInt_AsLong(PyTuple_GetItem(args, 0)));
    long value = long(PyInt_AsLong(PyTuple_GetItem(args, 1)));
    debug("Emit Event: (%i, %ld)\n", type, value);
    GS_EmitEvent(type, value);
    Py_RETURN_NONE;
}


/*
  Returns the logging path.
*/
static PyObject* get_logging_path(PyObject* self, PyObject* args) {
    debug("Get logs path\n");
    char* log_path;
    GS_Get_AppDir(&log_path);
    debug("- COMPSs log path %s\n", (log_path));
    // This makes log_path unallocatable
    PyObject* ret = Py_BuildValue("s", log_path);
    return ret;
}

/*
  Returns the master working path.
*/
static PyObject* get_master_working_path(PyObject* self, PyObject* args) {
    debug("Get master working path\n");
    char* master_working_path;
    GS_Get_MasterWorkingDir(&master_working_path);
    debug("- COMPSs master working path %s\n", (master_working_path));
    // This makes log_path unallocatable
    PyObject* ret = Py_BuildValue("s", master_working_path);
    return ret;
}

/*
  Requests the number of active resources to the runtime.
*/
static PyObject* get_number_of_resources(PyObject* self, PyObject* args) {
    debug("Get number of resources\n");
    long app_id;
    if (!PyArg_ParseTuple(args, "l", &app_id)) {
        return NULL;
    }
    debug("- App id: %ld\n", (app_id));
    int resources = GS_GetNumberOfResources(app_id);
    debug("Number of resources: %i\n", (resources));
    PyObject* ret = Py_BuildValue("i", resources);
    return ret;
}

/*
  Requests the runtime to increase a given number of resources.
*/
static PyObject* request_resources(PyObject* self, PyObject* args) {
    debug("Request resources creation\n");
    long app_id = long(PyInt_AsLong(PyTuple_GetItem(args, 0)));
    int num_resources = int(PyInt_AsLong(PyTuple_GetItem(args, 1)));
    char* group_name = _pystring_to_char(PyTuple_GetItem(args, 2));

    debug("- App id: %ld\n", (app_id));
    debug("- Number of resources: %i\n", (num_resources));
    debug("- Group name: %s\n", (group_name));

    GS_RequestResources(app_id, num_resources, group_name);
    Py_RETURN_NONE;
}

/*
  Requests the runtime to decrease a given number of resources.
*/
static PyObject* free_resources(PyObject* self, PyObject* args) {
    debug("Request resources destruction\n");

    long app_id = long(PyInt_AsLong(PyTuple_GetItem(args, 0)));
    int num_resources = int(PyInt_AsLong(PyTuple_GetItem(args, 1)));
    char *group_name = _pystring_to_char(PyTuple_GetItem(args, 2));

    debug("- App id: %ld\n", (app_id));
    debug("- Number of resources: %i\n", (num_resources));
    debug("- Group name: %s\n", (group_name));

    GS_FreeResources(app_id, num_resources, group_name);
    Py_RETURN_NONE;
}

/*
  Requests the runtime to decrease a given number of resources.
*/
static PyObject* set_wall_clock(PyObject* self, PyObject* args) {
    debug("Setting wall clock limit\n");

    long app_id = long(PyInt_AsLong(PyTuple_GetItem(args, 0)));
    long wcl = long(PyInt_AsLong(PyTuple_GetItem(args, 1)));

    debug("- App id: %ld\n", (app_id));
    debug("- Number of resources: %ld\n", (wcl));

    GS_Set_wall_clock(app_id, wcl, 0);
    Py_RETURN_NONE;
}

/*
  Registers a new core element
*/
static PyObject* register_core_element(PyObject* self, PyObject* args) {
    debug("Register core element\n");
    char* CESignature;
    char* ImplSignature;
    char* ImplConstraints;
    char* ImplType;
    char* ImplLocal;
    char* ImplIO;
    PyObject* prolog;
    PyObject* epilog;
    PyObject* container;
    PyObject* typeArgs;
    if (!PyArg_ParseTuple(args, "ssssssOOOO", &CESignature, &ImplSignature,
                         &ImplConstraints, &ImplType, &ImplLocal, &ImplIO, &prolog, &epilog, &container, &typeArgs)) {
        return NULL;
    }

    debug("- Core Element Signature: %s\n", CESignature);
    debug("- Implementation Signature: %s\n", ImplSignature);
    debug("- Implementation Constraints: %s\n", ImplConstraints);
    debug("- Implementation Type: %s\n", ImplType);
    debug("- Implementation Local: %s\n", ImplLocal);
    debug("- Implementation IO: %s\n", ImplIO);
    char** pro = new char*[3];
    char** epi = new char*[3];
    pro[0] = _pystring_to_char(PyList_GetItem(prolog, 0));
    pro[1] = _pystring_to_char(PyList_GetItem(prolog, 1));
    pro[2] = _pystring_to_char(PyList_GetItem(prolog, 2));
    epi[0] = _pystring_to_char(PyList_GetItem(epilog, 0));
    epi[1] = _pystring_to_char(PyList_GetItem(epilog, 1));
    epi[2] = _pystring_to_char(PyList_GetItem(epilog, 2));
    debug("- Prolog: %s %s\n", pro[0], pro[1]);
    debug("- Epilog: %s %s\n", epi[0], epi[1]);

    char** cont = new char*[3];
    cont[0] = _pystring_to_char(PyList_GetItem(container, 0));
    cont[1] = _pystring_to_char(PyList_GetItem(container, 1));
    cont[2] = _pystring_to_char(PyList_GetItem(container, 2));
    debug("- Container: %s %s\n", cont[0], cont[1], cont[2]);

    int num_params = PyList_Size(typeArgs);
    debug("- Implementation Type num args: %i\n", num_params);
    char** ImplTypeArgs = new char*[num_params];
    for(int i = 0; i < num_params; ++i) {
        ImplTypeArgs[i] = _pystring_to_char(PyList_GetItem(typeArgs, i));
        debug("- Implementation Type Args: %s\n", ImplTypeArgs[i]);
    }
    // Invoke the C library
    GS_RegisterCE(CESignature,
                  ImplSignature,
                  ImplConstraints,
                  ImplType,
                  ImplLocal,
                  ImplIO,
                  pro,
                  epi,
                  cont,
                  num_params,
                  ImplTypeArgs);
    debug("Core element registered\n");
    // Free all allocated memory
    delete ImplTypeArgs;
    Py_RETURN_NONE;
}

/*
  Method definition, generic argument speficication, and __doc__ field value
*/
static PyMethodDef CompssMethods[] = {
    { "error_out", (PyCFunction)error_out, METH_NOARGS, NULL},
    { "set_debug", set_debug, METH_VARARGS, "Set debug mode." },
    { "start_runtime", start_runtime, METH_VARARGS, "Start the COMPSs runtime." },
    { "stop_runtime", stop_runtime, METH_VARARGS, "Stop the COMPSs runtime." },
    { "cancel_application_tasks", cancel_application_tasks, METH_VARARGS, "Cancel all tasks of an application." },
    { "process_task", process_task, METH_VARARGS, "Process a task call from the application." },
    { "process_http_task", process_http_task, METH_VARARGS, "Process an http task call from the application." },
	{ "accessed_file", accessed_file, METH_VARARGS, "Check if a file has been already accessed. The file can contain an object." },
    { "open_file", open_file, METH_VARARGS, "Get a file for opening. The file can contain an object." },
    { "delete_file", delete_file, METH_VARARGS, "Delete a file." },
    { "close_file", close_file, METH_VARARGS, "Close a file." },
    { "get_file", get_file, METH_VARARGS, "Get last version of file with its original name." },
    { "get_directory", get_directory, METH_VARARGS, "Get last version of a directory with its original name." },
    { "barrier", barrier, METH_VARARGS, "Perform a barrier until the tasks already submitted have finished." },
    { "barrier_group", barrier_group, METH_VARARGS, "Barrier for a task group." },
    { "open_task_group", open_task_group, METH_VARARGS, "Opens a new task group." },
    { "close_task_group", close_task_group, METH_VARARGS, "Closes a new task group." },
	{ "cancel_task_group", cancel_task_group, METH_VARARGS, "Cancels a new task group." },
    { "snapshot", snapshot, METH_VARARGS, "Perform a snapshot of the tasks and data." },
    { "get_logging_path", get_logging_path, METH_VARARGS, "Requests the app log path." },
    { "get_master_working_path", get_master_working_path, METH_VARARGS, "Requests the master working path." },
    { "get_number_of_resources", get_number_of_resources, METH_VARARGS, "Requests the number of active resources." },
    { "request_resources", request_resources, METH_VARARGS, "Requests the creation of a new resource."},
    { "free_resources", free_resources, METH_VARARGS, "Requests the destruction of a resource."},
    { "register_core_element", register_core_element, METH_VARARGS, "Registers a task in the Runtime." },
	{ "emit_event", emit_event, METH_VARARGS, "Emit a event in the API Thread." },
	{ "set_pipes", set_pipes, METH_VARARGS, "Set compss module to pipe comunication mode." },
    { "read_pipes", read_pipes, METH_VARARGS, "Reads a command using the pipe comunication mode." },
	{ "set_wall_clock" , set_wall_clock, METH_VARARGS, "Set the application wall clock limit."},
    { NULL, NULL } /* sentinel */
};

extern "C" {

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
    PyMODINIT_FUNC PyInit_compss(void) {
        PyObject *module = PyModule_Create(&cModPy);

        if (module == NULL)
            INITERROR;
        struct module_state *st = GETSTATE(module);

        st->error = PyErr_NewException("compss.Error", NULL, NULL);
        if (st->error == NULL) {
            Py_DECREF(module);
            INITERROR;
        }

        return module;
    }
};
