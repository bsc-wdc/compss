/*
  Copyright 2.02-2017 Barcelona Supercomputing Center (www.bsc.es)

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
#include <Python.h>

/* ****************************************************************** */

#include <param_metadata.h>
#include <stdio.h>


struct list_int {
   void *val;
   struct list_el *next;
};


static PyObject *
start_runtime(PyObject *self, PyObject *args)
{
    //printf ("####C#### START\n");

    GS_On();

    return Py_BuildValue("i", 0);
}

static PyObject *
stop_runtime(PyObject *self, PyObject *args)
{
    //printf ("####C#### STOP\n");

    GS_Off();

    return Py_BuildValue("i", 0);
}

static PyObject *
process_task(PyObject *self, PyObject *args)
{
	//printf ("####C#### PROCESS TASK\n");

	long app_id = PyInt_AsLong(PyTuple_GetItem(args, 0));
	//printf ("####C####App id: %ld\n", app_id);
	char *path = PyString_AsString(PyTuple_GetItem(args, 1));
	//printf ("####C####Path: %s\n", path);
	char *func_name = PyString_AsString(PyTuple_GetItem(args, 2));
	//printf ("####C####Function name: %s\n", func_name);
	int priority = (int)PyInt_AsLong(PyTuple_GetItem(args, 3));
	//printf ("####C####Priority: %d\n", priority);
	int has_target = (int)PyInt_AsLong(PyTuple_GetItem(args, 4));
	//printf ("####C####Has target: %d\n", has_target);

	PyObject *values = PyList_AsTuple(PyTuple_GetItem(args, 5));
	PyObject *compss_types = PyList_AsTuple(PyTuple_GetItem(args, 6));
	PyObject *compss_directions = PyList_AsTuple(PyTuple_GetItem(args, 7));
	Py_ssize_t num_pars = PyTuple_Size(values);
	//printf ("####C####Num pars: %d\n", num_pars);

	PyObject *type, *val, *direction;

	Py_ssize_t j, pj;
    long l;
    int i;
    double d;
    char *s;

    void **params[num_pars * 3];
    int c_types[num_pars], c_directions[num_pars];
    char *c_values, *ini_c_values;

    int val_size = 0;

    // Get C types and directions
    for (j = 0; j < num_pars; j++) {
    	type = PyTuple_GetItem(compss_types, j); // this does not increment reference (we don't own it) so no need for decref
    	direction = PyTuple_GetItem(compss_directions, j);

    	c_types[j] = (int)PyInt_AsLong(type);
    	c_directions[j] = (int)PyInt_AsLong(direction);

    	//printf ("#### c_type: %d\n", c_types[j]);
    	//printf ("#### c_direction: %d\n", c_directions[j]);
    	switch ((enum datatype) c_types[j]) {
    	    case file_dt:
    	        //printf ("#### file_dt\n");
    	        val_size += sizeof(char*);
        	        break;
    	    case external_psco_dt:
    	        //printf ("#### external_psco_dt\n");
        		val_size += sizeof(char *);
        		break;
    	    case string_dt:
    	        //printf ("#### string_dt\n");
        	    val_size += sizeof(char*);
        		break;
    	    case int_dt:
    	        //printf ("#### int_dt\n");
    	    	val_size += sizeof(int);
    	        break;
    	    case long_dt:
    	        //printf ("#### long_dt\n");
    	    	val_size += sizeof(long);
    	        break;
    	    case double_dt:
    	        //printf ("#### double_dt\n");
    	    	val_size += sizeof(double);
    	        break;
    	    case boolean_dt:
    	        //printf ("#### boolean_dt\n");
    		val_size += sizeof(int);
    		break;
     	    default:
     	        //printf ("#### default\n");
     		break;
        }
    }

    //printf ("####C####Size of values: %d\n", val_size);

    // Build the C values
    //c_values = (char *)malloc(val_size);
    c_values = (char *)PyMem_Malloc(val_size); // allocate the memory in the Python heap
    ini_c_values = c_values;
    for (j = 0; j < num_pars; j++) {
    	pj = j * 3;
    	val = PyTuple_GetItem(values, j); // this does not increment reference (we don't own it) so no need for decref
    	params[pj] = (void *)c_values;
    	switch ((enum datatype) c_types[j]) {
            case file_dt:
    			s = PyString_AsString(val);
    			*(char**)c_values = s;
    			//printf ("####C#### \t Arg %d (FILE): %s, add %ld\n", j, *(char**)c_values, c_values);
    			c_values += sizeof(char*);
    			break;
	       case external_psco_dt:
                s = PyString_AsString(val);
                *(char**)c_values = s;
                //printf ("####C#### \t Arg %d (PERSISTENT): %s, add %ld\n", j, *(char**)c_values, c_values);
			    c_values += sizeof(char*);
			    break;
    	    case string_dt:
    	    	s = PyString_AsString(val);
    	    	*(char**)c_values = s;
    	    	//printf ("####C#### \t Arg %d (STRING): %s, add %ld\n", j, *(char**)c_values, c_values);
    	    	c_values += sizeof(char*);
    	        break;
    	    case int_dt:
    	    	i = (int)PyInt_AsLong(val);
    	    	*(int*)c_values = i;
    	    	//printf ("####C#### \t Arg %d (INT): %d, add %ld\n", j, *(int*)c_values, c_values);
    	    	c_values += sizeof(int);
    	        break;
    	    case long_dt:
    	    	l = PyLong_AsLong(val);
    	    	*(long*)c_values = l;
    	    	//printf ("####C#### \t Arg %d (LONG): %ld, add %ld\n", j, *(long*)c_values, c_values);
    	    	c_values += sizeof(long);
    	        break;
    	    case double_dt:
    	    	d = PyFloat_AsDouble(val);
				*(double*)c_values = d;
				//printf ("####C#### \t Arg %d (FLOAT): %f, add %ld\n", j, *(double *)c_values, c_values);
				c_values += sizeof(double);
				break;
    	    case boolean_dt:
    			i = (int)PyInt_AsLong(val);
    			*(int*)c_values = i;
    			//printf ("####C#### \t Arg %d (BOOL): %d, add %ld\n", j, *(int*)c_values, c_values);
    			c_values += sizeof(int);
    			break;
    		default:
    			break;
    	}
    	params[pj+1] = (void *)&c_types[j];
    	params[pj+2] = (void *)&c_directions[j];
    }

    // Invoke the C library
    GS_ExecuteTask(app_id,
    			   path, // class_name
    			   func_name, // method_name
    			   priority,
    			   has_target,
    			   (int)num_pars,
    			   params);

    //free(c_values);
    PyMem_Free(ini_c_values);

    Py_DECREF(values);
    Py_DECREF(compss_types);
    Py_DECREF(compss_directions);

    return Py_BuildValue("i", 0);
}

static PyObject *
get_file(PyObject *self, PyObject *args)
{
    //printf ("####C#### GET FILE\n");

    char *file_name = PyString_AsString(PyTuple_GetItem(args, 0));
    int mode = (int)PyInt_AsLong(PyTuple_GetItem(args, 1));

    char *compss_name;
    GS_Get_File(file_name, mode, &compss_name);

    //printf("####C#### COMPSs file name %s\n", compss_name);

    return Py_BuildValue("s", compss_name);
}


static PyObject *
delete_file(PyObject *self, PyObject *args)
{
    //printf ("####C#### DELETE FILE\n");

    char *file_name = PyString_AsString(PyTuple_GetItem(args, 0));
    int *result;
    GS_Delete_File(file_name, &result);

    //printf("####C#### COMPSs delete file name %s with result %i \n", (file_name, result));

    return Py_BuildValue("i", result);
}


static PyObject *
barrier(PyObject *self, PyObject *args)
{
    //printf ("####C#### BARRIER\n");

    long app_id = PyInt_AsLong(PyTuple_GetItem(args, 0));
    GS_Barrier(app_id);

    //printf("####C#### COMPSs barrier for AppId: %ld \n", (app_id));

    return Py_BuildValue("i", 0);
}


static PyObject *
get_logging_path(PyObject *self, PyObject *args)
{
    //printf ("####C#### GET LOG PATH\n");

	char *logPath;
	GS_Get_AppDir(&logPath);

	//printf("####C#### COMPSs log path %s\n", &logPath);
    return Py_BuildValue("s", logPath);

}

static PyObject *
set_constraints(PyObject *self, PyObject *args)
{
    //printf ("####C#### REGISTER CONSTRAINTS\n");

	long app_id = PyInt_AsLong(PyTuple_GetItem(args, 0));
    //printf ("####C####App id: %ld\n", app_id);
	char *func_module = PyString_AsString(PyTuple_GetItem(args, 1));
	//printf ("####C####Path: %s\n", func_module);
	char *func_name = PyString_AsString(PyTuple_GetItem(args, 2));
	//printf ("####C####Function name: %s\n", func_name);
	int has_target = (int)PyInt_AsLong(PyTuple_GetItem(args, 3));
	//printf ("####C####Has target: %d\n", has_target);
	int has_return = (int)PyInt_AsLong(PyTuple_GetItem(args, 4));
	//printf ("####C####Has return: %d\n", has_return);
	char *constraints = PyString_AsString(PyTuple_GetItem(args, 5));
	//printf ("####C####Constraints: %s\n", constraints);
	int parameterCount = (int)PyInt_AsLong(PyTuple_GetItem(args, 6));
	//printf ("####C####parameter Count: %d\n", parameterCount);

	PyObject *values = PyList_AsTuple(PyTuple_GetItem(args, 7));
	Py_ssize_t num_pars = PyTuple_Size(values);
	//printf ("####C####Num pars: %d\n", num_pars);

    void **params[num_pars]; // = 0;

	// Invoke the C library
	GS_RegisterCE(app_id,
			      func_module,
				  func_name,
				  has_target,
				  has_return,
				  parameterCount, //(int)num_pars,
				  params,
				  constraints);

	//printf("####C#### COMPSs CONSTRAINTS ALREADY SET\n");
    return Py_BuildValue("i", 0);

}

static PyMethodDef CompssMethods[] = {

    { "start_runtime", start_runtime, METH_VARARGS, "Start the COMPSs runtime." },

    { "stop_runtime", stop_runtime, METH_VARARGS, "Stop the COMPSs runtime." },

    { "process_task", process_task, METH_VARARGS, "Process a task call from the application." },

    { "get_file", get_file, METH_VARARGS, "Get a file for opening. The file can contain an object." },

    { "delete_file", delete_file, METH_VARARGS, "Delete a file." },

    { "barrier", barrier, METH_VARARGS, "Perform a barrier until the tasks already submitted have finished." },

    { "get_logging_path", get_logging_path, METH_VARARGS, "Requests the app log path." },

    { "set_constraints", set_constraints, METH_VARARGS, "Sets the task constraints." },

    { NULL, NULL, 0, NULL } /* sentinel */

};

PyMODINIT_FUNC
initcompss(void)
{
    (void) Py_InitModule("compss", CompssMethods);
}

