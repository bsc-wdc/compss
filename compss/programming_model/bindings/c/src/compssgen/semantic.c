/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "semantic.h"

#define DEBUG 1

#if 0 /* Unsupported on AIX */
#ifdef DEBUG
#define debug_printf(fmt, args...) printf(fmt, ## args)
#else
#define debug_printf(args...) {}
#endif
#endif

#ifndef DEBUG
#define DEBUG 0
#endif
#define debug_printf if (DEBUG) printf

#define TRUE 1
#define FALSE 0

#define unalloc(x) if (x!=NULL) {free(x); x=NULL;}


extern int line;

static interface *main_interface = NULL;
static function *current_function = NULL;
static argument *current_argument = NULL;
static constraints *current_constraints = NULL;
static char *current_implements = NULL;
static int has_errors = 0;
static int function_count = 0;
static char *current_function_name = NULL;
static int parsing_args = 0;
static char const *filename = NULL;
static include *current_include = NULL;
static include *first_include = NULL;

int exists_function(char *name) {
    function *curr;
    debug_printf("Exists function\n");
    assert(main_interface != NULL);
    curr = main_interface->first_function;
    while (curr != NULL) {
        if (strcmp(curr->name, name) == 0) {
            return TRUE;
        }
        curr = curr->next_function;
    }
    return FALSE;
}


int exists_argument(char *name) {
    argument *curr;
    debug_printf("Exists argument\n");
    assert(current_function != NULL);
    curr = current_function->first_argument;
    while (curr != NULL) {
        if (strcmp(curr->name, name) == 0) {
            return TRUE;
        }
        curr = curr->next_argument;
    }
    return FALSE;
}

void add_header(char* name) {
    debug_printf("Adding header %s\n", name);

    if (current_include == NULL) {
        current_include = (include*) malloc(sizeof(include));
    }
    else {
        current_include->next_include = (include*) malloc(sizeof(include));
        current_include = current_include->next_include;
    }
    current_include->name = name;

    if (first_include == NULL) {
        first_include = current_include;
    }
}

void begin_interface(char *interface_name) {
    debug_printf("Begin interface %s\n", interface_name);
    if (main_interface != NULL) {
        fprintf(stderr, "%s:%i: Only one interface allowed\n",
                get_filename(), line);
        has_errors = 1;
    }
    main_interface = (interface *)malloc(sizeof(interface));
    main_interface->name = strdup(interface_name);
    main_interface->first_function = NULL;
    current_function = NULL;
    current_argument = NULL;
}


void end_interface() {
    debug_printf("End interface\n");
}


void begin_function(char *fn_name) {
    function *new_function;
    debug_printf("Begin function %s\n", fn_name);
    assert(main_interface != NULL);
    if (exists_function(fn_name)) {
        fprintf(stderr, "%s:%i: Duplicated function name '%s'\n",
                get_filename(), line, fn_name);
        has_errors = 1;
    }
    new_function = (function *)malloc(sizeof(function));
    new_function->name = strdup(fn_name);
    new_function->classname = strdup(fn_name);
    char *p = strrchr(new_function->classname, ':');
    if (p != NULL) {
        p++;
        new_function->methodname = strdup(p);
        p--;
        p--;
        *p = '\0';
    } else {
        new_function->methodname = strdup(fn_name);
        new_function->classname = NULL;
    }
    new_function->return_type = null_dt;
    new_function->first_argument = NULL;
    new_function->argument_count = 0;
    new_function->exec_arg_count = 0;
    new_function->next_function = NULL;
    if (current_function != NULL) {
        current_function->next_function = new_function;
    } else {
        main_interface->first_function = new_function;
    }
    current_function = new_function;
    current_function_name = new_function->name;
    current_function->constraints = current_constraints;
    current_function->implements_name = current_implements;
    current_constraints = NULL;
    current_implements = NULL;
    current_argument = NULL;
    function_count++;
}

void add_static(int val) {
    current_function->access_static = val;
}

void add_return_type(enum datatype return_type, char *return_typename, char* elements) {
    debug_printf("Add return type %i (%s, %s)\n", return_type, return_typename, elements);
    assert(current_function != NULL);
    assert(current_function->return_type == null_dt);
    /*
    if (return_type != void_dt) {
    	fprintf(stderr, "%s:%i: Invalid return type in function '%s'\n",
    		get_filename(), line, get_current_function_name());
    	has_errors = 1;
    }
    */
    current_function->return_type = return_type;
    current_function->return_typename = strdup(return_typename);
    if (elements !=NULL) {
        current_function->return_elements = strdup(elements);
        switch (return_type) {
        case char_dt:
        case wchar_dt:
            debug_printf("Change return type to %i\n", array_char_dt);
            current_function->return_type = array_char_dt;
            current_function->return_typename = "char";
            break;
        case short_dt:
            debug_printf("Change return type to %i\n", array_short_dt);
            current_function->return_type = array_short_dt;
            current_function->return_typename = "short";
            break;
        case long_dt:
            debug_printf("Change return type to %i\n", array_long_dt);
            current_function->return_type = array_long_dt;
            current_function->return_typename = "long";
            break;
        case int_dt:
            debug_printf("Change return type to %i\n", array_int_dt);
            current_function->return_type = array_int_dt;
            current_function->return_typename = "int";
            break;
        case float_dt:
            debug_printf("Change return type to %i\n", array_float_dt);
            current_function->return_type = array_float_dt;
            current_function->return_typename = "float";
            break;
        case double_dt:
            debug_printf("Change return type to %i\n", array_double_dt);
            current_function->return_type = array_double_dt;
            current_function->return_typename = "double";
            break;
        default:
            ;
        }
    } else {
        current_function->return_elements ="0";
    }
}


char* build_signature(const char *method, const char *classname, const argument *args) {

    if (!method) return NULL;

    /* ---- First pass: compute required length ---- */
    size_t len = strlen(method) + 2 + 1; // method + "(" + ")" + '\0'

    const argument *arg = args;
    int count = 0;

    while (arg) {
        if (arg->signature_type) {
            len += strlen(arg->signature_type);
            count++;
        }
        arg = arg->next_argument;
    }

    if (count > 1) {
        len += (count - 1); // commas
    }

    if (classname) {
        len += strlen(classname);
    }

    /* ---- Allocate ---- */
    char *signature = malloc(len);
    if (!signature) return NULL;

    /* ---- Build string ---- */
    size_t pos = 0;

    pos += sprintf(signature + pos, "%s(", method);

    arg = args;
    int written = 0;

    while (arg) {
        if (arg->signature_type) {
            if (written > 0) {
                signature[pos++] = ',';
            }

            size_t tlen = strlen(arg->signature_type);
            memcpy(signature + pos, arg->signature_type, tlen);
            pos += tlen;

            written++;
        }
        arg = arg->next_argument;
    }

    signature[pos++] = ')';

    if (classname) {
        size_t clen = strlen(classname);
        memcpy(signature + pos, classname, clen);
        pos += clen;
    }

    signature[pos] = '\0';

    return signature;
}

void end_function() {
    debug_printf("End function\n");
    assert(current_function != NULL);
    current_function->ce_signature = strdup(build_signature(current_function->implements_name ? current_function->implements_name : current_function->name, "", current_function->first_argument));
    current_function->impl_signature = strdup(build_signature(current_function->name, current_function->classname, current_function->first_argument));

    current_function->polymorphism_next = current_function;
    function *polymorphism_target = main_interface->first_function;
    while (polymorphism_target != current_function) {
        if (strcmp(polymorphism_target->ce_signature, current_function->ce_signature) == 0) {
            function *swap = polymorphism_target->polymorphism_next;
            polymorphism_target->polymorphism_next = current_function;
            current_function->polymorphism_next = swap;
            break;
        }
        polymorphism_target = polymorphism_target->next_function;
    }
    current_function_name = NULL;
}

void add_implements(char *function_name) {
    debug_printf("Add implements %s\n", function_name);
    current_implements = strdup(function_name);
}


void begin_constraints(){
    printf("Parsing constraints \n");
    current_constraints = (constraints *)malloc(sizeof(constraints));
    current_constraints->first_processor = NULL;
    current_constraints->current_processor = NULL;
    current_constraints->first_property = NULL;
    current_constraints->current_property = NULL;
}

void begin_processors(){
    printf("Parsing processors \n");
}

void begin_processor() {
    printf("Parsing processor \n");
    processor *new_processor = (processor *)malloc(sizeof(processor));
    new_processor->first_property = NULL;
    new_processor->next_processor = NULL;
    if (current_constraints->current_processor != NULL) {
        current_constraints->current_processor->next_processor = new_processor;
    } else {
        current_constraints->first_processor = new_processor;
    }
    current_constraints->current_processor = new_processor;

}

void add_processor_param(char *key, char *value) {
    printf("Adding processor param %s:%s \n", key, value);
    property *new_property;
    assert(current_constraints->current_processor != NULL);
    new_property = (property *)malloc(sizeof(property));
    new_property->name = strdup(key);
    new_property->value = strdup(value);
    new_property->next_property = NULL;
    processor *current_processor = current_constraints->current_processor;
    if (current_processor->current_property != NULL) {
        current_processor->current_property->next_property = new_property;
    } else {
        current_processor->first_property = new_property;
    }
    current_processor->current_property = new_property;
}

void end_processor(){
    printf("End processor \n");
}

void end_processors(){
    printf("End processors \n");
}

void add_constraint(char *key, char *value){
    printf("Adding constraint %s:%s \n", key, value);
    property *new_property;

    new_property = (property *)malloc(sizeof(property));
    new_property->name=strdup(key);
    new_property->value=strdup(value);
    new_property->next_property = NULL;
    if (current_constraints->current_property != NULL) {
        current_constraints->current_property->next_property = new_property;
    } else {
        current_constraints->first_property = new_property;
    }
    current_constraints->current_property = new_property;
}

void end_constraints(){
    printf("End constraints \n");
}

char const* get_current_function_name() {
    return current_function_name;
}

void argument_set_type(char* elements, enum datatype dt, char *classname, char *signature, enum datatype array_dt, argument *new_argument) {
    if (elements !=NULL) {
            new_argument->elements = strdup(elements);
            new_argument->type = array_dt;
            new_argument->classname = classname;
            new_argument->signature_type = "BINDING_OBJECT_T";
        } else {
            new_argument->elements = "0";
            new_argument->type = dt;
            new_argument->signature_type = signature;
        }
        new_argument->classname = classname;
}

void add_argument(enum direction dir, enum datatype dt, char *classname, char *name, char* elements) {
    argument *new_argument;
    debug_printf("Add argument %i %i %s %s %s \n", dir, dt, classname, name, elements);
    assert(current_function != NULL);
    if (exists_argument(name)) {
        fprintf(stderr, "%s:%i: Duplicated argument name '%s' in function '%s'\n",
                get_filename(), line, name, get_current_function_name());
        has_errors = 1;
        return;
    }
    if (dt == void_dt) {
        fprintf(stderr, "%s:%i: Invalid parameter type for argument %i in function '%s'\n",
                get_filename(), line, get_next_argnum(), get_current_function_name());
        has_errors = 1;
        return;
    }
    new_argument = (argument *)malloc(sizeof(argument));
    new_argument->name = strdup(name);
    switch (dt) {
    case char_dt:
    case wchar_dt:
        argument_set_type(elements, dt, "char", "CHAR_T", array_char_dt, new_argument);
        break;
    case boolean_dt:
        new_argument->elements = "0";
        new_argument->type = dt;
        new_argument->classname = "int";
        new_argument->signature_type = "BOOLEAN_T";
        break;
    case short_dt:
        argument_set_type(elements, dt, "short", "SHORT_T", array_short_dt, new_argument);
        break;
    case long_dt:
        argument_set_type(elements, dt, "long", "LONG_T", array_long_dt, new_argument);
        break;
    case longlong_dt:
        new_argument->elements = "0";
        new_argument->type = dt;
        new_argument->classname = "long long";
        new_argument->signature_type = "FLOAT_T";
        break;
    case int_dt:
        argument_set_type(elements, dt, "int", "INT_T", array_int_dt, new_argument);
        break;
    case float_dt:
        argument_set_type(elements, dt, "float", "FLOAT_T", array_float_dt, new_argument);
        break;
    case double_dt:
        argument_set_type(elements, dt, "double", "DOUBLE_T", array_double_dt, new_argument);
        break;
    case object_dt:
        new_argument->elements = "0";
        new_argument->type = dt;
        new_argument->classname = strdup(classname);
        new_argument->signature_type = "BINDING_OBJECT_T";
        break;
    case string_dt:
    case string_64_dt:
    case wstring_dt:
        new_argument->elements = "0";
        new_argument->type = dt;
        new_argument->classname = "string";
        new_argument->signature_type = "STRING_T";
        break;
    case file_dt:
        new_argument->elements = "0";
        new_argument->classname = "File";
        new_argument->type = dt;
        new_argument->signature_type = "FILE_T";
        break;
    case enum_dt:
        new_argument->elements = "0";
        new_argument->type = enum_dt;
        new_argument->classname = strdup(classname);
        new_argument->signature_type = "ENUM_T";
        break;
    case null_dt:
        new_argument->elements = "0";
        new_argument->type = dt;
        new_argument->signature_type = "NULL_T";
    case void_dt:
    case any_dt:
    default:
        new_argument->elements = "0";
        new_argument->type = dt;
    }
    new_argument->dir = dir;
    new_argument->passing_in_order = 0;
    new_argument->passing_out_order = 0;
    new_argument->next_argument = NULL;

    if (current_argument != NULL) {
        current_argument->next_argument = new_argument;
    } else {
        current_function->first_argument = new_argument;
    }
    current_argument = new_argument;
    current_function->argument_count++;
    switch (dir) {
    case in_dir:
    case out_dir:
        current_function->exec_arg_count++;
        break;
    case inout_dir:
        current_function->exec_arg_count += 2;
        break;
    default:
        break;
    }
}


int can_generate() {
    return !has_errors;
}


function *get_first_function() {
    assert(main_interface != NULL);
    return main_interface->first_function;
}

include* get_first_include() {
    return first_include;
}

interface *get_main_interface() {
    return main_interface;
}


int get_function_count() {
    return function_count;
}


void begin_arguments() {
    parsing_args = 1;
}


void end_arguments() {
    parsing_args = 0;
}


int get_next_argnum() {
    return current_function->argument_count + 1;
}


int began_arguments() {
    return parsing_args;
}


void set_serious_error() {
    has_errors = 1;
}


void set_filename(char *fn) {
    filename = strdup(fn);
}


char const* get_filename() {
    return filename;
}


