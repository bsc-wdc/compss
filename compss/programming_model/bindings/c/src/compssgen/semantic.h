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
#ifndef SEMANTIC_H
#define SEMANTIC_H

#include <param_metadata.h>

typedef struct function function;
typedef struct interface interface;
typedef struct argument argument;
typedef struct constraint constraint;
typedef struct include include;

struct argument {
    char *name;
    char *classname;
    enum datatype	type;
    enum direction	dir;
    enum io_stream     stream;
    int passing_in_order;
    int passing_out_order;
    char *elements;
    argument *next_argument;
};

struct constraint {
    char *name;
    constraint *next_constraint;
};

struct include {
    char *name; //Name of the header to include
    include *next_include;
};

struct function {
    char *name;
    int access_static;
    char *methodname;
    char *classname;
    char *return_typename;
    enum datatype return_type;
    char *return_elements;
    argument *first_argument;
    int argument_count;
    int exec_arg_count;
    constraint *first_constraint;
    function *next_function;
};

struct interface {
        char *name;
        function *first_function;
};

void add_header(char* name);
void add_static(int val);
void begin_interface(char *interface_name);
void end_interface();
void begin_function(char *function_name);
void add_return_type(enum datatype return_type, char *return_typename, char* return_elements);
void end_function();
char const* get_current_function_name();
void begin_arguments();
void begin_constraints();
void add_constraint(char *constraint);

void end_arguments();
int began_arguments();
int get_next_argnum();
void add_argument(enum direction dir, enum datatype dt, char *classname, char *name, char *elements);
int can_generate();
function *get_first_function();
include *get_first_include();
interface *get_main_interface();
int get_function_count();
void set_serious_error();
void set_filename(char *fn);
char const* get_filename();


#endif /* SEMANTIC_H */
