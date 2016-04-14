

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "semantic.h"

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
static constraint *current_constraint = NULL;
static int has_errors = 0;
static int function_count = 0;
static char *current_function_name = NULL;
static int parsing_args = 0;
static char const *filename = NULL;


int exists_function(char *name)
{
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


int exists_argument(char *name)
{
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


void begin_interface(char *interface_name)
{
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


void end_interface()
{
	debug_printf("End interface\n");
}

		
void begin_function(char *fn_name)
{
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
	current_argument = NULL;
	current_constraint = NULL;
	function_count++;
	current_function_name = new_function->name;
}

void add_static(int val) {
	current_function->access_static = val;
}

void add_return_type(enum datatype return_type, char *return_typename)
{
	debug_printf("Add return type %i\n", return_type);
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
}

void end_function()
{
	debug_printf("End function\n");
	assert(current_function != NULL);
	current_function_name = NULL;
}


char const* get_current_function_name()
{
	return current_function_name;
}


void add_argument(enum direction dir, enum datatype dt, char *classname, char *name)
{
	argument *new_argument;
	debug_printf("Add argument %i %i %s %s\n", dir, dt, classname, name);
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
			 new_argument->classname = "char";
			 break;
		 case boolean_dt:
			 new_argument->classname = "int";
			 break;
		 case short_dt:
			 new_argument->classname = "short";
			 break;
		 case long_dt:
			 new_argument->classname = "long";
			 break;
		 case longlong_dt:
			 new_argument->classname = "long long";
			 break;
		 case int_dt:
			 new_argument->classname = "int";
			 break;
		 case float_dt:
			 new_argument->classname = "float";
			 break;
		 case double_dt:
			 new_argument->classname = "double";
			 break;
		 case object_dt:
			 new_argument->classname = strdup(classname);
			 break;
		 case string_dt:
		 case wstring_dt:
			 new_argument->classname = "string";
			 break;
		 case file_dt:
			 new_argument->classname = "File";
			 break;
		 case void_dt:
		 case any_dt:
		 case null_dt:
		 default:;
	 }

	new_argument->type = dt;
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


int can_generate()
{
	return !has_errors;
}


function *get_first_function()
{
	assert(main_interface != NULL);
	return main_interface->first_function;
}


interface *get_main_interface()
{
	return main_interface;
}


int get_function_count()
{
	return function_count;
}


void begin_arguments()
{
	parsing_args = 1;
}

void begin_constraints()
{
	//printf("constraint:\n");
}

void add_constraint(char *constr)
{
	printf("add constraint %s to function %s \n", constr, current_function->name);
	constraint *new_constraint;
	assert(current_function != NULL);
	
	new_constraint = (constraint *)malloc(sizeof(constraint));
	new_constraint->name=strdup(constr);
	new_constraint->next_constraint = NULL;
	if(current_constraint != NULL) {
		current_constraint->next_constraint = new_constraint;
	} else {
		current_function->first_constraint = new_constraint;
	}
	current_constraint = new_constraint;
}


void end_arguments()
{
	parsing_args = 0;
}


int get_next_argnum()
{
	return current_function->argument_count + 1;
}


int began_arguments()
{
	return parsing_args;
}


void set_serious_error()
{
	has_errors = 1;
}


void set_filename(char *fn)
{
	filename = strdup(fn);
}


char const* get_filename()
{
	return filename;
}


