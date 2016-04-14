

#ifndef SEMANTIC_H
#define SEMANTIC_H

#include <param_metadata.h>

typedef struct function function;
typedef struct interface interface;
typedef struct argument argument;
typedef struct constraint constraint;


struct argument
{
	char *name;
	char *classname;
	enum datatype	type;
	enum direction	dir;
	int passing_in_order;
	int passing_out_order;
	argument *next_argument;
};

struct constraint
{
	char *name;
	constraint *next_constraint;
};

struct function
{
	char *name;
	int access_static;
	char *methodname;
	char *classname;
	char *return_typename;
	enum datatype return_type;
	argument *first_argument;
	int argument_count;
	int exec_arg_count;
	constraint *first_constraint;
	function *next_function;
}; 

struct interface
{
	char *name;
	function *first_function;
};

void add_static(int val);
void begin_interface(char *interface_name);
void end_interface();
void begin_function(char *function_name);
void add_return_type(enum datatype return_type, char *return_typename);
void end_function();
char const* get_current_function_name();
void begin_arguments();
void begin_constraints();
void add_constraint(char *constraint);

void end_arguments();
int began_arguments();
int get_next_argnum();
void add_argument(enum direction dir, enum datatype dt, char *classname, char *name);
int can_generate();
function *get_first_function();
interface *get_main_interface();
int get_function_count();
void set_serious_error();
void set_filename(char *fn);
char const* get_filename();


#endif /* SEMANTIC_H */
