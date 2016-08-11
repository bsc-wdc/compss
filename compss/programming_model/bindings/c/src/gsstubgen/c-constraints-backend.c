#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <ctype.h>
#include "backend.h"
#include "semantic.h"
#include "backendlib.h"

static const char cvs_file_id[]="$Id: c-constraints-backend.c,v 1.2 2004/11/19 10:00:34 perez Exp $";


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


static FILE *constraintsFile = NULL;
static FILE *includeFile = NULL;
static FILE *wrapperFile = NULL;
static char includeName[PATH_MAX];

static char *c_types[] = { "void", "char", "char", "int", "void *", "short",
	"long","long long", "int", "float", "double", "file", "char *", "char *", "error" };


static void generate_c_constraints_wrapper_prototypes(FILE *wrapperFile)
{
	function *current_function;
	
	fprintf(wrapperFile, "// Prototypes\n");
	
	current_function = get_first_function();
	while (current_function != NULL) {
		fprintf(wrapperFile, "ClassAd %s_constraints_wrapper(char **_parameters, int OpId, int nparams);\n", current_function->name);
		fprintf(wrapperFile, "double %s_cost_wrapper(char **_parameters, int OpId, int nparams);\n", current_function->name);
		current_function = current_function->next_function;
	}
	
	fprintf(wrapperFile, "\n\n");
}

/*
static void generate_c_constraints_wrapper_tables(FILE *wrapperFile)
{
	function *current_function;
	
	fprintf(wrapperFile, "// Function tables\n");
	
	fprintf(wrapperFile, "constraints_wrapper constraints_functions[%i] = {\n", get_function_count());
	current_function = get_first_function();
	while (current_function != NULL) {
		fprintf(wrapperFile, "   %s_constraints_wrapper", current_function->name);
		if (current_function->next_function != NULL) {
			fprintf(wrapperFile, ",");
		}
		fprintf(wrapperFile, "\n");
		current_function = current_function->next_function;
	}
	fprintf(wrapperFile, "};\n");
	fprintf(wrapperFile, "\n");
	
	fprintf(wrapperFile, "cost_wrapper cost_functions[%i] = {\n", get_function_count());
	current_function = get_first_function();
	while (current_function != NULL) {
		fprintf(wrapperFile, "   %s_cost_wrapper", current_function->name);
		if (current_function->next_function != NULL) {
			fprintf(wrapperFile, ",");
		}
		fprintf(wrapperFile, "\n");
		current_function = current_function->next_function;
	}
	fprintf(wrapperFile, "};\n");
	fprintf(wrapperFile, "\n\n");
}
*/

static void generate_c_constraints_wrapper_pointers(FILE *wrapperFile)
{
	function *current_function;
	
	fprintf(wrapperFile, "// Pointers to function tables\n");	
	fprintf(wrapperFile, "constraints_wrapper * constraints_functions;\n");	
	fprintf(wrapperFile, "cost_wrapper * cost_functions;\n\n");
}

static void generate_c_contraints_wrapper_initConstraints_function(FILE *wrapperFile)
{
	function *current_function;
	int i;

	fprintf(wrapperFile, "void initConstraints()\n");
	fprintf(wrapperFile, "{\n");
	fprintf(wrapperFile, "   int Ops = %d;\n\n", get_function_count());

	fprintf(wrapperFile, "   // Allocate memory for function pointers\n");
	fprintf(wrapperFile, "   constraints_functions = new constraints_wrapper[Ops];\n");
	fprintf(wrapperFile, "   cost_functions = new cost_wrapper[Ops];\n\n");
	current_function = get_first_function();
	for ( i = 0; current_function != NULL; i++)
	{
		fprintf(wrapperFile, "   constraints_functions[%d] = &%s_constraints_wrapper;\n", i, current_function->name );
		fprintf(wrapperFile, "   cost_functions[%d] = &%s_cost_wrapper;\n", i, current_function->name );
		current_function = current_function->next_function;
	}
	fprintf(wrapperFile,"}\n");
	fprintf(wrapperFile,"\n\n");
}

static void generate_c_contraints_wrapper_finConstraints_function(FILE *wrapperFile)
{
	function *current_function;

	fprintf(wrapperFile, "void finConstraints()\n");
	fprintf(wrapperFile, "{\n");

	fprintf(wrapperFile, "   // Free memory allocated for function arrays\n");
	fprintf(wrapperFile, "   delete constraints_functions;\n");
	fprintf(wrapperFile, "   delete cost_functions;\n");

	fprintf(wrapperFile,"}\n");
	fprintf(wrapperFile,"\n\n");
}

void generate_c_constraints_prolog()
{
	char name[PATH_MAX];
	char *c;
	
	debug_printf("Generate C constraints prolog\n");
	strncpy(name, get_filename_base(), PATH_MAX);
	strncat(name, "_constraints.cc", PATH_MAX);
	constraintsFile = create_without_overwrite(name);
	if (constraintsFile == NULL) {
		fprintf(stderr, "Error: Could not open %s for writing.\n", name);
		exit(1);
	}

	strncpy(name, get_filename_base(), PATH_MAX);
	strncat(name, "_constraints_wrapper.cc", PATH_MAX);
	rename_if_clash(name);
	wrapperFile = fopen(name, "w");
	if (wrapperFile == NULL) {
		fprintf(stderr, "Error: Could not open %s for writing.\n", name);
		exit(1);
	}

	strncpy(includeName, get_filename_base(), PATH_MAX);
	strncat(includeName, "_constraints.h", PATH_MAX);
	rename_if_clash(includeName);
	includeFile = fopen(includeName, "w");
	if (includeFile == NULL) {
		fprintf(stderr, "Error: Could not open %s for writing.\n", includeName);
		exit(1);
	}
	
	fprintf(constraintsFile, "/* This file has been autogenerated from '%s'. */\n", get_filename());
	fprintf(constraintsFile, "/* Generator component: $Id: c-constraints-backend.c,v 1.2 2004/11/19 10:00:34 perez Exp $ */\n");
	fprintf(constraintsFile, "/* You can customize this file to your needs */\n");
	fprintf(constraintsFile, "\n");
	fprintf(constraintsFile, "static const char gs_generator[]=\"$Id: c-constraints-backend.c,v 1.2 2004/11/19 10:00:34 perez Exp $\";\n");
	fprintf(constraintsFile, "\n");
	fprintf(constraintsFile, "#include <string>\n");
	fprintf(constraintsFile, "#include \"%s\"\n", includeName);
	fprintf(constraintsFile, "\n\n");
	fprintf(constraintsFile, "using namespace std;\n");
	fprintf(constraintsFile, "\n\n");
	
	fprintf(wrapperFile, "/* This file has been autogenerated from '%s'. */\n", get_filename());
	fprintf(wrapperFile, "/* Generator component: $Id: c-constraints-backend.c,v 1.2 2004/11/19 10:00:34 perez Exp $ */\n");
	fprintf(wrapperFile, "/* CHANGES TO THIS FILE WILL BE LOST */\n");
	fprintf(wrapperFile, "\n");
	fprintf(wrapperFile, "static const char gs_generator[]=\"$Id: c-constraints-backend.c,v 1.2 2004/11/19 10:00:34 perez Exp $\";\n");
	fprintf(wrapperFile, "\n");
	fprintf(wrapperFile, "#include <stdio.h>\n");
	fprintf(wrapperFile, "#include <stdlib.h>\n");
	fprintf(wrapperFile, "#include <limits.h>\n");
	fprintf(wrapperFile, "#include <string.h>\n");
	fprintf(wrapperFile, "#include <string>\n");
	fprintf(wrapperFile, "#include <classad_distribution.h>\n");
	fprintf(wrapperFile, "#include <gs_base64.h>\n");
	fprintf(wrapperFile, "#include \"%s\"\n", includeName);
	fprintf(wrapperFile, "\n\n");
	fprintf(wrapperFile, "using namespace std;\n");
	fprintf(wrapperFile, "\n\n");
	fprintf(wrapperFile, "typedef ClassAd (*constraints_wrapper) (char **_parameters, int OpId, int nparams);\n");
	fprintf(wrapperFile, "typedef double (*cost_wrapper) (char **_parameters, int OpId, int nparams);\n");
	fprintf(wrapperFile, "\n\n");
	
	generate_c_constraints_wrapper_prototypes(wrapperFile);
	generate_c_constraints_wrapper_pointers(wrapperFile);
	generate_c_contraints_wrapper_initConstraints_function(wrapperFile);
	generate_c_contraints_wrapper_finConstraints_function(wrapperFile);

	fprintf(includeFile, "/* This file has been autogenerated from '%s'. */\n", get_filename());
	fprintf(includeFile, "/* Generator component: $Id: c-constraints-backend.c,v 1.2 2004/11/19 10:00:34 jorgee Exp $ */\n");
	fprintf(includeFile, "/* CHANGES TO THIS FILE WILL BE LOST */\n");
	fprintf(includeFile, "\n");
	fprintf(includeFile, "#ifndef _GSS_");
	for (c = includeName; *c; c++) {
		if (isalnum(*c)) {
			fprintf(includeFile, "%c", toupper(*c));
		} else {
			fprintf(includeFile, "_");
		}
	}
	fprintf(includeFile, "\n");
	
	fprintf(includeFile, "#define _GSS_");
	for (c = includeName; *c; c++) {
		if (isalnum(*c)) {
			fprintf(includeFile, "%c", toupper(*c));
		} else {
			fprintf(includeFile, "_");
		}
	}
	fprintf(includeFile, "\n");
	fprintf(includeFile, "\n");
	fprintf(includeFile, "#include <string>\n");
	fprintf(includeFile, "\n");
	fprintf(includeFile, "using namespace std;");
	fprintf(includeFile, "\n");
	fprintf(includeFile, "typedef char* file;\n");
	fprintf(includeFile, "\n");
	fprintf(includeFile, "/* Functions to be implemented in file '%s_constraints.cc'. */\n", get_filename_base());
	fprintf(includeFile, "\n");
}


void generate_c_constraints_epilogue(void)
{
	char *c;
	
	debug_printf("Generate C constraints epilogue\n");

	fprintf(includeFile, "\n");

	fprintf(includeFile, "#endif /* _GSS_");
	for (c = includeName; *c; c++) {
		if (isalnum(*c)) {
			fprintf(includeFile, "%c", toupper(*c));
		} else {
			fprintf(includeFile, "_");
		}
	}
	
	fprintf(includeFile, " */\n");
	
	fclose(constraintsFile);
	fclose(wrapperFile);
	fclose(includeFile);
}


static void generate_c_constraints_call_parameters(FILE *outFile, char *suffix, function *func)
{
	argument *arg;
	int is_first_arg = 1;
	
	arg = func->first_argument;
	while (arg != NULL) {
		if (arg->dir == in_dir || arg->dir == inout_dir) {
			if (!is_first_arg || ( !strcmp(suffix, "description" ) )) {
				fprintf(outFile, ", ");
			}
			fprintf(outFile, "%s", arg->name);
			is_first_arg = 0;
		}
		arg = arg->next_argument;
	}
}


static void generate_c_constraints_prototype(FILE *outFile, function *func, char *suffix, char *return_type)
{
	argument *arg;
	int is_first_arg = 1;

	fprintf(outFile, "%s %s_%s(", return_type, func->name, suffix);
	if ( !strcmp(suffix, "description" ) ){
                fprintf(outFile, "int *numCPUs, char **jobType ");
	}	
		arg = func->first_argument;
		while (arg != NULL) {
			if (arg->dir == in_dir || arg->dir == inout_dir) {
				if (!is_first_arg || ( !strcmp(suffix, "description" ) )) {
					fprintf(outFile, ", ");
				}
				fprintf(outFile, "%s ", c_types[arg->type]);
				fprintf(outFile, "%s", arg->name);
				is_first_arg = 0;
			}
			arg = arg->next_argument;
		}
	
	fprintf(outFile, ")");
}


static void generate_c_constraints_skeletons(FILE *constraintsFile, function *current_function)
{
		generate_c_constraints_prototype(constraintsFile, current_function, "constraints", "string");
		fprintf(constraintsFile, "\n");
		fprintf(constraintsFile, "{\n");
		fprintf(constraintsFile, "   return \"true\";\n");
		fprintf(constraintsFile, "}\n");
		fprintf(constraintsFile, "\n\n");
		
		generate_c_constraints_prototype(constraintsFile, current_function, "cost", "double");
		fprintf(constraintsFile, "\n");
		fprintf(constraintsFile, "{\n");
		fprintf(constraintsFile, "   return 1.0;\n");
		fprintf(constraintsFile, "}\n");
		fprintf(constraintsFile, "\n\n");
		
		generate_c_constraints_prototype(constraintsFile, current_function, "description", "void");
                fprintf(constraintsFile, "\n");
                fprintf(constraintsFile, "{\n");
                fprintf(constraintsFile, "   *numCPUs = 1;\n");
		fprintf(constraintsFile, "   *jobType = \"sequential\";\n");	
                fprintf(constraintsFile, "}\n");
                fprintf(constraintsFile, "\n\n");
}


static void generate_c_constraints_wrapper_function_prolog(FILE *outFile, function *func)
{
	argument *arg;
	
	fprintf(outFile, "   char **_argp;\n");
	fprintf(outFile, "\n");
	
	/* Declare generic buffers */
	fprintf(outFile, "   // Generic buffers\n");
	arg = func->first_argument;
	while (arg != NULL) {
		if (arg->dir == in_dir || arg->dir == inout_dir) {
			switch (arg->type) {
				case char_dt:
				case wchar_dt:
				case boolean_dt:
				case short_dt:
				case long_dt:
				case longlong_dt:
				case int_dt:
				case float_dt:
				case double_dt:
				case file_dt:
					fprintf(outFile, "   char *buff_%s;\n", arg->name);
					break;
				case string_dt:
					fprintf(outFile, "   char *base64buff_%s;\n", arg->name);
					break;
				case void_dt:
			case any_dt:
				case null_dt:
				default:
					;
			}
		}
		arg = arg->next_argument;
	}
	fprintf(outFile, "\n");
	
	/* Declare variables */
	fprintf(outFile, "   // Real parameters\n");
	arg = func->first_argument;
	while (arg != NULL) {
		if (arg->dir == in_dir || arg->dir == inout_dir) {
			switch (arg->type) {
				case char_dt:
				case wchar_dt:
					fprintf(outFile, "   char %s;\n", arg->name);
					break;
				case boolean_dt:
					fprintf(outFile, "   int %s;\n", arg->name);
					break;
				case short_dt:
					fprintf(outFile, "   short %s;\n", arg->name);
					break;
				case long_dt:
					fprintf(outFile, "   long %s;\n", arg->name);
					break;
				case longlong_dt:
                                        fprintf(outFile, "   long long %s;\n", arg->name);
                                        break;
				case int_dt:
					fprintf(outFile, "   int %s;\n", arg->name);
					break;
				case float_dt:
					fprintf(outFile, "   float %s;\n", arg->name);
					break;
				case double_dt:
					fprintf(outFile, "   double %s;\n", arg->name);
					break;
				case file_dt:
					fprintf(outFile, "   char *%s;\n", arg->name);
					break;
				case string_dt:
					fprintf(outFile, "   char *%s;\n", arg->name);
					break;
				case void_dt:
			case any_dt:
				case null_dt:
				default:
					;
			}
		}
		arg = arg->next_argument;
	}
	fprintf(outFile, "\n");
	
	fprintf(outFile, "   // Allocate buffers\n");
	/* Allocate generic buffers */
	arg = func->first_argument;
	while (arg != NULL) {
		if (arg->dir == in_dir || arg->dir == inout_dir) {
			switch (arg->type) {
				case char_dt:
				case wchar_dt:
				case boolean_dt:
				case short_dt:
				case long_dt:
				case longlong_dt:
				case int_dt:
				case float_dt:
				case double_dt:
				case file_dt:
					break; 
				case string_dt:
					fprintf(outFile, "   %s = (char *)malloc(atoi(getenv(\"GS_GENLENGTH\"))+1);\n", arg->name);
					break;
				case void_dt:
				case any_dt:
				case null_dt:
				default:
					;
			}
		}
		arg = arg->next_argument;
	}
	fprintf(outFile, "\n");
	
	/* Read varargs */
	fprintf(outFile, "   // Read parameters\n");
	fprintf(outFile, "   _argp = _parameters;\n");

	arg = func->first_argument;
	while (arg != NULL) {
		if ((arg->dir == in_dir || arg->dir == inout_dir) && arg->type == file_dt){
					fprintf(outFile, "   buff_%s = *(_argp++);\n", arg->name);
		}
		arg = arg->next_argument;
	}
	arg = func->first_argument;
        while (arg != NULL) {
                if ((arg->dir == in_dir || arg->dir == inout_dir) && arg->type != file_dt) {
                        switch (arg->type) {
                                case char_dt:
                                case wchar_dt:
                                        fprintf(outFile, "   buff_%s = *(_argp++);\n", arg->name);
                                        break;
                                case boolean_dt:
                                        fprintf(outFile, "   buff_%s = *(_argp++);\n", arg->name);
                                        break;
                                case short_dt:
                                        fprintf(outFile, "   buff_%s = *(_argp++);\n", arg->name);
                                        break;
                                case long_dt:
                                        fprintf(outFile, "   buff_%s = *(_argp++);\n", arg->name);
                                        break;
                                case longlong_dt:
                                        fprintf(outFile, "   buff_%s = *(_argp++);\n", arg->name);
                                        break;
                                case int_dt:
                                        fprintf(outFile, "   buff_%s = *(_argp++);\n", arg->name);
                                        break;
                                case float_dt:
                                case double_dt:
                                        fprintf(outFile, "   buff_%s = *(_argp++);\n", arg->name);
                                        break;
                                case file_dt:
                                        fprintf(outFile, "   buff_%s = *(_argp++);\n", arg->name);
                                        break;
                                case string_dt:
                                        fprintf(outFile, "   base64buff_%s = *(_argp++);\n", arg->name);
                                        break;
                                case void_dt:
                                case any_dt:
                                case null_dt:
                                default:
                                        ;
                        }
                }
                arg = arg->next_argument;
        }
	fprintf(outFile, "\n");

	fprintf(outFile, "   //Datatype conversion\n");
	arg = func->first_argument;
	while (arg != NULL) {
		if (arg->dir == in_dir || arg->dir == inout_dir) {
			switch (arg->type) {
				case char_dt:
				case wchar_dt:
					fprintf(outFile, "   %s = buff_%s[0];\n", arg->name, arg->name);
					break;
				case boolean_dt:
					fprintf(outFile, "   %s = atoi(buff_%s);\n", arg->name, arg->name);
					break;
				case short_dt:
					fprintf(outFile, "   %s = atoi(buff_%s);\n", arg->name, arg->name);
					break;
				case long_dt:
					fprintf(outFile, "   %s = atol(buff_%s);\n", arg->name, arg->name);
					break;
				case longlong_dt:
                                        fprintf(outFile, "   %s = atoll(buff_%s);\n", arg->name, arg->name);
                                        break;
				case int_dt:
					fprintf(outFile, "   %s = atoi(buff_%s);\n", arg->name, arg->name);
					break;
				case float_dt:
				case double_dt:
					fprintf(outFile, "   %s = strtod(buff_%s, NULL);\n", arg->name, arg->name);
					break;
				case file_dt:
					fprintf(outFile, "   %s = buff_%s;\n", arg->name, arg->name);
					break;
				case string_dt:
					fprintf(outFile, "   gs_b64_pton(base64buff_%s, %s, atoi(getenv(\"GS_GENLENGTH\"))+1);\n", arg->name, arg->name);
					break;
				case void_dt:
				case any_dt:
				case null_dt:
				default:
					;
			}
		}
		arg = arg->next_argument;
	}
	fprintf(outFile, "\n");
}


static void generate_c_constraints_wrapper_function_epilogue(FILE *outFile, function *func)
{
	argument *arg;
	
	/* Deallocate generic buffers */
	fprintf(outFile, "   // Free buffers\n");
	arg = func->first_argument;
	while (arg != NULL) {
		if ((arg->dir == in_dir || arg->dir == inout_dir) && arg->type != file_dt) {
			switch (arg->type) {
				case char_dt:
				case wchar_dt:
				case boolean_dt:
				case short_dt:
				case long_dt:
				case longlong_dt:
				case int_dt:
				case float_dt:
				case double_dt:
					break; 
				case string_dt:
					fprintf(outFile, "   free(%s);\n", arg->name);
					break;
				case void_dt:
				case any_dt:
				case null_dt:
				default:
					;
			}
		}
		arg = arg->next_argument;
	}
	fprintf(outFile, "\n");
}


static void generate_c_constraints_constraints_wrapper(FILE *outFile, function *func)
{
	fprintf(outFile, "ClassAd %s_constraints_wrapper(char **_parameters, int OpId, int nparams) {\n", func->name);
	generate_c_constraints_wrapper_function_prolog(outFile, func);
	fprintf(outFile, "   \n//Description function parameters\n");
	fprintf(outFile, "   int numCPUs;\n");
	fprintf(outFile, "   char *jobType;\n");
	fprintf(outFile, "   %s_description( &numCPUs, &jobType ", func->name);
	generate_c_constraints_call_parameters(outFile, "description", func);
 	fprintf(outFile, ");\n");
	fprintf(outFile, "   string _constraints = %s_constraints(", func->name);
	generate_c_constraints_call_parameters(outFile,"constraints", func);
	fprintf(outFile, ");\n");
	fprintf(outFile, "\n");
	
	fprintf(outFile, "   ClassAd _ad;\n");
	fprintf(outFile, "   ClassAdParser _parser;\n");
	fprintf(outFile, "   _ad.Insert(\"Requirements\", _parser.ParseExpression(_constraints));\n");
	fprintf(outFile, "   _ad.InsertAttr(\"numCPUs\", numCPUs);\n");
	fprintf(outFile, "   _ad.InsertAttr(\"jobType\", jobType);\n");	
	fprintf(outFile, "\n");
	
	generate_c_constraints_wrapper_function_epilogue(outFile, func);
	
	fprintf(outFile, "   return _ad;\n");
	fprintf(outFile, "}\n");
	fprintf(outFile, "\n\n");
}
	
	
static void generate_c_constraints_cost_wrapper(FILE *outFile, function *func)
{
	fprintf(outFile, "double %s_cost_wrapper(char **_parameters, int OpId, int nparams) {\n", func->name);
	generate_c_constraints_wrapper_function_prolog(outFile, func);
	
	fprintf(outFile, "   double _cost = %s_cost(", func->name);
	generate_c_constraints_call_parameters(outFile,"cost", func);
	fprintf(outFile, ");\n");
	fprintf(outFile, "\n");
	
	generate_c_constraints_wrapper_function_epilogue(outFile, func);
	
	fprintf(outFile, "   return _cost;\n");
	fprintf(outFile, "}\n");
	fprintf(outFile, "\n\n");
}
	
	
void generate_c_constraints_body(void)
{
	function *current_function;
	debug_printf("Generate C constraints body\n");
	
	current_function = get_first_function();
	while (current_function != NULL) {
		generate_c_constraints_prototype(includeFile, current_function, "constraints", "string");
		fprintf(includeFile, ";\n");
		generate_c_constraints_prototype(includeFile, current_function, "cost", "double");
		fprintf(includeFile, ";\n");
		generate_c_constraints_prototype(includeFile, current_function, "description", "void");
		fprintf(includeFile, ";\n");	
		generate_c_constraints_skeletons(constraintsFile, current_function);
		
		generate_c_constraints_constraints_wrapper(wrapperFile, current_function);
		generate_c_constraints_cost_wrapper(wrapperFile, current_function);
		
		current_function = current_function->next_function;
	}
}



