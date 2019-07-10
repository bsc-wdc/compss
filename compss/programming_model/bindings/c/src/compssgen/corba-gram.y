%{
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "param_metadata.h"
#include "semantic.h"

#if 0
#define YYERROR_VERBOSE
#endif
#define YYERROR_VERBOSE

int yylex(void);
void yyerror(char *s);
%}

%union {
    char		*elements;
	char		*name;
	char		*classname;
	enum datatype	dtype;
	enum direction	dir;
}

%token TOK_INTERFACE TOK_LEFT_CUR_BRAKET TOK_RIGHT_CUR_BRAKET TOK_LEFT_PARENTHESIS TOK_LEFT_BRAKET TOK_RIGHT_BRAKET
%token TOK_RIGHT_PARENTHESIS TOK_COMMA TOK_SEMICOLON TOK_IN TOK_OUT TOK_INOUT TOK_FILE
%token TOK_STATIC TOK_UNSIGNED TOK_VOID TOK_SHORT TOK_LONG TOK_LONGLONG TOK_INT TOK_FLOAT TOK_DOUBLE TOK_CHAR
%token TOK_WCHAR TOK_BOOLEAN TOK_STRING TOK_WSTRING TOK_ANY
%token TOK_ERROR
%token TOK_EQUAL TOK_DBLQUOTE
%token TOK_ENUM TOK_INCLUDE 

%token <name> TOK_IDENTIFIER TOK_HEADER 
%token <elements> NUMBER
%type <dtype> data_type numeric_type array_type enum_type
%type <dir> direction

%%

start:		/* Empty */
		| start includes interface
;

includes: 
        | includes TOK_INCLUDE TOK_HEADER { add_header($3); } TOK_SEMICOLON
;

interface: TOK_INTERFACE TOK_IDENTIFIER { begin_interface($2); } TOK_LEFT_CUR_BRAKET 
		prototypes
		TOK_RIGHT_CUR_BRAKET TOK_SEMICOLON { end_interface(); }
;

prototypes:	/* Empty */
		| prototypes prototype 
;

prototype: data_type TOK_IDENTIFIER {  begin_function($2); add_static(0); add_return_type($1, "", NULL); } TOK_LEFT_PARENTHESIS { begin_arguments(); } arguments0 { end_arguments(); }	TOK_RIGHT_PARENTHESIS { end_function(); } TOK_SEMICOLON
		| array_type TOK_LEFT_BRAKET TOK_IDENTIFIER TOK_RIGHT_BRAKET TOK_IDENTIFIER {  begin_function($5); add_static(0); add_return_type($1, "", $3); } TOK_LEFT_PARENTHESIS { begin_arguments(); } arguments0 { end_arguments(); }	TOK_RIGHT_PARENTHESIS { end_function(); } TOK_SEMICOLON
		| array_type TOK_LEFT_BRAKET NUMBER TOK_RIGHT_BRAKET TOK_IDENTIFIER {  begin_function($5); add_static(0); add_return_type($1, "", $3); } TOK_LEFT_PARENTHESIS { begin_arguments(); } arguments0 { end_arguments(); }	TOK_RIGHT_PARENTHESIS { end_function(); } TOK_SEMICOLON
		| TOK_IDENTIFIER TOK_IDENTIFIER { begin_function($2); add_static(0); add_return_type(object_dt, $1, NULL); } TOK_LEFT_PARENTHESIS { begin_arguments(); } arguments0 { end_arguments(); }	TOK_RIGHT_PARENTHESIS { end_function(); } TOK_SEMICOLON
		| TOK_STATIC TOK_IDENTIFIER TOK_IDENTIFIER { begin_function($3); add_static(1); add_return_type(object_dt, $2, NULL); } TOK_LEFT_PARENTHESIS { begin_arguments(); } arguments0 { end_arguments(); }	TOK_RIGHT_PARENTHESIS { end_function(); } TOK_SEMICOLON
		| TOK_STATIC array_type TOK_LEFT_BRAKET TOK_IDENTIFIER TOK_RIGHT_BRAKET TOK_IDENTIFIER { begin_function($6); add_static(1); add_return_type($2, "", $4); } TOK_LEFT_PARENTHESIS { begin_arguments(); } arguments0 { end_arguments(); }   TOK_RIGHT_PARENTHESIS { end_function(); } TOK_SEMICOLON
		| TOK_STATIC array_type TOK_LEFT_BRAKET NUMBER TOK_RIGHT_BRAKET TOK_IDENTIFIER { begin_function($6); add_static(1); add_return_type($2, "", $4); } TOK_LEFT_PARENTHESIS { begin_arguments(); } arguments0 { end_arguments(); }   TOK_RIGHT_PARENTHESIS { end_function(); } TOK_SEMICOLON
		| TOK_STATIC data_type TOK_IDENTIFIER { begin_function($3); add_static(1); add_return_type($2, "", NULL); } TOK_LEFT_PARENTHESIS { begin_arguments(); } arguments0 { end_arguments(); }   TOK_RIGHT_PARENTHESIS { end_function(); } TOK_SEMICOLON
		| error TOK_SEMICOLON
;


arguments0:	/* Empty */
		| arguments1
;


arguments1:	argument
		| arguments1 TOK_COMMA argument
		| arguments1 TOK_COMMA error
		| error
;

argument:	direction data_type TOK_IDENTIFIER { add_argument($1, $2, "", $3, NULL); }
		|	direction array_type TOK_LEFT_BRAKET TOK_IDENTIFIER TOK_RIGHT_BRAKET TOK_IDENTIFIER { add_argument($1, $2, "", $6, $4);}
		|	direction array_type TOK_LEFT_BRAKET NUMBER TOK_RIGHT_BRAKET TOK_IDENTIFIER { add_argument($1, $2, "", $6, $4);}
		|	direction TOK_IDENTIFIER TOK_IDENTIFIER { add_argument($1, object_dt, $2, $3, NULL); }
        |   direction enum_type TOK_IDENTIFIER TOK_IDENTIFIER { add_argument($1, $2, $3, $4, NULL); }
;

direction:	TOK_IN			{ $$ = in_dir; }
		| TOK_OUT		{ $$ = out_dir; }
		| TOK_INOUT		{ $$ = inout_dir; }
;

data_type:	TOK_UNSIGNED numeric_type	{ $$ = $2; }
		| numeric_type		{ $$ = $1; }
		| TOK_CHAR			{ $$ = char_dt; }
		| TOK_WCHAR			{ $$ = wchar_dt; }
		| TOK_BOOLEAN		{ $$ = boolean_dt; }
		| TOK_STRING		{ $$ = string_dt; }
		| TOK_WSTRING		{ $$ = wstring_dt; }
		| TOK_ANY			{ $$ = any_dt; }
		| TOK_VOID			{ $$ = void_dt; }
		| TOK_FILE			{ $$ = file_dt; }
;

enum_type: TOK_ENUM { $$ = enum_dt; }
;

numeric_type:	TOK_SHORT	{ $$ = short_dt; }
		| TOK_LONG		{ $$ = long_dt; }
		| TOK_LONGLONG  { $$ = longlong_dt; }
		| TOK_INT		{ $$ = int_dt; }
		| TOK_FLOAT		{ $$ = float_dt; }
		| TOK_DOUBLE	{ $$ = double_dt; }
;

array_type: TOK_SHORT	{ $$ = short_dt; }
		| TOK_LONG		{ $$ = long_dt; }
		| TOK_LONGLONG  { $$ = longlong_dt; }
		| TOK_INT		{ $$ = int_dt; }
		| TOK_FLOAT		{ $$ = float_dt; }
		| TOK_DOUBLE	{ $$ = double_dt; }
		| TOK_CHAR      { $$ = char_dt; }
;

%%

extern int line;


void yyerror(char *s)
{
	char const *function_name;
	
	fprintf(stderr, "%s:%i: ", get_filename(), line);
	fprintf(stderr, "%s", s);
	function_name = get_current_function_name();
	if (function_name != NULL) {
		fprintf(stderr, " in function '%s'", function_name);
		if (began_arguments) {
			fprintf(stderr, " parameter %i", get_next_argnum());
		}
	}
	fprintf(stderr,"\n");
	set_serious_error();
}
