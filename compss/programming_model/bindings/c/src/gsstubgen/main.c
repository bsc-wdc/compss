
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "backend.h"
#include "semantic.h"
#include "backendlib.h"

static const char cvs_file_id[]="$Id: main.c,v 1.5 2004/11/19 10:00:35 perez Exp $";



extern FILE *yyin;
int yyparse (void);
char *filename;


static void print_help(char *command)
{       
        printf("Usage: %s [-splxn] [-P<perl-binary>] <input file>\n", command);
        printf("   The output files are generated according to the input filename.\n");
        printf("   -s  Generate swig files.\n");
        printf("   -p  Generate perl files.\n");
        printf("   -l  Generate shell script files.\n");
        printf("   -j  Generate java files.\n");
        printf("   -P  Specify which perl interpreter to use (default /usr/bin/perl).\n");
        printf("   -Q  Specify the directory where the GRID superscalar perl modules are installed.\n");
        printf("   -x  Generate XML formatted file.\n");
        printf("   -n  Do not generate backups of generated files.\n");
}       


int main(int argc, char **argv)
{
	char *filename;
	int opt;
	int correct_args = 1;
	
	while ((opt = getopt(argc, argv, "spljP:Q:xn")) != -1) {
		switch ((char)opt) {
			case 'n':
				set_no_backups();
				break;
			default:
				correct_args = 0;
				break;
		}
	}
	filename = argv[optind];
	
	if (!filename || !correct_args) {
		print_help(argv[0]);
		exit(1);
	}
	set_filename(filename);
	yyin = fopen(filename, "r");
	if (yyin == NULL) {
		fprintf(stderr, "Error: file not found.\n");
		exit(2);
	}
	yyparse();
	if (can_generate()) {
		generate_prolog();
		generate_body();
		generate_epilogue();

		/*
			generate_c_constraints_prolog();
			generate_c_constraints_body();
			generate_c_constraints_epilogue();
		*/
	} else {
		printf("No code generated.\n");
		return 1;
	}
	return 0;
}


