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


static void print_help(char *command) {
    printf("Usage: %s [-n] <input file>\n", command);
    printf("   The output files are generated according to the input filename.\n");
    printf("   -n  Do not generate backups of generated files.\n");
}


int main(int argc, char **argv) {
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
        printf("Generating prolog\n");
        generate_prolog();
        printf("Generating body\n");
        generate_body();
        printf("Generating epilog\n");
        generate_epilogue();
    } else {
        printf("No code generated.\n");
        return 1;
    }
    return 0;
}


