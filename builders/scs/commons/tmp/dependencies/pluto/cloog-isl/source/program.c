
/**-------------------------------------------------------------------**
 **                              CLooG                                **
 **-------------------------------------------------------------------**
 **                            program.c                              **
 **-------------------------------------------------------------------**
 **                 First version: october 25th 2001                  **
 **-------------------------------------------------------------------**/


/******************************************************************************
 *               CLooG : the Chunky Loop Generator (experimental)             *
 ******************************************************************************
 *                                                                            *
 * Copyright (C) 2001-2005 Cedric Bastoul                                     *
 *                                                                            *
 * This library is free software; you can redistribute it and/or              *
 * modify it under the terms of the GNU Lesser General Public                 *
 * License as published by the Free Software Foundation; either               *
 * version 2.1 of the License, or (at your option) any later version.         *
 *                                                                            *
 * This library is distributed in the hope that it will be useful,            *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of             *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU          *
 * Lesser General Public License for more details.                            *
 *                                                                            *
 * You should have received a copy of the GNU Lesser General Public           *
 * License along with this library; if not, write to the Free Software        *
 * Foundation, Inc., 51 Franklin Street, Fifth Floor,                         *
 * Boston, MA  02110-1301  USA                                                *
 *                                                                            *
 * CLooG, the Chunky Loop Generator                                           *
 * Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                         *
 *                                                                            *
 ******************************************************************************/
/* CAUTION: the english used for comments is probably the worst you ever read,
 *          please feel free to correct and improve it !
 */


# include <sys/types.h>
# include <sys/time.h>
#include <stdarg.h>
# include <stdlib.h>
# include <stdio.h>
# include <string.h>
# include <ctype.h>
# include <unistd.h>
# include "../include/cloog/cloog.h"
#ifdef CLOOG_RUSAGE
# include <sys/resource.h>
#endif

#define ALLOC(type) (type*)malloc(sizeof(type))

#ifdef OSL_SUPPORT
#include <osl/scop.h>
#include <osl/extensions/coordinates.h>
#include <osl/extensions/loop.h>
#endif

/******************************************************************************
 *                          Structure display function                        *
 ******************************************************************************/


/**
 * cloog_program_print function:
 * this function is a human-friendly way to display the CloogProgram data
 * structure, it shows all the different fields and includes an indentation
 * level (level) in order to work with others print_structure functions.
 * - July 1st 2005: first version based on the old cloog_program_print function.
 */
void cloog_program_print_structure(file, program, level)
FILE * file ;
CloogProgram * program ;
int level ;
{
    int i, j ;

    /* Go to the right level. */
    for (i=0; i<level; i++)
        fprintf(file,"|\t") ;

    fprintf(file,"+-- CloogProgram\n") ;

    /* A blank line. */
    for (i=0; i<=level+1; i++)
        fprintf(file,"|\t") ;
    fprintf(file,"\n") ;

    /* Print the language. */
    for (i=0; i<=level; i++)
        fprintf(file,"|\t") ;
    fprintf(file, "Language: %c\n",program->language) ;

    /* A blank line. */
    for (i=0; i<=level+1; i++)
        fprintf(file,"|\t") ;
    fprintf(file,"\n") ;

    /* Print the scattering dimension number. */
    for (i=0; i<=level; i++)
        fprintf(file,"|\t") ;
    fprintf(file,"Scattering dimension number: %d\n",program->nb_scattdims) ;

    /* A blank line. */
    for (i=0; i<=level+1; i++)
        fprintf(file,"|\t") ;
    fprintf(file,"\n") ;

    /* Print the scalar scattering dimension informations. */
    for (i=0; i<=level; i++)
        fprintf(file,"|\t") ;
    if (program->scaldims != NULL) {
        fprintf(file,"Scalar dimensions:") ;
        for (i=0; i<program->nb_scattdims; i++)
            fprintf(file," %d:%d ",i,program->scaldims[i]) ;
        fprintf(file,"\n") ;
    } else
        fprintf(file,"No scalar scattering dimensions\n") ;

    /* A blank line. */
    for (i=0; i<=level+1; i++)
        fprintf(file,"|\t") ;
    fprintf(file,"\n") ;

    /* Print the parameter and the iterator names. */
    cloog_names_print_structure(file,program->names,level+1) ;

    /* A blank line. */
    for (i=0; i<=level+1; i++)
        fprintf(file,"|\t") ;
    fprintf(file,"\n") ;

    /* Print the context. */
    cloog_domain_print_structure(file, program->context, level+1, "Context");

    /* Print the loop. */
    cloog_loop_print_structure(file,program->loop,level+1) ;

    /* One more time something that is here only for a better look. */
    for (j=0; j<2; j++) {
        for (i=0; i<=level; i++)
            fprintf(file,"|\t") ;

        fprintf(file,"\n") ;
    }
}


/**
 * cloog_program_dump_cloog function:
 * This function dumps a CloogProgram structure supposed to be completely
 * filled in a CLooG input file (foo possibly stdout) such as CLooG can
 * rebuild almost exactly the data structure from the input file.
 *
 * If the scattering is already applied, the scattering parameter is supposed to
 * be NULL. In this case the number of scattering functions is lost, since they
 * are included inside the iteration domains. This can only lead to a less
 * beautiful pretty printing.
 *
 * In case the scattering is not yet applied it can be passed to this function
 * and will be included in the CLooG input file dump.
 */
void cloog_program_dump_cloog(FILE * foo, CloogProgram * program,
                              CloogScatteringList *scattering) {
    int i;
    CloogLoop * loop ;
    CloogScatteringList *tmp_scatt;

    fprintf(foo,
            "# CLooG -> CLooG\n"
            "# This is an automatic dump of a CLooG input file from a CloogProgram data\n"
            "# structure. WARNING: it is highly dangerous and MAY be correct ONLY if\n"
            "# - it has been dumped before loop generation.\n"
            "# - option -noscalars is used (it removes scalar dimensions otherwise)\n"
            "# - option -l is at least the original scattering dimension number\n"
            "# ASK THE AUTHOR IF YOU *NEED* SOMETHING MORE ROBUST\n") ;

    /* Language. */
    if (program->language == 'f')
        fprintf(foo,"# Language: FORTRAN\n");
    else if (program->language == 'p')
        fprintf(foo,"# Language: PYTHON\n");
    else
        fprintf(foo,"# Language: C\n") ;
    fprintf(foo,"%c\n\n",program->language) ;

    /* Context. */
    fprintf(foo, "# Context (%d parameter(s)):\n", program->names->nb_parameters);
    cloog_domain_print_constraints(foo, program->context, 0);
    fprintf(foo,"1 # Parameter name(s)\n") ;
    for (i=0; i<program->names->nb_parameters; i++)
        fprintf(foo,"%s ",program->names->parameters[i]) ;

    /* Statement number. */
    i = 0 ;
    loop = program->loop ;
    while (loop != NULL) {
        i++ ;
        loop = loop->next ;
    }
    fprintf(foo,"\n\n# Statement number:\n%d\n\n",i) ;

    /* Iteration domains. */
    i = 1 ;
    loop = program->loop ;
    while (loop != NULL) {
        /* Name of the domain. */
        fprintf(foo,"# Iteration domain of statement %d.\n",i) ;

        cloog_domain_print_constraints(foo, loop->domain, 1);
        fprintf(foo,"0 0 0 # For future options.\n\n") ;

        i++ ;
        loop = loop->next ;
    }
    fprintf(foo,"\n1 # Iterator name(s)\n") ;

    /* Scattering already applied? In this case print the scattering names as
     * additional iterator names. */
    if (!scattering)
        for (i = 0; i < program->names->nb_scattering; i++)
            fprintf(foo, "%s ", program->names->scattering[i]);
    for (i=0; i<program->names->nb_iterators; i++)
        fprintf(foo,"%s ",program->names->iterators[i]);
    fprintf(foo,"\n\n") ;

    /* Exit, if scattering is already applied. */
    if (!scattering) {
        fprintf(foo, "# No scattering functions.\n0\n\n");
        return;
    }

    /* Scattering relations. */
    fprintf(foo, "# --------------------- SCATTERING --------------------\n");

    i = 0;
    for (tmp_scatt = scattering; tmp_scatt; tmp_scatt = tmp_scatt->next)
        i++;

    fprintf(foo, "%d # Scattering functions", i);

    for (tmp_scatt = scattering; tmp_scatt; tmp_scatt = tmp_scatt->next)
        cloog_scattering_print_constraints(foo, tmp_scatt->scatt);

    fprintf(foo, "\n1 # Scattering dimension name(s)\n");

    for (i = 0; i < program->names->nb_scattering; i++)
        fprintf(foo, "%s ", program->names->scattering[i]);
}


/**
 * cloog_program_print function:
 * This function prints the content of a CloogProgram structure (program) into a
 * file (file, possibly stdout).
 * - July 1st 2005: Now this very old function (probably as old as CLooG) is
 *                  only a frontend to cloog_program_print_structure, with a
 *                  quite better human-readable representation.
 */
void cloog_program_print(FILE * file, CloogProgram * program) {
    cloog_program_print_structure(file,program,0) ;
}


static void print_comment(FILE *file, CloogOptions *options,
                          const char *fmt, ...) {
    va_list args;

    va_start(args, fmt);
    if (options->language == CLOOG_LANGUAGE_FORTRAN) {
        fprintf(file, "! ");
        vfprintf(file, fmt, args);
        fprintf(file, "\n");
    } else if (options->language == CLOOG_LANGUAGE_PYTHON) {
        fprintf(file, "# ");
        vfprintf(file, fmt, args);
        fprintf(file, "\n");
    } else {
        fprintf(file, "/* ");
        vfprintf(file, fmt, args);
        fprintf(file, " */\n");
    }
}

static void print_macros(FILE *file) {
    fprintf(file, "/* Useful macros. */\n") ;
    fprintf(file,
            "#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))\n");
    fprintf(file,
            "#define ceild(n,d)  (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))\n");
    fprintf(file, "#define max(x,y)    ((x) > (y) ? (x) : (y))\n") ;
    fprintf(file, "#define min(x,y)    ((x) < (y) ? (x) : (y))\n\n") ;
    fprintf(file, "#ifdef TIME \n#define IF_TIME(foo) foo; \n"
            "#else\n#define IF_TIME(foo)\n#endif\n\n");
}

static void print_declarations(FILE *file, int n, char **names, int indentation) {
    int i;

    for (i = 0; i < indentation; i++)
        fprintf(file, " ");
    fprintf(file, "int %s", names[0]);
    for (i = 1; i < n; i++)
        fprintf(file, ", %s", names[i]);
    fprintf(file, ";\n");
}

static void print_scattering_declarations(FILE *file, CloogProgram *program,
        int indentation) {
    int i, j, found = 0;
    int nb_scatnames = 0;
    CloogNames *names = program->names;

    // Copy pointer only to those scatering names that do not duplicate
    // iterator names.
    char **scatnames = (char **) malloc(sizeof(char *) * names->nb_scattering);
    for (i = 0; i < names->nb_scattering; ++i) {
        for (j = 0; j < names->nb_iterators; ++j) {
            found = 0;
            if (strcmp(names->scattering[i], names->iterators[j]) == 0) {
                found = 1;
            }
        }
        if (!found) {
            // Save a pointer (intentional!) to the names in the new array.
            scatnames[nb_scatnames++] = names->scattering[i];
        }
    }

    if (nb_scatnames) {
        for (i = 0; i < indentation; i++)
            fprintf(file, " ");
        fprintf(file, "/* Scattering iterators. */\n");
        print_declarations(file, nb_scatnames, scatnames, indentation);
    }
    free(scatnames);
}

static void print_iterator_declarations(FILE *file, CloogProgram *program,
                                        CloogOptions *options) {
    CloogNames *names = program->names;

    print_scattering_declarations(file, program, 2);
    if (names->nb_iterators) {
        fprintf(file, "  /* Original iterators. */\n");
        print_declarations(file, names->nb_iterators, names->iterators, 2);
    }
}

static void print_callable_preamble(FILE *file, CloogProgram *program,
                                    CloogOptions *options) {
    int j;
    CloogBlockList *blocklist;
    CloogBlock *block;
    CloogStatement *statement;

    fprintf(file, "extern void hash(int);\n\n");

    print_macros(file);

    for (blocklist = program->blocklist; blocklist; blocklist = blocklist->next) {
        block = blocklist->block;
        for (statement = block->statement; statement; statement = statement->next) {
            fprintf(file, "#define S%d(", statement->number);
            if (block->depth > 0) {
                fprintf(file, "%s", program->names->iterators[0]);
                for(j = 1; j < block->depth; j++)
                    fprintf(file, ",%s", program->names->iterators[j]);
            }
            fprintf(file,") { hash(%d);", statement->number);
            for(j = 0; j < block->depth; j++)
                fprintf(file, " hash(%s);", program->names->iterators[j]);
            fprintf(file, " }\n");
        }
    }
    fprintf(file, "\nvoid test(");
    if (program->names->nb_parameters > 0) {
        fprintf(file, "int %s", program->names->parameters[0]);
        for(j = 1; j < program->names->nb_parameters; j++)
            fprintf(file, ", int %s", program->names->parameters[j]);
    }
    fprintf(file, ")\n{\n");
    print_iterator_declarations(file, program, options);
}

static void print_callable_postamble(FILE *file, CloogProgram *program) {
    fprintf(file, "}\n");
}

#ifdef OSL_SUPPORT
static int get_osl_loop_flags (osl_scop_p scop) {
    int flags = 0;
    osl_loop_p ll = osl_generic_lookup(scop->extension, OSL_URI_LOOP);
    while (ll) {
        flags |= ll->directive;
        ll = ll->next;
    }

    return flags;
}

static void print_iterator_declarations_osl(FILE *file, CloogProgram *program,
        int indent, CloogOptions *options) {
    osl_coordinates_p co = NULL;
    int i;
    int loopflags = 0;
    char* vecvar[2] = {"lbv", "ubv"};
    char* parvar[2] = {"lbp", "ubp"};

    osl_scop_p scop = options->scop;
    CloogNames *names = program->names;

    print_scattering_declarations(file, program, indent);

    co = osl_generic_lookup(scop->extension, OSL_URI_COORDINATES);
    if (co==NULL //if coordinates exist then iterators already declared in file
            && names->nb_iterators) {
        for (i = 0; i < indent; i++)
            fprintf(file, " ");
        fprintf(file, "/* Original iterators. */\n");
        print_declarations(file, names->nb_iterators, names->iterators, indent);
    }

    loopflags = get_osl_loop_flags(scop);
    if(loopflags & CLAST_PARALLEL_OMP)
        print_declarations(file, 2, parvar, indent);
    if(loopflags & CLAST_PARALLEL_VEC)
        print_declarations(file, 2, vecvar, indent);

    fprintf(file, "\n");
}

/*
* add tags clast loops according to information in scop's osl_loop extension
*/
static int annotate_loops(osl_scop_p program, struct clast_stmt *root) {

    int j, nclastloops, nclaststmts;
    struct clast_for **clastloops = NULL;
    int *claststmts = NULL;
    int ret = 0;

    if (program == NULL) {
        return ret;
    }

    osl_loop_p ll = osl_generic_lookup(program->extension, OSL_URI_LOOP);
    while (ll) {
        //for each loop
        osl_loop_p loop = ll;
        ClastFilter filter = { loop->iter, loop->stmt_ids,
                               loop->nb_stmts, subset
                             };

        clast_filter(root, filter, &clastloops, &nclastloops,
                     &claststmts, &nclaststmts);

        if (claststmts) {
            free(claststmts);
            claststmts = NULL;
        }

        /* There should be at least one */
        if (nclastloops==0) {  //FROM PLUTO
            /* Sometimes loops may disappear (1) tile size larger than trip count
            * 2) it's a scalar dimension but can't be determined from the
            * trans matrix */
            printf("Warning: parallel poly loop not found in AST\n");
            ll = ll->next;
            continue;
        }
        for (j=0; j<nclastloops; j++) {

            if (loop->directive & CLAST_PARALLEL_VEC) {
                clastloops[j]->parallel |= CLAST_PARALLEL_VEC;
                ret |= CLAST_PARALLEL_VEC;
            }

            if (loop->directive & CLAST_PARALLEL_OMP) {
                clastloops[j]->parallel |= CLAST_PARALLEL_OMP;
                ret |= CLAST_PARALLEL_OMP;
                if (loop->private_vars) {
                    clastloops[j]->private_vars = strdup(loop->private_vars);
                }
            }
        }

        if (clastloops) {
            free(clastloops);
            clastloops = NULL;
        }

        ll = ll->next;
    }

    return ret;
}
#endif

/**
 * cloog_program_osl_pprint function:
 * this function pretty-prints the C or FORTRAN code generated from an
 * OpenScop specification by overwriting SCoP in a given code, if the
 * options -compilable or -callable are not set. The SCoP coordinates are
 * provided through the OpenScop "Coordinates" extension. It returns 1 if
 * it succeeds to find an OpenScop coordinates information
 * to pretty-print the generated code, 0 otherwise.
 * \param[in] file    The output stream (possibly stdout).
 * \param[in] program The generated pseudo-AST to pretty-print.
 * \param[in] options CLooG options (contains the OpenSCop specification).
 * \return 1 on success to pretty-print at the place of a SCoP, 0 otherwise.
 */
int cloog_program_osl_pprint(FILE * file, CloogProgram * program,
                             CloogOptions * options) {
#ifdef OSL_SUPPORT
    int lines = 0;
    int columns = 0;
    int read = 1;
    int indentation = 0;
    char c;
    osl_scop_p scop = options->scop;
    osl_coordinates_p coordinates;
    struct clast_stmt *root;
    FILE* original = NULL;

    if (scop && !options->compilable && !options->callable) {
#ifdef CLOOG_RUSAGE
        print_comment(file, options, "Generated from %s by %s in %.2fs.",
                      options->name, cloog_version(), options->time);
#else
        print_comment(file, options, "Generated from %s by %s.",
                      options->name, cloog_version());
#endif
        coordinates = osl_generic_lookup(scop->extension, OSL_URI_COORDINATES);
        if (coordinates) {
            original = fopen(coordinates->name, "r");
            indentation = coordinates->indent;
            if (!original) {
                cloog_msg(options, CLOOG_WARNING,
                          "unable to open the file specified in the SCoP "
                          "coordinates\n");
                coordinates = NULL;
            }
        }

        /* Print the macros the generated code may need. */
        print_macros(file);

        /* Print what was before the SCoP in the original file (if any). */
        if (coordinates) {
            while (((lines < coordinates->line_start - 1) ||
                    (columns < coordinates->column_start - 1)) && (read != EOF)) {
                read = fscanf(original, "%c", &c);
                columns++;
                if (read != EOF) {
                    if (c == '\n') {
                        lines++;
                        columns = 0;
                    }
                    fprintf(file, "%c", c);
                }
            }

            /* Carriage return to preserve indentation if necessary. */
            if (coordinates->column_start > 0)
                fprintf(file, "\n");
        }

        /* Generate the clast from the pseudo-AST then pretty-print it. */
        root = cloog_clast_create(program, options);
        annotate_loops(options->scop, root);
        print_iterator_declarations_osl(file, program, indentation, options);
        clast_pprint(file, root, indentation, options);
        cloog_clast_free(root);

        /* Print what was after the SCoP in the original file (if any). */
        if (coordinates) {
            while (read != EOF) {
                read = fscanf(original, "%c", &c);
                columns++;
                if (read != EOF) {
                    if (((lines == coordinates->line_end - 1) &&
                            (columns > coordinates->column_end)) ||
                            (lines > coordinates->line_end - 1))
                        fprintf(file, "%c", c);
                    if (c == '\n') {
                        lines++;
                        columns = 0;
                    }
                }
            }

            fclose(original);
        }

        return 1;
    }
#endif
    return 0;
}

/**
 * cloog_program_pprint function:
 * This function prints the content of a CloogProgram structure (program) into a
 * file (file, possibly stdout), in a C-like language.
 * - June 22nd 2005: Adaptation for GMP.
 */
void cloog_program_pprint(file, program, options)
FILE * file ;
CloogProgram * program ;
CloogOptions * options ;
{
    int i, j, indentation = 0;
    CloogStatement * statement ;
    CloogBlockList * blocklist ;
    CloogBlock * block ;
    struct clast_stmt *root;

    if (cloog_program_osl_pprint(file, program, options))
        return;

    if (program->language == 'f')
        options->language = CLOOG_LANGUAGE_FORTRAN ;
    else if (program->language == 'p')
        options->language = CLOOG_LANGUAGE_PYTHON ;
    else
        options->language = CLOOG_LANGUAGE_C ;

#ifdef CLOOG_RUSAGE
    print_comment(file, options, "Generated from %s by %s in %.2fs.",
                  options->name, cloog_version(), options->time);
#else
    print_comment(file, options, "Generated from %s by %s.",
                  options->name, cloog_version());
#endif
#ifdef CLOOG_MEMORY
    print_comment(file, options, "CLooG asked for %d KBytes.", options->memory);
    cloog_msg(CLOOG_INFO, "%.2fs and %dKB used for code generation.\n",
              options->time,options->memory);
#endif

    /* If the option "compilable" is set, we provide the whole stuff to generate
     * a compilable code. This code just do nothing, but now the user can edit
     * the source and set the statement macros and parameters values.
     */
    if (options->compilable && (program->language == 'c')) {
        /* The headers. */
        fprintf(file,"/* DON'T FORGET TO USE -lm OPTION TO COMPILE. */\n\n") ;
        fprintf(file,"/* Useful headers. */\n") ;
        fprintf(file,"#include <stdio.h>\n") ;
        fprintf(file,"#include <stdlib.h>\n") ;
        fprintf(file,"#include <math.h>\n\n") ;

        /* The value of parameters. */
        fprintf(file,"/* Parameter value. */\n") ;
        for (i = 1; i <= program->names->nb_parameters; i++)
            fprintf(file, "#define PARVAL%d %d\n", i, options->compilable);

        /* The macros. */
        print_macros(file);

        /* The statement macros. */
        fprintf(file,"/* Statement macros (please set). */\n") ;
        blocklist = program->blocklist ;
        while (blocklist != NULL) {
            block = blocklist->block ;
            statement = block->statement ;
            while (statement != NULL) {
                fprintf(file,"#define S%d(",statement->number) ;
                if (block->depth > 0) {
                    fprintf(file,"%s",program->names->iterators[0]) ;
                    for(j=1; j<block->depth; j++)
                        fprintf(file,",%s",program->names->iterators[j]) ;
                }
                fprintf(file,") {total++;") ;
                if (block->depth > 0) {
                    fprintf(file, " printf(\"S%d %%d", statement->number);
                    for(j=1; j<block->depth; j++)
                        fprintf(file, " %%d");

                    fprintf(file,"\\n\",%s",program->names->iterators[0]) ;
                    for(j=1; j<block->depth; j++)
                        fprintf(file,",%s",program->names->iterators[j]) ;
                    fprintf(file,");") ;
                }
                fprintf(file,"}\n") ;

                statement = statement->next ;
            }
            blocklist = blocklist->next ;
        }

        /* The iterator and parameter declaration. */
        fprintf(file,"\nint main() {\n") ;
        print_iterator_declarations(file, program, options);
        if (program->names->nb_parameters > 0) {
            fprintf(file,"  /* Parameters. */\n") ;
            fprintf(file, "  int %s=PARVAL1",program->names->parameters[0]);
            for(i=2; i<=program->names->nb_parameters; i++)
                fprintf(file, ", %s=PARVAL%d", program->names->parameters[i-1], i);

            fprintf(file,";\n");
        }
        fprintf(file,"  int total=0;\n");
        fprintf(file,"\n") ;

        /* And we adapt the identation. */
        indentation += 2 ;
    } else if (options->callable && program->language == 'c') {
        print_callable_preamble(file, program, options);
        indentation += 2;
    }

    root = cloog_clast_create(program, options);
    clast_pprint(file, root, indentation, options);
    cloog_clast_free(root);

    /* The end of the compilable code in case of 'compilable' option. */
    if (options->compilable && (program->language == 'c')) {
        fprintf(file, "\n  printf(\"Number of integral points: %%d.\\n\",total);");
        fprintf(file, "\n  return 0;\n}\n");
    } else if (options->callable && program->language == 'c')
        print_callable_postamble(file, program);
}


/******************************************************************************
 *                         Memory deallocation function                       *
 ******************************************************************************/


/**
 * cloog_program_free function:
 * This function frees the allocated memory for a CloogProgram structure.
 */
void cloog_program_free(CloogProgram * program) {
    cloog_names_free(program->names) ;
    cloog_loop_free(program->loop) ;
    cloog_domain_free(program->context) ;
    cloog_block_list_free(program->blocklist) ;
    if (program->scaldims != NULL)
        free(program->scaldims) ;

    free(program) ;
}


/******************************************************************************
 *                               Reading function                             *
 ******************************************************************************/


static void cloog_program_construct_block_list(CloogProgram *p) {
    CloogLoop *loop;
    CloogBlockList **next = &p->blocklist;

    for (loop = p->loop; loop; loop = loop->next) {
        *next = cloog_block_list_alloc(loop->block);
        next = &(*next)->next;
    }
}


/**
 * Construct a CloogProgram structure from a given context and
 * union domain representing the iteration domains and scattering functions.
 */
CloogProgram *cloog_program_alloc(CloogDomain *context, CloogUnionDomain *ud,
                                  CloogOptions *options) {
    int i;
    char prefix[] = "c";
    CloogScatteringList * scatteringl;
    CloogNames *n;
    CloogProgram * p ;

    /* Memory allocation for the CloogProgram structure. */
    p = cloog_program_malloc() ;

    if (options->language == CLOOG_LANGUAGE_FORTRAN)
        p->language = 'f';
    else if (options->language == CLOOG_LANGUAGE_PYTHON)
        p->language = 'p';
    else
        p->language = 'c';

    p->names = n = cloog_names_alloc();

    /* We then read the context data. */
    p->context = context;
    n->nb_parameters = ud->n_name[CLOOG_PARAM];

    /* First part of the CloogNames structure: the parameter names. */
    if (ud->name[CLOOG_PARAM]) {
        n->parameters = ud->name[CLOOG_PARAM];
        ud->name[CLOOG_PARAM] = NULL;
    } else
        n->parameters = cloog_names_generate_items(n->nb_parameters, NULL,
                        FIRST_PARAMETER);

    n->nb_iterators = ud->n_name[CLOOG_ITER];
    if (ud->name[CLOOG_ITER]) {
        n->iterators = ud->name[CLOOG_ITER];
        ud->name[CLOOG_ITER] = NULL;
    } else
        n->iterators = cloog_names_generate_items(n->nb_iterators, NULL,
                       FIRST_ITERATOR);

    if (ud->domain) {
        CloogNamedDomainList *l;
        CloogLoop **next = &p->loop;
        CloogScatteringList **next_scat = &scatteringl;

        scatteringl = NULL;
        for (i = 0, l = ud->domain; l; ++i, l = l->next) {
            *next = cloog_loop_from_domain(options->state, l->domain, i);
            l->domain = NULL;
            (*next)->block->statement->name = l->name;
            (*next)->block->statement->usr = l->usr;
            l->name = NULL;

            if (l->scattering) {
                *next_scat = ALLOC(CloogScatteringList);
                (*next_scat)->scatt = l->scattering;
                l->scattering = NULL;
                (*next_scat)->next = NULL;

                next_scat = &(*next_scat)->next;
            }

            next = &(*next)->next;
        }

        if (scatteringl != NULL) {
            p->nb_scattdims = cloog_scattering_dimension(scatteringl->scatt,
                              p->loop->domain);
            n->nb_scattering = p->nb_scattdims;
            if (ud->name[CLOOG_SCAT]) {
                n->scattering = ud->name[CLOOG_SCAT];
                ud->name[CLOOG_SCAT] = NULL;
            } else
                n->scattering = cloog_names_generate_items(n->nb_scattering, prefix, -1);

            /* The boolean array for scalar dimensions is created and set to 0. */
            p->scaldims = (int *)malloc(p->nb_scattdims*(sizeof(int))) ;
            if (p->scaldims == NULL)
                cloog_die("memory overflow.\n");
            for (i=0; i<p->nb_scattdims; i++)
                p->scaldims[i] = 0 ;

            /* We try to find blocks in the input problem to reduce complexity. */
            if (!options->noblocks)
                cloog_program_block(p, scatteringl, options);
            if (!options->noscalars)
                cloog_program_extract_scalars(p, scatteringl, options);

            cloog_program_scatter(p, scatteringl, options);
            cloog_scattering_list_free(scatteringl);

            if (!options->noblocks)
                p->loop = cloog_loop_block(p->loop, p->scaldims, p->nb_scattdims);
        } else {
            p->nb_scattdims = 0 ;
            p->scaldims  = NULL ;
        }

        cloog_names_scalarize(p->names,p->nb_scattdims,p->scaldims) ;

        cloog_program_construct_block_list(p);
    } else {
        p->loop      = NULL ;
        p->blocklist = NULL ;
        p->scaldims  = NULL ;
    }

    cloog_union_domain_free(ud);

    return(p) ;
}


/**
 * cloog_program_read function:
 * This function read the informations to put in a CloogProgram structure from
 * a file (file, possibly stdin). It returns a pointer to a CloogProgram
 * structure containing the read informations.
 * - October 25th 2001: first version.
 * - September 9th 2002: - the big reading function is now split in several
 *                         functions (one per read data structure).
 *                       - adaptation to the new file format with naming.
 */
CloogProgram *cloog_program_read(FILE *file, CloogOptions *options) {
    CloogInput *input;
    CloogProgram *p;

    input = cloog_input_read(file, options);
    p = cloog_program_alloc(input->context, input->ud, options);
    free(input);

    return p;
}


/******************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/


/**
 * cloog_program_malloc function:
 * This function allocates the memory space for a CloogProgram structure and
 * sets its fields with default values. Then it returns a pointer to the
 * allocated space.
 * - November 21th 2005: first version.
 */
CloogProgram * cloog_program_malloc() {
    CloogProgram * program ;

    /* Memory allocation for the CloogProgram structure. */
    program = (CloogProgram *)malloc(sizeof(CloogProgram)) ;
    if (program == NULL)
        cloog_die("memory overflow.\n");

    /* We set the various fields with default values. */
    program->language     = 'c' ;
    program->nb_scattdims = 0 ;
    program->context      = NULL ;
    program->loop         = NULL ;
    program->names        = NULL ;
    program->blocklist    = NULL ;
    program->scaldims     = NULL ;
    program->usr          = NULL;

    return program ;
}


/**
 * cloog_program_generate function:
 * This function calls the Quillere algorithm for loop scanning. (see the
 * Quillere paper) and calls the loop simplification function.
 * - depth is the loop depth we want to optimize (guard free as possible),
 *   the first loop depth is 1 and anegative value is the infinity depth.
 * - sep_level is the level number where we want to start loop separation.
 **
 * - October 26th 2001: first version.
 * - April   19th 2005: some basic fixes and memory usage feature.
 * - April   29th 2005: (bug fix, bug found by DaeGon Kim) see case 2 below.
 */
CloogProgram * cloog_program_generate(program, options)
CloogProgram * program ;
CloogOptions * options ;
{
#ifdef CLOOG_RUSAGE
    float time;
    struct rusage start, end ;
#endif
    CloogLoop * loop ;
#ifdef CLOOG_MEMORY
    char status_path[MAX_STRING_VAL] ;
    FILE * status ;

    /* We initialize the memory need to 0. */
    options->memory = 0 ;
#endif

    if (options->override) {
        cloog_msg(options, CLOOG_WARNING,
                  "you are using -override option, be aware that the "
                  "generated\n                code may be incorrect.\n") ;
    } else {
        /* Playing with options may be dangerous, here are two possible issues :
         * 1. Using -l option less than scattering dimension number may lead to
         *    an illegal target code (since the scattering is not respected), if
         *    it is the case, we set -l depth to the first acceptable value.
         */
        if ((program->nb_scattdims > options->l) && (options->l >= 0)) {
            cloog_msg(options, CLOOG_WARNING,
                      "-l depth is less than the scattering dimension number "
                      "(the \n                generated code may be incorrect), it has been "
                      "automaticaly set\n                to this value (use option -override "
                      "to override).\n") ;
            options->l = program->nb_scattdims ;
        }

        /* 2. Using -f option greater than one while -l depth is greater than the
         *    scattering dimension number may lead to iteration duplication (try
         *    test/daegon_lu_osp.cloog with '-f 3' to test) because of the step 4b
         *    of the cloog_loop_generate function, if it is the case, we set -l to
         *    the first acceptable value.
         */
        if (((options->f > 1) || (options->f < 0)) &&
                ((options->l > program->nb_scattdims) || (options->l < 0))) {
            cloog_msg(options, CLOOG_WARNING,
                      "-f depth is more than one, -l depth has been "
                      "automaticaly set\n                to the scattering dimension number "
                      "(target code may have\n                duplicated iterations), -l depth "
                      "has been automaticaly set to\n                this value (use option "
                      "-override to override).\n") ;
            options->l = program->nb_scattdims ;
        }
    }

#ifdef CLOOG_RUSAGE
    getrusage(RUSAGE_SELF, &start) ;
#endif
    if (program->loop != NULL) {
        loop = program->loop ;

        /* Here we go ! */
        loop = cloog_loop_generate(loop, program->context, 0, 0,
                                   program->scaldims,
                                   program->nb_scattdims,
                                   options);

#ifdef CLOOG_MEMORY
        /* We read into the status file of the process how many memory it uses. */
        sprintf(status_path,"/proc/%d/status",getpid()) ;
        status = fopen(status_path, "r") ;
        while (fscanf(status,"%s",status_path) && strcmp(status_path,"VmData:")!=0);
        fscanf(status,"%d",&(options->memory)) ;
        fclose(status) ;
#endif

        if ((!options->nosimplify) && (program->loop != NULL))
            loop = cloog_loop_simplify(loop, program->context, 0,
                                       program->nb_scattdims, options);

        program->loop = loop ;
    }

#ifdef CLOOG_RUSAGE
    getrusage(RUSAGE_SELF, &end) ;
    /* We calculate the time spent in code generation. */
    time =  (end.ru_utime.tv_usec -  start.ru_utime.tv_usec)/(float)(MEGA) ;
    time += (float)(end.ru_utime.tv_sec - start.ru_utime.tv_sec) ;
    options->time = time ;
#endif

    return program ;
}


/**
 * cloog_program_block function:
 * this function gives a last chance to the lazy user to consider statement
 * blocks instead of some statement lists where the whole list may be
 * considered as a single statement from a code generation point of view.
 * For instance two statements with the same iteration domain and the same
 * scattering functions may be considered as a block. This function is lazy
 * and can only find very simple forms of trivial blocks (see
 * cloog_domain_lazy_block function for more details). The useless loops and
 * scattering functions are removed and freed while the statement list of
 * according blocks are filled.
 * - program is the whole program structure (befaore applying scattering),
 * - scattering is the list of scattering functions.
 **
 * - April   30th 2005: first attempt.
 * - June 10-11th 2005: first working version.
 */
void cloog_program_block(CloogProgram *program,
                         CloogScatteringList *scattering, CloogOptions *options) {
    int blocked_reference=0, blocked=0, nb_blocked=0 ;
    CloogLoop * reference, * start, * loop ;
    CloogScatteringList * scatt_reference, * scatt_loop, * scatt_start;

    if ((program->loop == NULL) || (program->loop->next == NULL))
        return ;

    /* The process will use three variables for the linked list :
     * - 'start' is the starting point of a new block,
     * - 'reference' is the node of the block used for the block checking,
     * - 'loop' is the candidate to be inserted inside the block.
     * At the beginning of the process, the linked lists are as follow:
     *         O------>O------>O------>O------>NULL
     *         |       |
     *       start    loop
     *     reference
     */

    reference       = program->loop ;
    start           = program->loop ;
    loop            = reference->next ;
    scatt_reference = scattering ;
    scatt_start     = scattering ;
    scatt_loop      = scattering->next ;

    while (loop != NULL) {
        if (cloog_domain_lazy_equal(reference->domain,loop->domain) &&
                cloog_scattering_lazy_block(scatt_reference->scatt, scatt_loop->scatt,
                                            scattering,program->nb_scattdims)) {
            /* If we find a block we update the links:
             *     +---------------+
             *     |               v
             *     O       O------>O------>O------>NULL
             *     |       |
             *   start    loop
             * reference
             */
            blocked = 1 ;
            nb_blocked ++ ;
            cloog_block_merge(start->block,loop->block); /* merge frees loop->block */
            loop->block = NULL ;
            start->next = loop->next ;
            scatt_start->next = scatt_loop->next ;
        } else {
            /* If we didn't find a block, the next start of a block is updated:
             *     O------>O------>O------>O------>NULL
             *     |       |
             * reference start
             *           loop
             */
            blocked= 0 ;
            start = loop ;
            scatt_start = scatt_loop ;
        }

        /* If the reference node has been included into a block, we can free it. */
        if (blocked_reference) {
            reference->next = NULL ;
            cloog_loop_free(reference) ;
            cloog_scattering_free(scatt_reference->scatt);
            free(scatt_reference) ;
        }

        /* The reference and the loop are now updated for the next try, the
         * starting position depends on the previous step.
         *       O   ?   O------>O------>O------>NULL
         *               |       |
         *           reference loop
         */
        reference       = loop ;
        loop            = loop->next ;
        scatt_reference = scatt_loop ;
        scatt_loop      = scatt_loop->next ;

        /* We mark the new reference as being blocked or not, if will be freed
         * during the next while loop execution.
         */
        if (blocked)
            blocked_reference = 1 ;
        else
            blocked_reference = 0 ;
    }

    /* We free the last blocked reference if any (since in the while loop it was
     * freed during the next loop execution, it was not possible to free the
     * last one inside).
     */
    if (blocked_reference) {
        reference->next = NULL ;
        cloog_loop_free(reference) ;
        cloog_scattering_free(scatt_reference->scatt);
        free(scatt_reference) ;
    }

    if (nb_blocked != 0)
        cloog_msg(options, CLOOG_INFO, "%d domains have been blocked.\n", nb_blocked);
}


/**
 * cloog_program_extract_scalars function:
 * this functions finds and removes the dimensions of the scattering functions
 * when they are scalar (i.e. of the shape "dim + scalar = 0") for all
 * scattering functions. The reason is that the processing of such dimensions
 * is trivial and do not need neither a row and a column in the matrix
 * representation of the domain (this will save memory) neither the full
 * Quillere processing (this will save time). The scalar dimensions data are
 * dispatched in the CloogProgram structure (the boolean vector scaldims will
 * say which original dimensions are scalar or not) and to the CloogBlock
 * structures (each one has a scaldims vector that contains the scalar values).
 * - June 14th 2005: first developments.
 * - June 30th 2005: first version.
 */
void cloog_program_extract_scalars(CloogProgram *program,
                                   CloogScatteringList *scattering, CloogOptions *options) {
    int i, j, scalar, current, nb_scaldims=0 ;
    CloogScatteringList *start;
    CloogScattering *old;
    CloogLoop *loop;
    CloogBlock * block ;

    start = scattering ;

    for (i=0; i<program->nb_scattdims; i++) {
        scalar = 1 ;
        scattering = start ;
        while (scattering != NULL) {
            if (!cloog_scattering_lazy_isscalar(scattering->scatt, i, NULL)) {
                scalar = 0 ;
                break ;
            }
            scattering = scattering->next ;
        }

        if (scalar) {
            nb_scaldims ++ ;
            program->scaldims[i] = 1 ;
        }
    }

    /* If there are no scalar dimensions, we can continue directly. */
    if (!nb_scaldims)
        return ;

    /* Otherwise, in each block, we have to put the number of scalar dimensions,
     * and to allocate the memory for the scalar values.
     */
    for (loop = program->loop; loop; loop = loop->next) {
        block = loop->block;
        block->nb_scaldims = nb_scaldims ;
        block->scaldims = (cloog_int_t *)malloc(nb_scaldims*sizeof(cloog_int_t));
        for (i=0; i<nb_scaldims; i++)
            cloog_int_init(block->scaldims[i]);
    }

    /* Then we have to fill these scalar values, so we can erase those dimensions
     * from the scattering functions. It's easier to begin with the last one,
     * since there would be an offset otherwise (if we remove the i^th dimension,
     * then the next one is not the (i+1)^th but still the i^th...).
     */
    current = nb_scaldims - 1 ;
    for (i=program->nb_scattdims-1; i>=0; i--)
        if (program->scaldims[i]) {
            scattering = start ;
            for (loop = program->loop; loop; loop = loop->next) {
                block = loop->block;
                if (!cloog_scattering_lazy_isscalar(scattering->scatt, i,
                                                    &block->scaldims[current])) {
                    /* We should have found a scalar value: if not, there is an error. */
                    cloog_die("dimension %d is not scalar as expected.\n", i);
                }
                scattering = scattering->next ;
            }

            scattering = start ;
            while (scattering != NULL) {
                old = scattering->scatt;
                scattering->scatt = cloog_scattering_erase_dimension(old, i);
                cloog_scattering_free(old);
                scattering = scattering->next ;
            }
            current-- ;
        }

    /* We postprocess the scaldims array in such a way that each entry is how
     * many scalar dimensions follows + 1 (the current one). This will make
     * some other processing easier (e.g. knowledge of some offsets).
     */
    for (i=0; i<program->nb_scattdims-1; i++) {
        if (program->scaldims[i]) {
            j = i + 1 ;
            while ((j < program->nb_scattdims) && program->scaldims[j]) {
                program->scaldims[i] ++ ;
                j ++ ;
            }
        }
    }

    if (nb_scaldims != 0)
        cloog_msg(options, CLOOG_INFO, "%d dimensions (over %d) are scalar.\n",
                  nb_scaldims,program->nb_scattdims) ;
}


/**
 * cloog_program_scatter function:
 * This function adds the scattering (scheduling) informations in a program.
 * If names is NULL, this function create names itself such that the i^th
 * name is ci.
 * - November 6th 2001: first version.
 */
void cloog_program_scatter(CloogProgram *program,
                           CloogScatteringList *scattering, CloogOptions *options) {
    int scattering_dim, scattering_dim2, not_enough_constraints=0 ;
    CloogLoop * loop ;

    if ((program != NULL) && (scattering != NULL)) {
        loop = program->loop ;

        /* We compute the scattering dimension and check it is >=0. */
        scattering_dim = cloog_scattering_dimension(scattering->scatt, loop->domain);
        if (scattering_dim < 0)
            cloog_die("scattering has not enough dimensions.\n");
        if (!cloog_scattering_fully_specified(scattering->scatt, loop->domain))
            not_enough_constraints ++ ;

        /* The scattering dimension may have been modified by scalar extraction. */
        scattering_dim = cloog_scattering_dimension(scattering->scatt, loop->domain);

        /* Finally we scatter all loops. */
        cloog_loop_scatter(loop, scattering->scatt);
        loop = loop->next ;
        scattering = scattering->next ;

        while ((loop != NULL) && (scattering != NULL)) {
            scattering_dim2 = cloog_scattering_dimension(scattering->scatt,
                              loop->domain);
            if (scattering_dim2 != scattering_dim)
                cloog_die("scattering dimensions are not the same.\n") ;
            if (!cloog_scattering_fully_specified(scattering->scatt, loop->domain))
                not_enough_constraints ++ ;

            cloog_loop_scatter(loop, scattering->scatt);
            loop = loop->next ;
            scattering = scattering->next ;
        }
        if ((loop != NULL) || (scattering != NULL))
            cloog_msg(options, CLOOG_WARNING,
                      "there is not a scattering for each statement.\n");

        if (not_enough_constraints)
            cloog_msg(options, CLOOG_WARNING, "not enough constraints for "
                      "%d scattering function(s).\n",not_enough_constraints) ;
    }
}
