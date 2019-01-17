
/*+------- <| --------------------------------------------------------**
 **         A                     Clan                                **
 **---     /.\   -----------------------------------------------------**
 **   <|  [""M#                options.c                              **
 **-   A   | #   -----------------------------------------------------**
 **   /.\ [""M#         First version: 24/05/2008                     **
 **- [""M# | #  U"U#U  -----------------------------------------------**
      | #  | #  \ .:/
      | #  | #___| #
******  | "--'     .-"  ******************************************************
*     |"-"-"-"-"-#-#-##   Clan : the Chunky Loop Analyzer (experimental)     *
****  |     # ## ######  *****************************************************
*      \       .::::'/                                                       *
*       \      ::::'/     Copyright (C) 2008 University Paris-Sud 11         *
*     :8a|    # # ##                                                         *
*     ::88a      ###      This is free software; you can redistribute it     *
*    ::::888a  8a ##::.   and/or modify it under the terms of the GNU Lesser *
*  ::::::::888a88a[]:::   General Public License as published by the Free    *
*::8:::::::::SUNDOGa8a::. Software Foundation, either version 2.1 of the     *
*::::::::8::::888:Y8888:: License, or (at your option) any later version.    *
*::::':::88::::888::Y88a::::::::::::...                                      *
*::'::..    .   .....   ..   ...  .                                          *
* This software is distributed in the hope that it will be useful, but       *
* WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY *
* or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License   *
* for more details.							      *
*                                                                            *
* You should have received a copy of the GNU Lesser General Public License   *
* along with software; if not, write to the Free Software Foundation, Inc.,  *
* 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA                     *
*                                                                            *
* Clan, the Chunky Loop Analyzer                                             *
* Written by Cedric Bastoul, Cedric.Bastoul@u-psud.fr                        *
*                                                                            *
******************************************************************************/


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <osl/macros.h>
#include <clan/macros.h>
#include <clan/options.h>


/*+****************************************************************************
 *                          Structure display function                        *
 ******************************************************************************/


/**
 * clan_option_print function:
 * This function prints the content of a clan_options_t structure (program)
 * into a file (foo, possibly stdout).
 * \param foo     File where informations are printed.
 * \param options Option structure whose information have to be printed.
 */
void clan_options_print(FILE* foo, clan_options_p options) {
    fprintf(foo, "Options:\n");

    if (options->name != NULL)
        fprintf(foo, "name            = %s,\n", options->name);
    else
        fprintf(foo, "name            = NULL,\n");

    fprintf(foo, "castle          = %3d,\n", options->castle);
    fprintf(foo, "structure       = %3d.\n", options->structure);
    fprintf(foo, "autoscop        = %3d.\n", options->autoscop);
    fprintf(foo, "autopragma      = %3d.\n", options->autopragma);
    fprintf(foo, "autoinsert      = %3d.\n", options->autoinsert);
    fprintf(foo, "inputscop       = %3d.\n", options->inputscop);
    fprintf(foo, "bounded_context = %3d.\n", options->bounded_context);
    fprintf(foo, "noloopcontext   = %3d.\n", options->noloopcontext);
    fprintf(foo, "nosimplify      = %3d.\n", options->nosimplify);
    fprintf(foo, "extbody         = %3d.\n", options->extbody);
}


/*+****************************************************************************
 *                         Memory deallocation function                       *
 ******************************************************************************/


/**
 * clan_options_free function:
 * This function frees the allocated memory for a clan_options_t structure.
 * \param options Option structure to be freed.
 */
void clan_options_free(clan_options_p options) {
    free(options->name);
    free(options);
}


/*+****************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/


/**
 * clan_options_help function:
 * This function displays the quick help when the user set the option -help
 * while calling clan. Prints are cut to respect the 509 characters
 * limitation of the ISO C 89 compilers.
 */
void clan_options_help() {
    printf(
        "Usage: clan [ options | file ] ...\n");
    printf(
        "\nGeneral options:\n"
        "  -o <output>          Name of the output file; 'stdout' is a special\n"
        "                       value: when used, output is standard output\n"
        "                       (default setting: stdout).\n"
        "  -autoscop            Automatic SCoP extraction.\n"
        "  -autopragma          Automatic insertion of SCoP pragmas in input code.\n"
        "  -autoinsert          Automatic insertion of SCoP pragmas in input file.\n"
        "  -inputscop           Read a .scop as the input.\n"
        "  -precision <value>   32 to work in 32 bits, 64 for 64 bits, 0 for GMP.\n"
        "  -boundedctxt         Bound all global parameters to be >= -1.\n"
        "  -noloopctxt          Do not include loop context (simplifies domains).\n"
        "  -nosimplify          Do not simplify iteration domains.\n"
        "  -outscoplib          Print to the SCoPLib format.\n"
        "  -extbody             Will generate the extbody.\n"
        "  -v, --version        Display the release information (and more).\n"
        "  -h, --help           Display this information.\n\n");
    printf(
        "The special value 'stdin' for 'file' or the special option '-' makes clan\n"
        "to read data on standard input.\n\n"
        "For bug reporting or any suggestions, please send an email to the author\n"
        "Cedric Bastoul <cedric.bastoul@inria.fr>.\n");
}


/**
 * clan_options_version function:
 * This function displays some version informations when the user set the
 * option -version while calling clan. Prints are cut to respect the 509
 * characters limitation of the ISO C 89 compilers.
 */
void clan_options_version() {
    printf("clan %s ", CLAN_VERSION);
    printf("       The Chunky Loop Analyzer\n");

    printf(
        "-----\n"
        "This is a polyhedral representation extractor for imperative programs using "
        "a C\ngrammar for control flow and array accesses (this includes C, C++,"
        " Java, C#\nand probably few toy languages too). This program is distributed "
        "under the\nterms of the GNU Lesser General Public License, see details of "
        "the licence at\nhttp://www.gnu.org/copyleft/lgpl.html\n"
        "-----\n");
    printf(
        "It would be fair to refer the following paper in any publication "
        "resulting from\nthe use of this software or its library (it defines SCoPs):\n"
        "@InProceedings{Bas03,\n"
        "author    =  {Cedric Bastoul and Albert Cohen and Sylvain Girbal and\n"
        "              Saurabh Sharma and Olivier Temam},\n"
        "title     =  {Putting Polyhedral Loop Transformations to Work},\n"
        "booktitle =  {LCPC'16 International Workshop on Languages and\n"
        "              Compilers for Parallel Computers, LNCS 2958},\n"
        "pages     =  {209--225},\n"
        "month     =  {october},\n"
        "year      =  2003,\n"
        "address   =  {College Station, Texas}\n"
        "}\n"
        "-----\n");
    printf(
        "For any information, please send an email to the author\n"
        "Cedric Bastoul <cedric.bastoul@inria.fr>.\n");
}


/**
 * clan_options_set function:
 * This function sets the value of an option thanks to the user's calling line.
 * \param option The value to set,
 * \param argv   Number of elements in the user's calling line,
 * \param argc   Elements of the user's calling line,
 * \param number Number of the element corresponding to the considered option,
 *               this function adds 1 to number to pass away the option value.
 */
void clan_options_set(int* option, int argv, char** argc, int* number) {
    char** endptr;

    if (*number+1 >= argv)
        CLAN_error("an option lacks of argument");

    endptr = NULL;
    *option = strtol(argc[*number+1], endptr, 10);
    if (endptr != NULL) {
        fprintf(stderr, "[Clan] Error: %s value for %s option is not valid.\n",
                argc[*number + 1], argc[*number]);
        exit(1);
    }
    *number = *number + 1;
}


/**
 * clan_options_malloc function:
 * This functions allocate the memory space for a clan_options_t structure and
 * fill its fields with the defaults values. It returns a pointer to the
 * allocated clan_options_t structure.
 */
clan_options_p clan_options_malloc(void) {
    clan_options_p options;

    // Memory allocation for the clan_options_t structure.
    CLAN_malloc(options, clan_options_p, sizeof(clan_options_t));

    // We set the various fields with default values.
    options->name       = NULL;  // Name of the input file is not set.
    options->castle     = 1;     // Do print the Clan McCloog castle in output.
    options->structure  = 0;     // Don't print internal structure.
    options->autoscop   = 0;     // Do not extract SCoPs automatically.
    options->autopragma = 0;     // Do not insert SCoP pragmas in the input code.
    options->autoinsert = 0;     // Do not insert SCoP pragmas in the input file.
    options->inputscop  = 0;     // Default input is a source file, not a .scop.
    options->precision  = 64;    // Work in 64 bits by default.
    options->bounded_context = 0;// Don't bound the global parameters.
    options->noloopcontext   = 0;// Do include loop context in domains.
    options->nosimplify      = 0;// Do simplify iteration domains.
    options->outscoplib      = 0;// Default OpenScop format
    options->extbody         = 0;// Don't generate the extbody
    return options;
}


/**
 * clan_options_read function:
 * This functions reads all the options and the input/output files thanks
 * the the user's calling line elements (in argc). It fills a clan_options_t
 * structure, a NULL-terminated array of input file names and the FILE
 * structure corresponding to the output files.
 * \param[in]  argv        Number of strings in command line.
 * \param[in]  argc        Array of command line strings.
 * \param[out] input_files Null-terminated array of input file names.
 * \param[out] output      Output file.
 */
clan_options_p clan_options_read(int argv, char** argc,
                                 char*** input_files, FILE** output) {
    int i, infos=0, input_is_stdin=0;
    clan_options_p options;
    int nb_input_files = 0;

    // clan_options_t structure allocation and initialization.
    options = clan_options_malloc();

    // The default output is the standard output.
    *output = stdout;
    *input_files = NULL;

    // Prepare an empty array of input file names.
    CLAN_malloc(*input_files, char**, sizeof(char*));
    (*input_files)[0] = NULL;

    for (i=1; i < argv; i++) {
        if (argc[i][0] == '-') {
            if (argc[i][1] == '\0') {
                // "-" alone is a special option to set input to standard input.
                nb_input_files++;
                input_is_stdin = 1;
                CLAN_realloc(*input_files, char**, sizeof(char*) * (nb_input_files+1));
                CLAN_strdup((*input_files)[nb_input_files-1], "stdin");
                (*input_files)[nb_input_files] = NULL;
                nb_input_files++;
            } else if (strcmp(argc[i], "-castle") == 0) {
                clan_options_set(&(options)->castle, argv, argc, &i);
            } else if (strcmp(argc[i], "-structure") == 0) {
                options->structure = 1;
            } else if (strcmp(argc[i], "-autoscop") == 0) {
                options->autoscop = 1;
            } else if (strcmp(argc[i], "-autopragma") == 0) {
                options->autoscop = 1;
                options->autopragma = 1;
            } else if (strcmp(argc[i], "-autoinsert") == 0) {
                options->autoscop = 1;
                options->autoinsert = 1;
            } else if (strcmp(argc[i], "-inputscop") == 0) {
                options->inputscop = 1;
            } else if (strcmp(argc[i], "-boundedctxt") == 0) {
                options->bounded_context = 1;
            } else if (strcmp(argc[i], "-noloopctxt") == 0) {
                options->noloopcontext = 1;
            } else if (strcmp(argc[i], "-nosimplify") == 0) {
                options->nosimplify = 1;
            } else if (strcmp(argc[i], "-outscoplib") == 0) {
                options->outscoplib = 1;
            } else if (strcmp(argc[i], "-extbody") == 0) {
                options->extbody = 1;
            } else if (strcmp(argc[i], "-precision") == 0) {
                clan_options_set(&(options)->precision, argv, argc, &i);
            } else if ((strcmp(argc[i], "--help") == 0) ||
                       (strcmp(argc[i], "-h") == 0)) {
                clan_options_help();
                infos = 1;
            } else if ((strcmp(argc[i],"--version") == 0) ||
                       (strcmp(argc[i],"-v") == 0)) {
                clan_options_version();
                infos = 1;
            } else if (strcmp(argc[i], "-o") == 0) {
                if (i+1 >= argv)
                    CLAN_error("no output name for -o option");

                // stdout is a special value to set output to standard output.
                if (strcmp(argc[i+1], "stdout") == 0) {
                    *output = stdout;
                } else {
                    *output = fopen(argc[i+1], "w");
                    if (*output == NULL)
                        CLAN_error("cannot open the output file");
                }
                i++;
            } else {
                fprintf(stderr, "[Clan] Warning: unknown %s option.\n", argc[i]);
            }
        } else {
            if (!input_is_stdin) {
                nb_input_files++;
                CLAN_realloc(*input_files, char**, sizeof(char*) * (nb_input_files+1));
                CLAN_strdup((*input_files)[nb_input_files-1], argc[i]);
                (*input_files)[nb_input_files] = NULL;
                // stdin is a special value to set input to standard input.
                if (strcmp(argc[i], "stdin") == 0) {
                    input_is_stdin = 1;
                }
            } else {
                CLAN_error("Cannot have multiple input files with stdin");
            }
        }
    }

    if ((options->precision != OSL_PRECISION_MP) &&
            (options->precision != OSL_PRECISION_SP) &&
            (options->precision != OSL_PRECISION_DP))
        CLAN_error("invalid precision (use 32, 64 or 0 for GMP)");

    if ((options->autoscop || options->autopragma || options->autoinsert) &&
            !nb_input_files )
        CLAN_error("autoscop/autopragma/autoinsert options need an input file");

    if (!input_is_stdin && !nb_input_files && !infos)
        CLAN_error("no input file (-h for help)");

    return options;
}

const char* clan_options_autopragma_file(void) {
    /*
     * This function aims to "improve" the macro CLAN_AUTOPRAGMA_FILE...
     * The function returns a pointer to a static char array! This is dirty but
     * the previous implementation of the macro expanded to a literal string.
     * At least now, two processes of CLAN can run at the same time.
     */
    static char clan_autopragma_filename[128] = { 0 };
    if (!clan_autopragma_filename[0]) {
        strcpy(clan_autopragma_filename, "/tmp/clan_autopragmaXXXXXX");
        int fd = mkstemp(clan_autopragma_filename);
        if (fd == -1)
            CLAN_error("mkstemp");
        if (unlink(clan_autopragma_filename) == -1)
            CLAN_error("unlink");
        if (close(fd) == -1)
            CLAN_error("close");
    }

    return clan_autopragma_filename;
}
