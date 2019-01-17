
/*+------- <| --------------------------------------------------------**
 **         A                     Clan                                **
 **---     /.\   -----------------------------------------------------**
 **   <|  [""M#                  scop.c                               **
 **-   A   | #   -----------------------------------------------------**
 **   /.\ [""M#         First version: 30/04/2008                     **
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
#include <ctype.h>
#include <string.h>

#include <osl/vector.h>
#include <osl/relation.h>
#include <osl/relation_list.h>
#include <osl/statement.h>
#include <osl/strings.h>
#include <osl/extensions/scatnames.h>
#include <osl/extensions/arrays.h>
#include <osl/extensions/coordinates.h>
#include <osl/extensions/clay.h>
#include <osl/extensions/extbody.h>
#include <osl/generic.h>
#include <osl/body.h>
#include <osl/scop.h>
#include <source/parser.h>
#include <clan/macros.h>
#include <clan/options.h>
#include <clan/relation.h>
#include <clan/statement.h>
#include <clan/scop.h>


extern int scanner_scop_start;
extern int scanner_scop_end;
extern int parser_indent;


/*+****************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/

osl_scop_p clan_parse(FILE*, clan_options_p);

/**
 * clan_scop_extract function:
 * this function is a wrapper to the clan_parse function that parses a file to
 * extract a SCoP and returns, if successful, a pointer to the osl_scop_t
 * structure.
 * \param input   The file to parse (already open).
 * \param options Options for file parsing.
 */
osl_scop_p clan_scop_extract(FILE* input, clan_options_p options) {
    return clan_parse(input, options);
}


/**
 * clan_scop_compact function:
 * This function scans the SCoP to put the right number of columns to every
 * relation (during construction we used CLAN_MAX_DEPTH and CLAN_MAX_PARAMETERS
 * to define relation and vector sizes).
 * \param scop The scop to scan to compact matrices.
 */
void clan_scop_compact(osl_scop_p scop) {
    clan_statement_compact(scop->statement, osl_scop_get_nb_parameters(scop));
}


/**
 * clan_scop_print function:
 * this function prints the content of an osl_scop_t structure (*scop)
 * into a file (file, possibly stdout) in the OpenScop textual format.
 * \param[in] file    The file where the information has to be printed.
 * \param[in] scop    The scop structure whose information has to be printed.
 * \param[in] options Clan's general option settings.
 */
void clan_scop_print(FILE* file, osl_scop_p scop, clan_options_p options) {

    if ((scop != NULL) && (options->castle)) {
        fprintf(file, "#                                                     \n");
        fprintf(file, "#          <|                                         \n");
        fprintf(file, "#           A                                         \n");
        fprintf(file, "#          /.\\                                       \n");
        fprintf(file, "#     <|  [\"\"M#                                     \n");
        fprintf(file, "#      A   | #            Clan McCloog Castle         \n");
        fprintf(file, "#     /.\\ [\"\"M#           [Generated by Clan ");
        fprintf(file, "%s]\n", CLAN_VERSION);
        fprintf(file, "#    [\"\"M# | #  U\"U#U                              \n");
        fprintf(file, "#     | #  | #  \\ .:/                                \n");
        fprintf(file, "#     | #  | #___| #                                  \n");
        fprintf(file, "#     | \"--'     .-\"                                \n");
        fprintf(file, "#   |\"-\"-\"-\"-\"-#-#-##                            \n");
        fprintf(file, "#   |     # ## ######                                 \n");
        fprintf(file, "#    \\       .::::'/                                 \n");
        fprintf(file, "#     \\      ::::'/                                  \n");
        fprintf(file, "#   :8a|    # # ##                                    \n");
        fprintf(file, "#   ::88a      ###                                    \n");
        fprintf(file, "#  ::::888a  8a ##::.                                 \n");
        fprintf(file, "#  ::::::888a88a[]::::                                \n");
        fprintf(file, "# :::::::::SUNDOGa8a::::. ..                          \n");
        fprintf(file, "# :::::8::::888:Y8888:::::::::...                     \n");
        fprintf(file, "#::':::88::::888::Y88a______________________________");
        fprintf(file, "________________________\n");
        fprintf(file, "#:: ::::88a::::88a:Y88a                             ");
        fprintf(file, "     __---__-- __\n");
        fprintf(file, "#' .: ::Y88a:::::8a:Y88a                            ");
        fprintf(file, "__----_-- -------_-__\n");
        fprintf(file, "#  :' ::::8P::::::::::88aa.                   _ _- -");
        fprintf(file, "-  --_ --- __  --- __--\n");
        fprintf(file, "#.::  :::::::::::::::::::Y88as88a...s88aa.\n#\n");
    }

    if (options->outscoplib)
        osl_scop_print_scoplib(file, scop);
    else
        osl_scop_print(file, scop);
}


/**
 * clan_scop_generate_scatnames function:
 * this function generates a scatnames extension for the scop passed as
 * an argument. Since Clan use a "2d+1" scattering strategy, the
 * scattering dimension names are generated by reusing the original
 * iterator names of the deepest statement and by inserting between those
 * names some beta vector elements (the Xth beta element is called bX).
 * \param[in,out] scop The scop to add a scatnames extension to.
 */
void clan_scop_generate_scatnames(osl_scop_p scop) {
    osl_statement_p current, deepest;
    osl_scatnames_p scatnames;
    osl_strings_p iterators = NULL;
    osl_strings_p names = NULL;
    osl_generic_p extension;
    osl_body_p body = NULL;
    char buffer[CLAN_MAX_STRING];
    int max_depth = -1;
    int i;

    // Find the deepest statement to reuse its original iterators.
    current = scop->statement;
    while (current != NULL) {
        if (current->domain->nb_output_dims > max_depth) {
            max_depth = current->domain->nb_output_dims;
            deepest = current;
            body = (osl_body_p)osl_generic_lookup(deepest->extension, OSL_URI_BODY);
            if (body)
                iterators = body->iterators;
        }
        current = current->next;
    }

    // It there are no scattering dimension, do nothing.
    if (max_depth <= 0)
        return;

    // Create the NULL-terminated list of scattering dimension names.
    names = osl_strings_malloc();
    for (i = 0; i < max_depth; i++) {
        sprintf(buffer, "b%d", i);
        osl_strings_add(names, buffer);
        osl_strings_add(names, iterators->string[i]);
    }
    sprintf(buffer, "b%d", max_depth);
    osl_strings_add(names, buffer);

    // Build the scatnames extension.
    scatnames = osl_scatnames_malloc();
    scatnames->names = names;

    // Build the generic extension and insert it to the extension list.
    extension = osl_generic_malloc();
    extension->interface = osl_scatnames_interface();
    extension->data = scatnames;
    osl_generic_add(&scop->extension, extension);
}


/**
 * clan_scop_generate_coordinates function:
 * this function generates a coordinates extension for the scop passed as
 * an argument.
 * \param[in]     name The name of the SCoP original file.
 * \param[in,out] scop The scop to add a scatnames extension to.
 */
void clan_scop_generate_coordinates(osl_scop_p scop, char* name) {
    osl_coordinates_p coordinates;
    osl_generic_p extension;

    // Build the coordinates extension
    coordinates = osl_coordinates_malloc();
    CLAN_strdup(coordinates->name, name);
    coordinates->line_start   = scanner_scop_start + 1;
    coordinates->line_end     = scanner_scop_end;
    coordinates->column_start = 0;
    coordinates->column_end   = 0;
    coordinates->indent = (parser_indent != CLAN_UNDEFINED) ? parser_indent : 0;

    // Build the generic extension and insert it to the extension list.
    extension = osl_generic_malloc();
    extension->interface = osl_coordinates_interface();
    extension->data = coordinates;
    osl_generic_add(&scop->extension, extension);
}


/**
 * clan_scop_generate_clay function:
 * this function generates a clay extension for the scop passed as
 * an argument.
 * \param[in,out] scop   The scop to add a clay extension to.
 * \param[in]     script The clay script.
 */
void clan_scop_generate_clay(osl_scop_p scop, char* script) {
    osl_clay_p clay;
    osl_generic_p extension;

    if ((script != NULL) && (strlen(script) > 0)) {
        // Build the clay extension
        clay = osl_clay_malloc();
        CLAN_strdup(clay->script, script);

        // Build the generic extension and insert it to the extension list.
        extension = osl_generic_malloc();
        extension->interface = osl_clay_interface();
        extension->data = clay;
        osl_generic_add(&scop->extension, extension);
    }
}


/**
 * clan_scop_update_coordinates function:
 * this function replaces the values in the coordinates extension of
 * each SCoP of the list 'scop' by those in the 'coordinates' array.
 * The rows of the coordinates array have the following meaning:
 * 0: line start, 1: line end, 2: column start, 3: column end,
 * 4: boolean set to 1 for an auto-discovered scop, 0 for user-scop.
 * The ith column of the coordinates array describes the coordinates
 * of the ith SCoP.
 * \param[in,out] scop        SCoP list to update the coordinates.
 * \param[in]     coordinates Array of coordinates.
 */
void clan_scop_update_coordinates(osl_scop_p scop,
                                  int coordinates[5][CLAN_MAX_SCOPS]) {
    int i = 0;
    osl_coordinates_p old;

    while (scop != NULL) {
        if (i > CLAN_MAX_SCOPS)
            CLAN_error("too many SCoPs! Change CLAN_MAX_SCOPS and recompile Clan.");

        old = osl_generic_lookup(scop->extension, OSL_URI_COORDINATES);
        if (old == NULL)
            CLAN_error("coordinates extension not present");
        // When columns are at 0, it means the scop has not been autodetected.
        // - The line starts at +1 (after the pragma scop) if no autodetection,
        // - The column stops at -1 (previous read char) if autodetection.
        old->line_start   = coordinates[0][i] + ((coordinates[2][i] == 0)? 1 : 0);
        old->line_end     = coordinates[1][i];
        old->column_start = coordinates[2][i];
        old->column_end   = coordinates[3][i] - ((coordinates[3][i] > 0)? 1 : 0);
        i++;
        scop = scop->next;
    }
}


/**
 * clan_scop_print_autopragma function:
 * this function prints a copy of the input file 'input' to a file
 * named by the CLAN_AUTOPRAGMA_FILE macro, where the SCoP pragmas
 * for 'nb_scops' SCoPs are inserted according to the coordinates array.
 * The rows of the coordinates array have the following meaning:
 * 0: line start, 1: line end, 2: column start, 3: column end,
 * 4: boolean set to 1 for an auto-discovered scop, 0 for user-scop.
 * \param[in] input       The input stream (must be open).
 * \param[in] nb_scops    The number of scops.
 * \param[in] coordinates The array of coordinates for each SCoPs.
 */
void clan_scop_print_autopragma(FILE* input, int nb_scops,
                                int coordinates[5][CLAN_MAX_SCOPS]) {
    int i, j, line, column;
    char c;
    FILE* autopragma;

    if (CLAN_DEBUG) {
        CLAN_debug("coordinates:");
        for (i = 0; i < 5; i++) {
            for (j = 0; j < nb_scops; j++)
                printf("%3d ", coordinates[i][j]);
            printf("\n");
        }
    }

    if ((autopragma = fopen(CLAN_AUTOPRAGMA_FILE, "w")) == NULL)
        CLAN_error("cannot create the autopragma file");
    line = 1;
    column = 1;
    i = 0;
    while ((c = fgetc(input)) != EOF) {
        if (nb_scops > 0) {
            if ((line == coordinates[0][i]) && (column == coordinates[2][i])) {
                fprintf(autopragma, "\n#pragma scop\n");
                for (j = 0; j < coordinates[2][i] - 1; j++)
                    fprintf(autopragma, " ");
            }
            if ((line == coordinates[1][i]) && (column == coordinates[3][i])) {
                fprintf(autopragma, "\n#pragma endscop\n");
                for (j = 0; j < coordinates[3][i] - 1; j++)
                    fprintf(autopragma, " ");
                if (i < nb_scops - 1) {
                    do
                        i++;
                    while ((i < nb_scops - 1) && !coordinates[4][i]);
                }
            }
        }
        fputc(c, autopragma);
        column++;
        if (c == '\n') {
            line++;
            column = 1;
        }
    }
    fclose(autopragma);
}


/**
 * clan_scop_no_pragma function:
 * this function returns CLAN_FALSE if there is a "#pragma scop" at the
 * beginning of the line number "line_start" of the file "filename",
 * CLAN_TRUE otherwise.
 * \param[in] filename   Name of the file to be checked.
 * \param[in] line_start Line number to check.
 * \return 0 if filename's line_start line starts with "#pragma scop", resp. 1.
 */
static
int clan_scop_no_pragma(char * filename, int line_start) {
    int lines = 0;
    int read = 1;
    char c;
    FILE* file;
    char s1[CLAN_MAX_STRING];
    char s2[CLAN_MAX_STRING];

    if (line_start < 0)
        CLAN_error("negative line number");

    if (!(file = fopen(filename, "r")))
        CLAN_error("unable to read the file");

    // Go to line_start in the file.
    while ((lines < line_start - 1) && (read != EOF)) {
        read = fscanf(file, "%c", &c);
        if (read != EOF) {
            if (c == '\n')
                lines ++;
        }
    }

    if (lines != line_start - 1) {
        fclose(file);
        CLAN_error("not enough lines in the file");
    }

    if (fscanf(file, " %s %s", s1, s2) != 2) {
        fclose(file);
        CLAN_debug("pragma not found: cannot read the two chains");
        return CLAN_TRUE;
    }

    fclose(file);
    if (strcmp(s1, "#pragma") || strcmp(s2, "scop")) {
        CLAN_debug("pragma not found: do not match \"#pragma scop\"");
        return CLAN_TRUE;
    }

    CLAN_debug("pragma found");
    return CLAN_FALSE;
}


/**
 * clan_scop_insert_pragmas function:
 * inserts "#pragma scop" and "#pragma endscop" in a source file
 * around the SCoPs related in the input SCoP list that have no
 * surrounding pragmas in the file.
 * \param[in] scop     The list of SCoPS.
 * \param[in] filename Name of the file where to insert pragmas.
 * \param[in] test     0 to insert, 1 to leave the result in the
 *                     CLAN_AUTOPRAGMA_FILE temporary file.
 */
void clan_scop_insert_pragmas(osl_scop_p scop, char* filename, int test) {
    int i, j, n = 0;
    int infos[5][CLAN_MAX_SCOPS];
    int tmp[5];
    osl_coordinates_p coordinates;
    FILE* input, *output;
    size_t size;
    char buffer[BUFSIZ];

    // Get coordinate information from the list of SCoPS.
    while (scop != NULL) {
        coordinates = osl_generic_lookup(scop->extension, OSL_URI_COORDINATES);
        infos[0][n] = coordinates->line_start;
        infos[1][n] = coordinates->line_end;
        infos[2][n] = coordinates->column_start;
        infos[3][n] = coordinates->column_end + 1;
        infos[4][n] = clan_scop_no_pragma(filename, coordinates->line_start);
        n++;
        scop = scop->next;
    }

    // Dirty and inefficient bubble sort to ensure the SCoP ordering is correct
    // (this is ensured in Clan, but not if it is called from outside...).
    for (i = n - 2; i >= 0; i--) {
        for (j = 0; j <= i; j++) {
            if (infos[0][j] > infos[0][j+1]) {
                tmp[0]=infos[0][j];
                infos[0][j]=infos[0][j+1];
                infos[0][j+1]=tmp[0];
                tmp[1]=infos[1][j];
                infos[1][j]=infos[1][j+1];
                infos[1][j+1]=tmp[1];
                tmp[2]=infos[2][j];
                infos[2][j]=infos[2][j+1];
                infos[2][j+1]=tmp[2];
                tmp[3]=infos[3][j];
                infos[3][j]=infos[3][j+1];
                infos[3][j+1]=tmp[3];
                tmp[4]=infos[4][j];
                infos[4][j]=infos[4][j+1];
                infos[4][j+1]=tmp[4];
            }
        }
    }

    // Quick check that there is no scop interleaving.
    for (i = 0; i < n - 1; i++)
        if (infos[1][i] > infos[0][i+1])
            CLAN_error("SCoP interleaving");

    // Generate the temporary file with the pragma inserted.
    if (!(input = fopen(filename, "r")))
        CLAN_error("unable to read the input file");
    clan_scop_print_autopragma(input, n, infos);
    fclose(input);

    // Replace the original file, or keep the temporary file.
    if (!test) {
        if (!(input = fopen(CLAN_AUTOPRAGMA_FILE, "rb")))
            CLAN_error("unable to read the temporary file");

        if (!(output = fopen(filename, "wb")))
            CLAN_error("unable to write the output file");

        while ((size = fread(buffer, 1, BUFSIZ, input))) {
            fwrite(buffer, 1, size, output);
        }

        fclose(input);
        fclose(output);

        if (remove(CLAN_AUTOPRAGMA_FILE) == -1)
            CLAN_warning("unable to remove the temporary file");
    }
}


/**
 * clan_scop_simplify function:
 * this function tries to simplify the iteration domains of the SCoP.
 * /param[in,out] scop SCoP to simplify (updated).
 */
void clan_scop_simplify(osl_scop_p scop) {
    osl_statement_p statement;

    while (scop != NULL) {
        statement = scop->statement;
        while (statement != NULL) {
            clan_relation_simplify(statement->domain);
            statement = statement->next;
        }
        scop = scop->next;
    }
}
