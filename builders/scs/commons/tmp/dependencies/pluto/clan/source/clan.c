
/*+------- <| --------------------------------------------------------**
 **         A                     Clan                                **
 **---     /.\   -----------------------------------------------------**
 **   <|  [""M#                  clan.c                               **
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
#include <string.h>

#include <clan/clan.h>
#include <osl/scop.h>

int main(int argc, char* argv[]) {
    osl_scop_p scop = NULL;
    clan_options_p options;
    FILE* input, *output, *autopragma;
    char **input_files = NULL;
    char c;
    int i;

    // Options and input/output file setting.
    options = clan_options_read(argc, argv, &input_files, &output);

    i = 0;
    while (input_files[i]) {
        if (options->name)
            free(options->name);
        options->name = strdup(input_files[i]);

        if (!strcmp(options->name, "stdin"))
            input = stdin;
        else
            input = fopen(options->name, "r");

        if (input == NULL) {
            fprintf(stderr, "[Clan] Error: Unable to open input file %s\n",
                    input_files[i]);
            break;
        }

        printf("[Clan] Info: parsing file #%d (%s)\n", i+1, input_files[i]);

        // Extraction of the polyhedral representation of the SCoP from the input.
        if (input != NULL) {
            if (options->inputscop) {
                // Input is a .scop file.
                scop = osl_scop_read(input);
            } else {
                // Input is a source code.
                if (scop)
                    osl_scop_free(scop);
                scop = clan_scop_extract(input, options);
                fclose(input);
            }

            // Printing of the internal data structure of the SCoP if asked.
            if (options->structure)
                osl_scop_dump(stdout, scop);

            if (!options->autopragma && !options->autoinsert) {
                // Generation of the .scop output file.
                clan_scop_print(output, scop, options);
            } else {
                if (options->autopragma) {
                    clan_scop_insert_pragmas(scop, options->name, 1);
                    // Output the file with inserted SCoP pragmas.
                    if ((autopragma = fopen(CLAN_AUTOPRAGMA_FILE, "r")) == NULL)
                        CLAN_error("cannot read the temporary file");
                    while ((c = fgetc(autopragma)) != EOF)
                        fputc(c, output);
                    fclose(autopragma);
                    remove(CLAN_AUTOPRAGMA_FILE);
                }
                if (options->autoinsert) {
                    clan_scop_insert_pragmas(scop, options->name, 0);
                }
            }
        }

        i++;
    }

    // Save the planet.
    i = 0;
    while (input_files[i]) {
        free(input_files[i]);
        i++;
    }
    free(input_files);

    clan_options_free(options);
    osl_scop_free(scop);
    fclose(output);

    return 0;
}
