
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                    extensions/coordinates.c                     **
 **-----------------------------------------------------------------**
 **                   First version: 07/12/2010                     **
 **-----------------------------------------------------------------**


*****************************************************************************
* OpenScop: Structures and formats for polyhedral tools to talk together    *
*****************************************************************************
*    ,___,,_,__,,__,,__,,__,,_,__,,_,__,,__,,___,_,__,,_,__,                *
*    /   / /  //  //  //  // /   / /  //  //   / /  // /  /|,_,             *
*   /   / /  //  //  //  // /   / /  //  //   / /  // /  / / /\             *
*  |~~~|~|~~~|~~~|~~~|~~~|~|~~~|~|~~~|~~~|~~~|~|~~~|~|~~~|/_/  \            *
*  | G |C| P | = | L | P |=| = |C| = | = | = |=| = |=| C |\  \ /\           *
*  | R |l| o | = | e | l |=| = |a| = | = | = |=| = |=| L | \# \ /\          *
*  | A |a| l | = | t | u |=| = |n| = | = | = |=| = |=| o | |\# \  \         *
*  | P |n| l | = | s | t |=| = |d| = | = | = | |   |=| o | | \# \  \        *
*  | H | | y |   | e | o | | = |l|   |   | = | |   | | G | |  \  \  \       *
*  | I | |   |   | e |   | |   | |   |   |   | |   | |   | |   \  \  \      *
*  | T | |   |   |   |   | |   | |   |   |   | |   | |   | |    \  \  \     *
*  | E | |   |   |   |   | |   | |   |   |   | |   | |   | |     \  \  \    *
*  | * |*| * | * | * | * |*| * |*| * | * | * |*| * |*| * | /      \* \  \   *
*  | O |p| e | n | S | c |o| p |-| L | i | b |r| a |r| y |/        \  \ /   *
*  '---'-'---'---'---'---'-'---'-'---'---'---'-'---'-'---'          '--'    *
*                                                                           *
* Copyright (C) 2008 University Paris-Sud 11 and INRIA                      *
*                                                                           *
* (3-clause BSD license)                                                    *
* Redistribution and use in source  and binary forms, with or without       *
* modification, are permitted provided that the following conditions        *
* are met:                                                                  *
*                                                                           *
* 1. Redistributions of source code must retain the above copyright notice, *
*    this list of conditions and the following disclaimer.                  *
* 2. Redistributions in binary form must reproduce the above copyright      *
*    notice, this list of conditions and the following disclaimer in the    *
*    documentation and/or other materials provided with the distribution.   *
* 3. The name of the author may not be used to endorse or promote products  *
*    derived from this software without specific prior written permission.  *
*                                                                           *
* THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR      *
* IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES *
* OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.   *
* IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,          *
* INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT  *
* NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, *
* DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY     *
* THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT       *
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF  *
* THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.         *
*                                                                           *
* OpenScop Library, a library to manipulate OpenScop formats and data       *
* structures. Written by:                                                   *
* Cedric Bastoul     <Cedric.Bastoul@u-psud.fr> and                         *
* Louis-Noel Pouchet <Louis-Noel.pouchet@inria.fr>                          *
*                                                                           *
*****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <osl/macros.h>
#include <osl/util.h>
#include <osl/interface.h>
#include <osl/extensions/coordinates.h>


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * osl_coordinates_idump function:
 * this function displays an osl_coordinates_t structure (*coordinates) into a
 * file (file, possibly stdout) in a way that trends to be understandable. It
 * includes an indentation level (level) in order to work with others
 * idump functions.
 * \param file        The file where the information has to be printed.
 * \param coordinates The coordinates structure to print.
 * \param level       Number of spaces before printing, for each line.
 */
void osl_coordinates_idump(FILE* file, osl_coordinates_p coordinates,
                           int level) {
    int j;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file, "|\t");

    if (coordinates != NULL)
        fprintf(file, "+-- osl_coordinates_t\n");
    else
        fprintf(file, "+-- NULL coordinates\n");

    if (coordinates != NULL) {
        // Go to the right level.
        for(j = 0; j <= level; j++)
            fprintf(file, "|\t");

        // Display the file name.
        if (coordinates->name != NULL)
            fprintf(file, "File name__: %s\n", coordinates->name);
        else
            fprintf(file, "NULL file name\n");

        // Go to the right level.
        for(j = 0; j <= level; j++)
            fprintf(file, "|\t");

        // Display the lines.
        fprintf(file, "Coordinates: [%d,%d -> %d,%d]\n",
                coordinates->line_start, coordinates->column_start,
                coordinates->line_end,   coordinates->column_end);

        // Go to the right level.
        for(j = 0; j <= level; j++)
            fprintf(file, "|\t");

        // Display the indentation.
        fprintf(file, "Indentation: %d\n", coordinates->indent);
    }

    // The last line.
    for (j = 0; j <= level; j++)
        fprintf(file, "|\t");
    fprintf(file, "\n");
}


/**
 * osl_coordinates_dump function:
 * this function prints the content of an osl_coordinates_t structure
 * (*coordinates) into a file (file, possibly stdout).
 * \param file        The file where the information has to be printed.
 * \param coordinates The coordinates structure to print.
 */
void osl_coordinates_dump(FILE* file, osl_coordinates_p coordinates) {
    osl_coordinates_idump(file, coordinates, 0);
}


/**
 * osl_coordinates_sprint function:
 * this function prints the content of an osl_coordinates_t structure
 * (*coordinates) into a string (returned) in the OpenScop textual format.
 * \param  coordinates The coordinates structure to be print.
 * \return A string containing the OpenScop dump of the coordinates structure.
 */
char* osl_coordinates_sprint(osl_coordinates_p coordinates) {
    size_t high_water_mark = OSL_MAX_STRING;
    char* string = NULL;
    char buffer[OSL_MAX_STRING];

    if (coordinates != NULL) {
        OSL_malloc(string, char*, high_water_mark * sizeof(char));
        string[0] = '\0';

        // Print the coordinates content.
        sprintf(buffer, "# File name\n%s\n", coordinates->name);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        sprintf(buffer, "# Starting line and column\n%d %d\n",
                coordinates->line_start, coordinates->column_start);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        sprintf(buffer, "# Ending line and column\n%d %d\n",
                coordinates->line_end, coordinates->column_end);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        sprintf(buffer, "# Indentation\n%d\n", coordinates->indent);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        // Keep only the memory space we need.
        OSL_realloc(string, char*, (strlen(string) + 1) * sizeof(char));
    }

    return string;
}


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/**
 * osl_coordinates_sread function:
 * this function reads a coordinates structure from a string complying to the
 * OpenScop textual format and returns a pointer to this structure.
 * The input parameter is updated to the position in the input string this
 * function reach right after reading the coordinates structure.
 * \param[in,out] input The input string where to find coordinates.
 *                      Updated to the position after what has been read.
 * \return A pointer to the coordinates structure that has been read.
 */
osl_coordinates_p osl_coordinates_sread(char** input) {
    osl_coordinates_p coordinates;

    if (*input == NULL) {
        OSL_debug("no coordinates optional tag");
        return NULL;
    }

    // Build the coordinates structure.
    coordinates = osl_coordinates_malloc();

    // Read the file name (and path).
    coordinates->name = osl_util_read_line(NULL, input);

    // Read the coordinates.
    coordinates->line_start   = osl_util_read_int(NULL, input);
    coordinates->column_start = osl_util_read_int(NULL, input);
    coordinates->line_end     = osl_util_read_int(NULL, input);
    coordinates->column_end   = osl_util_read_int(NULL, input);

    // Read the indentation level.
    coordinates->indent = osl_util_read_int(NULL, input);

    return coordinates;
}


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/


/**
 * osl_coordinates_malloc function:
 * this function allocates the memory space for an osl_coordinates_t
 * structure and sets its fields with default values. Then it returns a
 * pointer to the allocated space.
 * \return A pointer to an empty coordinates structure with fields set to
 *         default values.
 */
osl_coordinates_p osl_coordinates_malloc(void) {
    osl_coordinates_p coordinates;

    OSL_malloc(coordinates, osl_coordinates_p, sizeof(osl_coordinates_t));
    coordinates->name         = NULL;
    coordinates->line_start   = OSL_UNDEFINED;
    coordinates->column_start = OSL_UNDEFINED;
    coordinates->line_end     = OSL_UNDEFINED;
    coordinates->column_end   = OSL_UNDEFINED;
    coordinates->indent       = OSL_UNDEFINED;

    return coordinates;
}


/**
 * osl_coordinates_free function:
 * this function frees the allocated memory for an osl_coordinates_t
 * structure.
 * \param coordinates The pointer to the coordinates structure to free.
 */
void osl_coordinates_free(osl_coordinates_p coordinates) {
    if (coordinates != NULL) {
        free(coordinates->name);
        free(coordinates);
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_coordinates_clone function:
 * this function builds and returns a "hard copy" (not a pointer copy) of an
 * osl_coordinates_t data structure.
 * \param coordinates The pointer to the coordinates structure to clone.
 * \return A pointer to the clone of the coordinates structure.
 */
osl_coordinates_p osl_coordinates_clone(osl_coordinates_p coordinates) {
    osl_coordinates_p clone;

    if (coordinates == NULL)
        return NULL;

    clone = osl_coordinates_malloc();
    OSL_strdup(clone->name, coordinates->name);
    clone->line_start   = coordinates->line_start;
    clone->column_start = coordinates->column_start;
    clone->line_end     = coordinates->line_end;
    clone->column_end   = coordinates->column_end;
    clone->indent       = coordinates->indent;

    return clone;
}


/**
 * osl_coordinates_equal function:
 * this function returns true if the two coordinates structures are the same
 * (content-wise), false otherwise. This functions considers two coordinates
 * \param c1  The first coordinates structure.
 * \param c2  The second coordinates structure.
 * \return 1 if c1 and c2 are the same (content-wise), 0 otherwise.
 */
int osl_coordinates_equal(osl_coordinates_p c1, osl_coordinates_p c2) {
    if (c1 == c2)
        return 1;

    if (((c1 == NULL) && (c2 != NULL)) || ((c1 != NULL) && (c2 == NULL)))
        return 0;

    if (strcmp(c1->name, c2->name)) {
        OSL_info("file names are not the same");
        return 0;
    }

    if (c1->line_start != c2->line_start) {
        OSL_info("starting lines are not the same");
        return 0;
    }

    if (c1->column_start != c2->column_start) {
        OSL_info("starting columns are not the same");
        return 0;
    }

    if (c1->line_end != c2->line_end) {
        OSL_info("Ending lines are not the same");
        return 0;
    }

    if (c1->column_end != c2->column_end) {
        OSL_info("Ending columns are not the same");
        return 0;
    }

    if (c1->indent != c2->indent) {
        OSL_info("indentations are not the same");
        return 0;
    }

    return 1;
}


/**
 * osl_coordinates_interface function:
 * this function creates an interface structure corresponding to the coordinates
 * extension and returns it).
 * \return An interface structure for the coordinates extension.
 */
osl_interface_p osl_coordinates_interface(void) {
    osl_interface_p interface = osl_interface_malloc();

    OSL_strdup(interface->URI, OSL_URI_COORDINATES);
    interface->idump  = (osl_idump_f)osl_coordinates_idump;
    interface->sprint = (osl_sprint_f)osl_coordinates_sprint;
    interface->sread  = (osl_sread_f)osl_coordinates_sread;
    interface->malloc = (osl_malloc_f)osl_coordinates_malloc;
    interface->free   = (osl_free_f)osl_coordinates_free;
    interface->clone  = (osl_clone_f)osl_coordinates_clone;
    interface->equal  = (osl_equal_f)osl_coordinates_equal;

    return interface;
}
