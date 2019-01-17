
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                      extensions/clay.c                          **
 **-----------------------------------------------------------------**
 **                   First version: 09/05/2012                     **
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
#include <osl/extensions/clay.h>


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * osl_clay_idump function:
 * this function displays an osl_clay_t structure (*clay) into a
 * file (file, possibly stdout) in a way that trends to be understandable. It
 * includes an indentation level (level) in order to work with others
 * idump functions.
 * \param[in] file  The file where the information has to be printed.
 * \param[in] clay  The clay structure to print.
 * \param[in] level Number of spaces before printing, for each line.
 */
void osl_clay_idump(FILE * file, osl_clay_p clay, int level) {
    int j;
    size_t l;
    char * tmp;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file, "|\t");

    if (clay != NULL)
        fprintf(file, "+-- osl_clay_t\n");
    else
        fprintf(file, "+-- NULL clay\n");

    if (clay != NULL) {
        // Go to the right level.
        for(j = 0; j <= level; j++)
            fprintf(file, "|\t");

        // Display the clay script (without any carriage return).
        OSL_strdup(tmp, clay->script);
        for (l = 0; l < strlen(tmp); l++)
            if (tmp[l] == '\n')
                tmp[l] = ' ';
        fprintf(file, "script: %s\n", tmp);
        free(tmp);
    }

    // The last line.
    for (j = 0; j <= level; j++)
        fprintf(file, "|\t");
    fprintf(file, "\n");
}


/**
 * osl_clay_dump function:
 * this function prints the content of an osl_clay_t structure
 * (*clay) into a file (file, possibly stdout).
 * \param[in] file The file where the information has to be printed.
 * \param[in] clay The clay structure to print.
 */
void osl_clay_dump(FILE * file, osl_clay_p clay) {
    osl_clay_idump(file, clay, 0);
}


/**
 * osl_clay_sprint function:
 * this function prints the content of an osl_clay_t structure
 * (*clay) into a string (returned) in the OpenScop textual format.
 * \param[in] clay The clay structure to print.
 * \return A string containing the OpenScop dump of the clay structure.
 */
char * osl_clay_sprint(osl_clay_p clay) {
    size_t high_water_mark = OSL_MAX_STRING;
    char * string = NULL;
    char buffer[OSL_MAX_STRING];

    if (clay != NULL) {
        OSL_malloc(string, char *, high_water_mark * sizeof(char));
        string[0] = '\0';

        // Print the clay.
        sprintf(buffer, "%s", clay->script);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        // Keep only the memory space we need.
        OSL_realloc(string, char *, (strlen(string) + 1) * sizeof(char));
    }

    return string;
}


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/**
 * osl_clay_sread function:
 * this function reads a clay structure from a string complying to the
 * OpenScop textual format and returns a pointer to this clay structure.
 * The input parameter is updated to the position in the input string this
 * function reach right after reading the clay structure.
 * \param[in,out] input The input string where to find a clay.
 *                      Updated to the position after what has been read.
 * \return A pointer to the clay structure that has been read.
 */
osl_clay_p osl_clay_sread(char ** input) {
    osl_clay_p clay;
    char * script;

    if (*input == NULL) {
        OSL_debug("no clay optional tag");
        return NULL;
    }

    if (strlen(*input) > OSL_MAX_STRING)
        OSL_error("clay script too long");

    // Build the clay structure
    clay = osl_clay_malloc();
    script = *input;

    // Pass the carriage returns (this allows to remove those inserted by
    // osl_generic_print), and copy the textual script.
    while (*script && (*script == '\n'))
        script++;
    OSL_strdup(clay->script, script);

    // Update the input pointer (everything has been read).
    input += strlen(*input);

    return clay;
}


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/


/**
 * osl_clay_malloc function:
 * this function allocates the memory space for an osl_clay_t
 * structure and sets its fields with default values. Then it returns a
 * pointer to the allocated space.
 * \return A pointer to an empty clay structure with fields set to
 *         default values.
 */
osl_clay_p osl_clay_malloc(void) {
    osl_clay_p clay;

    OSL_malloc(clay, osl_clay_p, sizeof(osl_clay_t));
    clay->script = NULL;

    return clay;
}


/**
 * osl_clay_free function:
 * this function frees the allocated memory for an osl_clay_t
 * structure.
 * \param[in,out] clay The pointer to the clay structure to free.
 */
void osl_clay_free(osl_clay_p clay) {
    if (clay != NULL) {
        if(clay->script != NULL)
            free(clay->script);
        free(clay);
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_clay_clone function:
 * this function builds and returns a "hard copy" (not a pointer copy) of an
 * osl_clay_t data structure.
 * \param[in] clay The pointer to the clay structure to clone.
 * \return A pointer to the clone of the clay structure.
 */
osl_clay_p osl_clay_clone(osl_clay_p clay) {
    osl_clay_p clone;

    if (clay == NULL)
        return NULL;

    clone = osl_clay_malloc();
    OSL_strdup(clone->script, clay->script);

    return clone;
}


/**
 * osl_clay_equal function:
 * this function returns true if the two clay structures are the same
 * (content-wise), false otherwise.
 * \param[in] c1  The first clay structure.
 * \param[in] c2  The second clay structure.
 * \return 1 if c1 and c2 are the same (content-wise), 0 otherwise.
 */
int osl_clay_equal(osl_clay_p c1, osl_clay_p c2) {
    if (c1 == c2)
        return 1;

    if (((c1 == NULL) && (c2 != NULL)) || ((c1 != NULL) && (c2 == NULL))) {
        OSL_info("clay extensions are not the same");
        return 0;
    }

    if (strcmp(c1->script, c2->script)) {
        OSL_info("clay scripts are not the same");
        return 0;
    }

    return 1;
}


/**
 * osl_clay_interface function:
 * this function creates an interface structure corresponding to the clay
 * extension and returns it).
 * \return An interface structure for the clay extension.
 */
osl_interface_p osl_clay_interface(void) {
    osl_interface_p interface = osl_interface_malloc();

    OSL_strdup(interface->URI, OSL_URI_CLAY);
    interface->idump  = (osl_idump_f)osl_clay_idump;
    interface->sprint = (osl_sprint_f)osl_clay_sprint;
    interface->sread  = (osl_sread_f)osl_clay_sread;
    interface->malloc = (osl_malloc_f)osl_clay_malloc;
    interface->free   = (osl_free_f)osl_clay_free;
    interface->clone  = (osl_clone_f)osl_clay_clone;
    interface->equal  = (osl_equal_f)osl_clay_equal;

    return interface;
}
