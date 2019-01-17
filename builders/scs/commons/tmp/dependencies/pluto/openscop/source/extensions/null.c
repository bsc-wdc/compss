
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                       extensions/null.c                         **
 **-----------------------------------------------------------------**
 **                   First version: 28/06/2012                     **
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
#include <osl/extensions/null.h>


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * osl_null_idump function:
 * this function displays an osl_null_t structure (*null) into a
 * file (file, possibly stdout) in a way that trends to be understandable. It
 * includes an indentation level (level) in order to work with others
 * idump functions.
 * \param[in] file  The file where the information has to be printed.
 * \param[in] null  The null structure to print.
 * \param[in] level Number of spaces before printing, for each line.
 */
void osl_null_idump(FILE * file, osl_null_p null, int level) {
    int j;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file, "|\t");

    if (null != NULL)
        fprintf(file, "+-- osl_null_t\n");
    else
        fprintf(file, "+-- NULL null\n");

    // The last line.
    for (j = 0; j <= level; j++)
        fprintf(file, "|\t");
    fprintf(file, "\n");
}


/**
 * osl_null_dump function:
 * this function prints the content of an osl_null_t structure
 * (*null) into a file (file, possibly stdout).
 * \param[in] file The file where the information has to be printed.
 * \param[in] null The null structure to print.
 */
void osl_null_dump(FILE * file, osl_null_p null) {
    osl_null_idump(file, null, 0);
}


/**
 * osl_null_sprint function:
 * this function prints the content of an osl_null_t structure
 * (*null) into a string (returned) in the OpenScop textual format.
 * \param[in] null The null structure to print.
 * \return A string containing the OpenScop dump of the null structure.
 */
char * osl_null_sprint(osl_null_p null) {
    char * string = NULL;

    if (null != NULL) {
        // Print nothing.
        OSL_malloc(string, char *, sizeof(char));
        string[0] = '\0';
    }

    return string;
}


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/**
 * osl_null_sread function:
 * this function reads a null structure from a string complying to the
 * OpenScop textual format and returns a pointer to this null structure.
 * The input parameter is updated to the position in the input string this
 * function reach right after reading the null structure.
 * \param[in,out] input The input string where to find a null.
 *                      Updated to the position after what has been read.
 * \return A pointer to the null structure that has been read.
 */
osl_null_p osl_null_sread(char ** input) {
    osl_null_p null;

    if (*input == NULL) {
        OSL_debug("no null optional tag");
        return NULL;
    }

    // Build the null structure
    null = osl_null_malloc();

    // Update the input pointer (everything has been read and ignored).
    input += strlen(*input);

    return null;
}


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/


/**
 * osl_null_malloc function:
 * this function allocates the memory space for an osl_null_t
 * structure and sets its fields with default values. Then it returns a
 * pointer to the allocated space.
 * \return A pointer to an empty null structure with fields set to
 *         default values.
 */
osl_null_p osl_null_malloc(void) {
    osl_null_p null;

    OSL_malloc(null, osl_null_p, sizeof(osl_null_t));
    return null;
}


/**
 * osl_null_free function:
 * this function frees the allocated memory for an osl_null_t
 * structure.
 * \param[in,out] null The pointer to the null structure to free.
 */
void osl_null_free(osl_null_p null) {
    if (null != NULL) {
        free(null);
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_null_clone function:
 * this function builds and returns a "hard copy" (not a pointer copy) of an
 * osl_null_t data structure.
 * \param[in] null The pointer to the null structure to clone.
 * \return A pointer to the clone of the null structure.
 */
osl_null_p osl_null_clone(osl_null_p null) {
    osl_null_p clone;

    if (null == NULL)
        return NULL;

    clone = osl_null_malloc();
    return clone;
}


/**
 * osl_null_equal function:
 * this function returns true if the two null structures are the same
 * (content-wise), false otherwise.
 * \param[in] c1  The first null structure.
 * \param[in] c2  The second null structure.
 * \return 1 if c1 and c2 are the same (content-wise), 0 otherwise.
 */
int osl_null_equal(osl_null_p c1, osl_null_p c2) {
    if (c1 == c2)
        return 1;

    if (((c1 == NULL) && (c2 != NULL)) || ((c1 != NULL) && (c2 == NULL))) {
        OSL_info("nulls are not the same");
        return 0;
    }

    return 1;
}


/**
 * osl_null_interface function:
 * this function creates an interface structure corresponding to the null
 * extension and returns it).
 * \return An interface structure for the null extension.
 */
osl_interface_p osl_null_interface(void) {
    osl_interface_p interface = osl_interface_malloc();

    OSL_strdup(interface->URI, OSL_URI_NULL);
    interface->idump  = (osl_idump_f)osl_null_idump;
    interface->sprint = (osl_sprint_f)osl_null_sprint;
    interface->sread  = (osl_sread_f)osl_null_sread;
    interface->malloc = (osl_malloc_f)osl_null_malloc;
    interface->free   = (osl_free_f)osl_null_free;
    interface->clone  = (osl_clone_f)osl_null_clone;
    interface->equal  = (osl_equal_f)osl_null_equal;

    return interface;
}
