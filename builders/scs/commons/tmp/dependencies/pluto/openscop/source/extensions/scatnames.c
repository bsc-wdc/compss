
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                    extensions/scatnames.c                       **
 **-----------------------------------------------------------------**
 **                   First version: 03/12/2011                     **
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
#include <osl/strings.h>
#include <osl/extensions/scatnames.h>


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * osl_scatnames_idump function:
 * this function displays an osl_scatnames_t structure (*scatnames) into a
 * file (file, possibly stdout) in a way that trends to be understandable. It
 * includes an indentation level (level) in order to work with others
 * idump functions.
 * \param[in] file      The file where the information has to be printed.
 * \param[in] scatnames Scatnames structure to print.
 * \param[in] level     Number of spaces before printing, for each line.
 */
void osl_scatnames_idump(FILE * file, osl_scatnames_p scatnames, int level) {
    int j;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file, "|\t");

    if (scatnames != NULL)
        fprintf(file, "+-- osl_scatnames_t\n");
    else
        fprintf(file, "+-- NULL scatnames\n");

    if (scatnames != NULL) {
        // Go to the right level.
        for(j = 0; j <= level + 1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        // Display the list of scattering names.
        osl_strings_idump(file, scatnames->names, level + 1);
    }

    // The last line.
    for (j = 0; j <= level; j++)
        fprintf(file, "|\t");
    fprintf(file, "\n");
}


/**
 * osl_scatnames_dump function:
 * this function prints the content of an osl_scatnames_t structure
 * (*scatnames) into a file (file, possibly stdout).
 * \param[in] file      The file where the information has to be printed.
 * \param[in] scatnames The scatnames structure to print.
 */
void osl_scatnames_dump(FILE * file, osl_scatnames_p scatnames) {
    osl_scatnames_idump(file, scatnames, 0);
}


/**
 * osl_scatnames_sprint function:
 * this function prints the content of an osl_scatnames_t structure
 * (*scatnames) into a string (returned) in the OpenScop textual format.
 * \param[in] scatnames The scatnames structure to print.
 * \return A string containing the OpenScop dump of the scatnames structure.
 */
char * osl_scatnames_sprint(osl_scatnames_p scatnames) {
    return osl_strings_sprint(scatnames->names);
}


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/**
 * osl_scatnames_sread function:
 * this function reads a scatnames structure from a string complying to the
 * OpenScop textual format and returns a pointer to this scatnames structure.
 * The input parameter is updated to the position in the input string this
 * function reach right after reading the scatnames structure. If there
 * is nothing to read, the function returns NULL.
 * \param[in,out] input The input string where to find a scatnames.
 *                      Updated to the position after what has been read.
 * \return A pointer to the scatnames structure that has been read.
 */
osl_scatnames_p osl_scatnames_sread(char ** input) {
    osl_scatnames_p scatnames = NULL;
    osl_strings_p names = NULL;

    if (*input == NULL) {
        OSL_debug("no scatnames optional tag");
        return NULL;
    }

    // Build the scatnames structure
    names = osl_strings_sread(input);
    if (names != NULL) {
        scatnames = osl_scatnames_malloc();
        scatnames->names = names;
    }

    return scatnames;
}


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/


/**
 * osl_scatnames_malloc function:
 * this function allocates the memory space for an osl_scatnames_t
 * structure and sets its fields with default values. Then it returns a
 * pointer to the allocated space.
 * \return A pointer to an empty scatnames structure with fields set to
 *         default values.
 */
osl_scatnames_p osl_scatnames_malloc(void) {
    osl_scatnames_p scatnames;

    OSL_malloc(scatnames, osl_scatnames_p, sizeof(osl_scatnames_t));
    scatnames->names = NULL;

    return scatnames;
}


/**
 * osl_scatnames_free function:
 * this function frees the allocated memory for an osl_scatnames_t
 * structure.
 * \param[in,out] scatnames The pointer to the scatnames structure to free.
 */
void osl_scatnames_free(osl_scatnames_p scatnames) {
    if (scatnames != NULL) {
        osl_strings_free(scatnames->names);
        free(scatnames);
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_scatnames_clone function:
 * This function builds and returns a "hard copy" (not a pointer copy) of an
 * osl_scatnames_t data structure.
 * \param[in] scatnames The pointer to the scatnames structure to clone.
 * \return A pointer to the clone of the scatnames structure.
 */
osl_scatnames_p osl_scatnames_clone(osl_scatnames_p scatnames) {
    osl_scatnames_p clone;

    if (scatnames == NULL)
        return NULL;

    clone = osl_scatnames_malloc();
    clone->names = osl_strings_clone(scatnames->names);

    return clone;
}


/**
 * osl_scatnames_equal function:
 * this function returns true if the two scatnames structures are the same
 * (content-wise), false otherwise.
 * \param[in] s1 The first scatnames structure.
 * \param[in] s2 The second scatnames structure.
 * \return 1 if s1 and s2 are the same (content-wise), 0 otherwise.
 */
int osl_scatnames_equal(osl_scatnames_p s1, osl_scatnames_p s2) {

    if (s1 == s2)
        return 1;

    if (((s1 == NULL) && (s2 != NULL)) || ((s1 != NULL) && (s2 == NULL)))
        return 0;

    if (!osl_strings_equal(s1->names, s2->names))
        return 0;

    return 1;
}


/**
 * osl_scatnames_interface function:
 * this function creates an interface structure corresponding to the scatnames
 * extension and returns it).
 * \return An interface structure for the scatnames extension.
 */
osl_interface_p osl_scatnames_interface(void) {
    osl_interface_p interface = osl_interface_malloc();

    OSL_strdup(interface->URI, OSL_URI_SCATNAMES);
    interface->idump  = (osl_idump_f)osl_scatnames_idump;
    interface->sprint = (osl_sprint_f)osl_scatnames_sprint;
    interface->sread  = (osl_sread_f)osl_scatnames_sread;
    interface->malloc = (osl_malloc_f)osl_scatnames_malloc;
    interface->free   = (osl_free_f)osl_scatnames_free;
    interface->clone  = (osl_clone_f)osl_scatnames_clone;
    interface->equal  = (osl_equal_f)osl_scatnames_equal;

    return interface;
}
