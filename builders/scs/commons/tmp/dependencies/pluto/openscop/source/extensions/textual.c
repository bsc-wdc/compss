
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                     extensions/textual.c                        **
 **-----------------------------------------------------------------**
 **                   First version: 15/17/2010                     **
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
#include <osl/extensions/textual.h>


/* CAUTION : TEXTUAL IS A VERY SPECIAL CASE: DO NOT USE IT AS AN EXAMPLE !!! */


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * osl_textual_idump function:
 * this function displays an osl_textual_t structure (*textual) into a
 * file (file, possibly stdout) in a way that trends to be understandable. It
 * includes an indentation level (level) in order to work with others
 * idump functions.
 * \param[in] file    The file where the information has to be printed.
 * \param[in] textual The textual structure to be printed.
 * \param[in] level   Number of spaces before printing, for each line.
 */
void osl_textual_idump(FILE * file, osl_textual_p textual, int level) {
    int j;
    char * tmp;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file, "|\t");

    if (textual != NULL) {
        fprintf(file, "+-- osl_textual_t: ");

        // Display the textual message (without any carriage return).
        OSL_strdup(tmp, textual->textual);
        for (j = 0; j < (int)strlen(tmp); j++)
            if (tmp[j] == '\n')
                tmp[j] = ' ';

        if (strlen(tmp) > 40) {
            for (j = 0; j < 20; j++)
                fprintf(file, "%c", tmp[j]);
            fprintf(file, "   ...   ");
            for (j = (int)strlen(tmp) - 20; j < (int)strlen(tmp); j++)
                fprintf(file, "%c", tmp[j]);
            fprintf(file, "\n");
        } else {
            fprintf(file,"%s\n", tmp);
        }
        free(tmp);
    } else {
        fprintf(file, "+-- NULL textual\n");
    }

    // The last line.
    for (j = 0; j <= level; j++)
        fprintf(file, "|\t");
    fprintf(file, "\n");
}


/**
 * osl_textual_dump function:
 * this function prints the content of an osl_textual_t structure
 * (*textual) into a file (file, possibly stdout).
 * \param[in] file    The file where the information has to be printed.
 * \param[in] textual The textual structure to be printed.
 */
void osl_textual_dump(FILE * file, osl_textual_p textual) {
    osl_textual_idump(file, textual, 0);
}



#if 0
/**
 * osl_textual_sprint function:
 * this function prints the content of an osl_textual_t structure
 * (*textual) into a string (returned) in the OpenScop textual format.
 * \param[in]  textual The textual structure to be printed.
 * \return A string containing the OpenScop dump of the textual structure.
 */
char * osl_textual_sprint(osl_textual_p textual) {
    char * string = NULL;

    if ((textual != NULL) && (textual->textual != NULL)) {
        if (strlen(textual->textual) > OSL_MAX_STRING)
            OSL_error("textual too long");

        string = strdup(textual->textual);
        if (string == NULL)
            OSL_error("memory overflow");
    }

    return string;
}
#else
/**
 * osl_textual_sprint function:
 * this function returns NULL. This is part of the special behavior of
 * the textual option (printing it along with other options would double
 * the options...).
 * \param[in]  textual The textual structure to be printed.
 * \return NULL.
 */
char * osl_textual_sprint(osl_textual_p textual) {
    (void) textual;
    return NULL;
}
#endif


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/**
 * osl_textual_sread function:
 * this function reads a textual structure from a string complying to the
 * OpenScop textual format and returns a pointer to this textual structure.
 * The string should contain only one textual format of a textual structure.
 * \param[in,out] extensions The input string where to find a textual struct.
 *                           Updated to the position after what has been read.
 * \return A pointer to the textual structure that has been read.
 */
osl_textual_p osl_textual_sread(char ** extensions) {
    osl_textual_p textual = NULL;

    if (*extensions != NULL) {
        textual = osl_textual_malloc();
        OSL_strdup(textual->textual, *extensions);

        // Update the input string pointer to the end of the string (since
        // everything has been read).
        *extensions = *extensions + strlen(*extensions);
    }

    return textual;
}


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/


/**
 * osl_textual_malloc function:
 * this function allocates the memory space for an osl_textual_t
 * structure and sets its fields with default values. Then it returns a
 * pointer to the allocated space.
 * \return A pointer to an empty textual structure with fields set to
 *         default values.
 */
osl_textual_p osl_textual_malloc(void) {
    osl_textual_p textual;

    OSL_malloc(textual, osl_textual_p, sizeof(osl_textual_t));
    textual->textual = NULL;

    return textual;
}


/**
 * osl_textual_free function:
 * this function frees the allocated memory for an osl_textual_t
 * structure.
 * \param[in,out] textual The pointer to the textual structure to be freed.
 */
void osl_textual_free(osl_textual_p textual) {
    if (textual != NULL) {
        if(textual->textual != NULL)
            free(textual->textual);
        free(textual);
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_textual_clone function:
 * this function builds and returns a "hard copy" (not a pointer copy) of an
 * osl_textual_t data structure.
 * \param[in] textual The pointer to the textual structure we want to clone.
 * \return A pointer to the clone of the textual structure.
 */
osl_textual_p osl_textual_clone(osl_textual_p textual) {
    osl_textual_p clone;

    if (textual == NULL)
        return NULL;

    clone = osl_textual_malloc();
    OSL_strdup(clone->textual, textual->textual);

    return clone;
}


#if 0
/**
 * osl_textual_equal function:
 * this function returns true if the two textual structures are the same
 * (content-wise), false otherwise.
 * \param f1  The first textual structure.
 * \param ff  The second textual structure.
 * \return 1 if f1 and f2 are the same (content-wise), 0 otherwise.
 */
int osl_textual_equal(osl_textual_p f1, osl_textual_p f2) {

    if (f1 == f2)
        return 1;

    if (((f1 == NULL) && (f2 != NULL)) || ((f1 != NULL) && (f2 == NULL)))
        return 0;

    if (strcmp(f1->textual, f2->textual))
        return 0;

    return 1;
}
#else
/**
 * osl_textual_equal function:
 * this function returns 1. This is part of the special behavior of
 * the textual option (the text string can be easily different while the
 * options are actually identical.
 * \param[in] f1  The first textual structure.
 * \param[in] f2  The second textual structure.
 * \return 1.
 */
int osl_textual_equal(osl_textual_p f1, osl_textual_p f2) {
    (void) f1;
    (void) f2;
    return 1;
}
#endif


/**
 * osl_textual_interface function:
 * this function creates an interface structure corresponding to the textual
 * extension and returns it).
 * \return An interface structure for the textual extension.
 */
osl_interface_p osl_textual_interface(void) {
    osl_interface_p interface = osl_interface_malloc();

    OSL_strdup(interface->URI, OSL_URI_TEXTUAL);
    interface->idump  = (osl_idump_f)osl_textual_idump;
    interface->sprint = (osl_sprint_f)osl_textual_sprint;
    interface->sread  = (osl_sread_f)osl_textual_sread;
    interface->malloc = (osl_malloc_f)osl_textual_malloc;
    interface->free   = (osl_free_f)osl_textual_free;
    interface->clone  = (osl_clone_f)osl_textual_clone;
    interface->equal  = (osl_equal_f)osl_textual_equal;

    return interface;
}

