
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                            body.c                               **
 **-----------------------------------------------------------------**
 **                   First version: 25/06/2011                     **
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


# include <stdlib.h>
# include <stdio.h>
# include <string.h>
# include <ctype.h>
# include <osl/macros.h>
# include <osl/util.h>
# include <osl/strings.h>
# include <osl/interface.h>
# include <osl/body.h>


/*+***************************************************************************
 *                         Structure display functions                       *
 *****************************************************************************/


/**
 * osl_body_idump function:
 * this function displays an osl_body_t structure (*body) into a
 * file (file, possibly stdout) in a way that trends to be understandable.
 * It includes an indentation level (level) in order to work with others
 * dumping functions.
 * \param[in] file  File where informations are printed.
 * \param[in] body  The body whose information has to be printed.
 * \param[in] level Number of spaces before printing, for each line.
 */
void osl_body_idump(FILE * file, osl_body_p body, int level) {
    int j;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file, "|\t");

    if (body != NULL) {
        fprintf(file, "+-- osl_body_t\n");

        // A blank line.
        for (j = 0; j <= level+1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        // Print the iterators
        osl_strings_idump(file, body->iterators, level + 1);

        // Print the original body expression.
        osl_strings_idump(file, body->expression, level + 1);
    } else {
        fprintf(file, "+-- NULL body\n");
    }

    // The last line.
    for (j = 0; j <= level; j++)
        fprintf(file, "|\t");
    fprintf(file, "\n");
}


/**
 * osl_body_dump function:
 * this function prints the content of an osl_body_t structure
 * (*body) into  a file (file, possibly stdout).
 * \param[in] file File where informations are printed.
 * \param[in] body The body whose information has to be printed.
 */
void osl_body_dump(FILE * file, osl_body_p body) {
    osl_body_idump(file, body, 0);
}


/**
 * osl_body_print function:
 * this function prints the content of an osl_body_t structure
 * (*body) into a file (file, possibly stdout) in the OpenScop format.
 * \param[in] file  File where informations are printed.
 * \param[in] body  The body whose information has to be printed.
 */
void osl_body_print(FILE * file, osl_body_p body) {
    size_t nb_iterators;

    if (body != NULL) {
        nb_iterators = osl_strings_size(body->iterators);
        fprintf(file, "# Number of original iterators\n");
        fprintf(file, "%zu\n", nb_iterators);

        if (nb_iterators > 0) {
            fprintf(file, "\n# List of original iterators\n");
            osl_strings_print(file, body->iterators);
        }

        fprintf(file, "\n# Statement body expression\n");
        osl_strings_print(file, body->expression);
    } else {
        fprintf(file, "# NULL statement body\n");
    }
}


/**
 * osl_body_print_scoplib function:
 * this function prints the content of an osl_body_t structure
 * (*body) into a file (file, possibly stdout) in the SCoPLib format.
 * \param[in] file  File where informations are printed.
 * \param[in] body  The body whose information has to be printed.
 */
void osl_body_print_scoplib(FILE * file, osl_body_p body) {
    size_t nb_iterators;

    if (body != NULL) {
        nb_iterators = osl_strings_size(body->iterators);

        if (nb_iterators > 0) {
            fprintf(file, "# List of original iterators\n");
            osl_strings_print(file, body->iterators);
        } else {
            fprintf(file, "fakeiter\n");
        }

        fprintf(file, "# Statement body expression\n");
        osl_strings_print(file, body->expression);
    } else {
        fprintf(file, "# NULL statement body\n");
    }
}


/**
 * osl_body_sprint function:
 * this function prints the content of an osl_body_t structure
 * (*body) into a string (returned) in the OpenScop textual format.
 * \param[in] body The body structure which has to be printed.
 * \return A string containing the OpenScop dump of the body structure.
 */
char * osl_body_sprint(osl_body_p body) {
    size_t nb_iterators;
    size_t high_water_mark = OSL_MAX_STRING;
    char * string = NULL;
    char buffer[OSL_MAX_STRING];
    char * iterators, * expression;

    OSL_malloc(string, char *, high_water_mark * sizeof(char));
    string[0] = '\0';

    if (body != NULL) {
        nb_iterators = osl_strings_size(body->iterators);
        sprintf(buffer, "# Number of original iterators\n%zu\n", nb_iterators);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        if (nb_iterators > 0) {
            sprintf(buffer, "# List of original iterators\n");
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
            iterators = osl_strings_sprint(body->iterators);
            osl_util_safe_strcat(&string, iterators, &high_water_mark);
            free(iterators);
        }

        sprintf(buffer, "# Statement body expression\n");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
        expression = osl_strings_sprint(body->expression);
        osl_util_safe_strcat(&string, expression, &high_water_mark);
        free(expression);
    } else {
        sprintf(buffer, "# NULL body\n");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
    }

    return string;
}



/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/**
 * osl_body_read function:
 * this function reads a body structure from a string complying to the
 * OpenScop textual format and returns a pointer to this body structure.
 * The input string should only contain the body this function
 * has to read (comments at the end of the line are accepted). The input
 * parameter is updated to the position in the input string this function
 * reach right after reading the strings structure.
 * \param[in,out] input The input string where to find a body structure.
 *                      Updated to the position after what has been read.
 * \return A pointer to the body structure that has been read.
 */
osl_body_p osl_body_sread(char ** input) {
    osl_body_p body = NULL;
    char * expression;
    int nb_iterators;

    if (input) {
        body = osl_body_malloc();

        // Read the number of iterators.
        nb_iterators = osl_util_read_int(NULL, input);

        // Read the iterator strings if any.
        if (nb_iterators > 0) {
            body->iterators = osl_strings_sread(input);
        } else {
            body->iterators = osl_strings_malloc();
        }

        // Read the body:
        expression = osl_util_read_line(NULL, input);

        // Insert the body.
        body->expression = osl_strings_encapsulate(expression);
    }

    return body;
}


/*+***************************************************************************
 *                   Memory allocation/deallocation functions                *
 *****************************************************************************/


/**
 * osl_body_malloc function:
 * this function allocates the memory space for an osl_body_t
 * structure and sets its fields with default values. Then it returns a pointer
 * to the allocated space.
 * \return A pointer to an empty body with fields set to default values.
 */
osl_body_p osl_body_malloc(void) {
    osl_body_p body;

    OSL_malloc(body, osl_body_p, sizeof(osl_body_t));
    body->iterators    = NULL;
    body->expression   = NULL;

    return body;
}


/**
 * osl_body_free function:
 * this function frees the allocated memory for an osl_body_t
 * structure.
 * \param[in,out] body The pointer to the body we want to free.
 */
void osl_body_free(osl_body_p body) {

    if (body != NULL) {
        osl_strings_free(body->iterators);
        osl_strings_free(body->expression);
        free(body);
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_body_clone function:
 * this functions builds and returns a "hard copy" (not a pointer copy) of an
 * osl_body_t data structure provided as parameter. However, let us
 * recall here that non-string elements are untouched by the OpenScop Library.
 * \param[in] body The pointer to the body we want to copy.
 * \return A pointer to the full copy of the body provided as parameter.
 */
osl_body_p osl_body_clone(osl_body_p body) {
    osl_body_p copy = NULL;

    if (body != NULL) {
        copy = osl_body_malloc();
        copy->iterators  = osl_strings_clone(body->iterators);
        copy->expression = osl_strings_clone(body->expression);
    }

    return copy;
}


/**
 * osl_body_equal function:
 * this function returns true if the two bodies are the same, false
 * otherwise (the usr field is not tested). However, let us
 * recall here that non-string elements are untouched by the OpenScop Library.
 * \param[in] b1 The first body.
 * \param[in] b2 The second body.
 * \return 1 if b1 and b2 are the same (content-wise), 0 otherwise.
 */
int osl_body_equal(osl_body_p b1, osl_body_p b2) {

    if (b1 == b2)
        return 1;

    if (((b1 != NULL) && (b2 == NULL)) ||
            ((b1 == NULL) && (b2 != NULL))) {
        OSL_info("bodies are not the same");
        return 0;
    }

    if (!osl_strings_equal(b1->iterators, b2->iterators)) {
        OSL_info("body iterators are not the same");
        return 0;
    }

    if (!osl_strings_equal(b1->expression, b2->expression)) {
        OSL_info("body expressions are not the same");
        return 0;
    }

    return 1;
}


/**
 * osl_body_interface function:
 * this function creates an interface structure corresponding to the body
 * structure and returns it).
 * \return An interface structure for the body structure.
 */
osl_interface_p osl_body_interface(void) {
    osl_interface_p interface = osl_interface_malloc();

    OSL_strdup(interface->URI, OSL_URI_BODY);
    interface->idump  = (osl_idump_f)osl_body_idump;
    interface->sprint = (osl_sprint_f)osl_body_sprint;
    interface->sread  = (osl_sread_f)osl_body_sread;
    interface->malloc = (osl_malloc_f)osl_body_malloc;
    interface->free   = (osl_free_f)osl_body_free;
    interface->clone  = (osl_clone_f)osl_body_clone;
    interface->equal  = (osl_equal_f)osl_body_equal;

    return interface;
}

