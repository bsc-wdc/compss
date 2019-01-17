
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                      extensions/arrays.c                        **
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
#include <ctype.h>

#include <osl/macros.h>
#include <osl/util.h>
#include <osl/strings.h>
#include <osl/interface.h>
#include <osl/extensions/arrays.h>


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * osl_arrays_idump function:
 * this function displays an osl_arrays_t structure (*arrays) into a file
 * (file, possibly stdout) in a way that trends to be understandable. It
 * includes an indentation level (level) in order to work with others
 * idump functions.
 * \param[in] file   The file where the information has to be printed.
 * \param[in] arrays The arrays structure to print.
 * \param[in] level  Number of spaces before printing, for each line.
 */
void osl_arrays_idump(FILE * file, osl_arrays_p arrays, int level) {
    int i, j;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file, "|\t");

    if (arrays != NULL)
        fprintf(file, "+-- osl_arrays_t\n");
    else
        fprintf(file, "+-- NULL arrays\n");

    if (arrays != NULL) {
        // Go to the right level.
        for(j = 0; j <= level; j++)
            fprintf(file, "|\t");

        // Display the number of names.
        fprintf(file, "nb_names: %d\n", arrays->nb_names);

        // Display the id/name.
        for(i = 0; i < arrays->nb_names; i++) {
            // Go to the right level.
            for(j = 0; j <= level; j++)
                fprintf(file, "|\t");

            fprintf(file, "id: %2d, name: %s\n", arrays->id[i], arrays->names[i]);
        }
    }

    // The last line.
    for (j = 0; j <= level; j++)
        fprintf(file, "|\t");
    fprintf(file, "\n");
}


/**
 * osl_arrays_dump function:
 * this function prints the content of an osl_arrays_t structure
 * (*arrays) into a file (file, possibly stdout).
 * \param[in] file   The file where the information has to be printed.
 * \param[in] arrays The arrays structure to print.
 */
void osl_arrays_dump(FILE * file, osl_arrays_p arrays) {
    osl_arrays_idump(file, arrays, 0);
}


/**
 * osl_arrays_sprint function:
 * this function prints the content of an osl_arrays_t structure
 * (*arrays) into a string (returned) in the OpenScop textual format.
 * \param[in] arrays The arrays structure to print.
 * \return A string containing the OpenScop dump of the arrays structure.
 */
char * osl_arrays_sprint(osl_arrays_p arrays) {
    int i;
    size_t high_water_mark = OSL_MAX_STRING;
    char * string = NULL;
    char buffer[OSL_MAX_STRING];

    if (arrays != NULL) {
        OSL_malloc(string, char *, high_water_mark * sizeof(char));
        string[0] = '\0';

        sprintf(buffer, "# Number of arrays\n");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        sprintf(buffer, "%d\n", arrays->nb_names);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        if (arrays->nb_names) {
            sprintf(buffer, "# Mapping array-identifiers/array-names\n");
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
        }
        for (i = 0; i < arrays->nb_names; i++) {
            sprintf(buffer, "%d %s\n", arrays->id[i], arrays->names[i]);
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
        }

        OSL_realloc(string, char *, (strlen(string) + 1) * sizeof(char));
    }

    return string;
}


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/**
 * osl_arrays_sread function:
 * this function reads an arrays structure from a string complying to the
 * OpenScop textual format and returns a pointer to this arrays structure.
 * The string should contain only one textual format of an arrays structure.
 * The input parameter is updated to the position in the input string this
 * function reach right after reading the comment structure.
 * \param[in,out] input The input string where to find an arrays structure.
 *                      Updated to the position after what has been read.
 * \return A pointer to the arrays structure that has been read.
 */
osl_arrays_p osl_arrays_sread(char ** input) {
    int i, k;
    int nb_names;
    osl_arrays_p arrays;

    if (input == NULL) {
        OSL_debug("no arrays optional tag");
        return NULL;
    }

    // Find the number of names provided.
    nb_names = osl_util_read_int(NULL, input);

    // Allocate the array of id and names.
    arrays = osl_arrays_malloc();
    OSL_malloc(arrays->id, int *, (size_t)nb_names * sizeof(int));
    OSL_malloc(arrays->names, char **, (size_t)nb_names * sizeof(char *));
    arrays->nb_names = nb_names;
    for (i = 0; i < nb_names; i++)
        arrays->names[i] = NULL;

    // Get each array id/name.
    for (k = 0; k < nb_names; k++) {
        // Get the array name id.
        arrays->id[k] = osl_util_read_int(NULL, input);

        // Get the array name string.
        arrays->names[k] = osl_util_read_string(NULL, input);
    }

    return arrays;
}


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/


/**
 * osl_arrays_malloc function:
 * this function allocates the memory space for an osl_arrays_t
 * structure and sets its fields with default values. Then it returns a
 * pointer to the allocated space.
 * \return A pointer to an empty arrays structure with fields set to
 *         default values.
 */
osl_arrays_p osl_arrays_malloc(void) {
    osl_arrays_p arrays;

    OSL_malloc(arrays, osl_arrays_p, sizeof(osl_arrays_t));
    arrays->nb_names = 0;
    arrays->id       = NULL;
    arrays->names    = NULL;

    return arrays;
}


/**
 * osl_arrays_free function:
 * this function frees the allocated memory for an arrays structure.
 * \param[in,out] arrays The pointer to the arrays structure we want to free.
 */
void osl_arrays_free(osl_arrays_p arrays) {
    int i;

    if (arrays != NULL) {
        free(arrays->id);
        for (i = 0; i < arrays->nb_names; i++)
            free(arrays->names[i]);
        free(arrays->names);
        free(arrays);
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_arrays_clone function:
 * this function builds and returns a "hard copy" (not a pointer copy) of an
 * osl_arrays_t data structure.
 * \param[in] arrays The pointer to the arrays structure to clone.
 * \return A pointer to the clone of the arrays structure.
 */
osl_arrays_p osl_arrays_clone(osl_arrays_p arrays) {
    int i;
    osl_arrays_p clone;

    if (arrays == NULL)
        return NULL;

    clone = osl_arrays_malloc();
    clone->nb_names = arrays->nb_names;
    OSL_malloc(clone->id, int *, (size_t)arrays->nb_names * sizeof(int));
    OSL_malloc(clone->names, char **, (size_t)arrays->nb_names * sizeof(char*));

    for (i = 0; i < arrays->nb_names; i++) {
        clone->id[i] = arrays->id[i];
        OSL_strdup(clone->names[i], arrays->names[i]);
    }

    return clone;
}


/**
 * osl_arrays_equal function:
 * this function returns true if the two arrays structures are the same
 * (content-wise), false otherwise. This functions considers two arrays
 * structures as equal if the order of the array names differ, however the
 * identifiers and names must be the same.
 * \param[in] a1 The first arrays structure.
 * \param[in] a2 The second arrays structure.
 * \return 1 if a1 and a2 are the same (content-wise), 0 otherwise.
 */
int osl_arrays_equal(osl_arrays_p a1, osl_arrays_p a2) {
    int i, j, found;

    if (a1 == a2)
        return 1;

    if (((a1 == NULL) && (a2 != NULL)) || ((a1 != NULL) && (a2 == NULL))) {
        OSL_info("arrays are not the same");
        return 0;
    }

    // Check whether the number of names is the same.
    if (a1->nb_names != a2->nb_names) {
        OSL_info("arrays are not the same");
        return 0;
    }

    // We accept a different order of the names, as long as the identifiers
    // are the same.
    for (i = 0; i < a1->nb_names; i++) {
        found = 0;
        for (j = 0; j < a2->nb_names; j++) {
            if ((a1->id[i] == a2->id[j]) && (!strcmp(a1->names[i], a2->names[j]))) {
                found = 1;
                break;
            }
        }
        if (found != 1) {
            OSL_info("arrays are not the same");
            return 0;
        }
    }

    return 1;
}


/**
 * osl_arrays_to_strings function:
 * this function creates a strings structure containing the textual names
 * contained in a names structure. Each name is placed according to its
 * id in the strings array. The "empty" strings cells are filled with
 * dummy names.
 * \param[in] arrays The arrays structure to convert to a strings.
 * \return A strings structure containing all the array names.
 */
osl_strings_p osl_arrays_to_strings(osl_arrays_p arrays) {
    int i, max_id = 0;
    osl_strings_p strings = NULL;

    if (arrays == NULL)
        return NULL;

    // Find the maximum array id.
    if (arrays->nb_names >= 1) {
        max_id = arrays->id[0];
        for (i = 1; i < arrays->nb_names; i++)
            if (arrays->id[i] > max_id)
                max_id = arrays->id[i];
    }

    // Build a strings structure for this number of ids.
    strings = osl_strings_generate("Dummy", max_id);
    for (i = 0; i < arrays->nb_names; i++) {
        free(strings->string[arrays->id[i] - 1]);
        OSL_strdup(strings->string[arrays->id[i] - 1], arrays->names[i]);
    }

    return strings;
}

/**
 * osl_arrays_add function:
 * this function adds a new variable at the end of osl_array
 *
 * \param[in] arrays The arrays structure to modify.
 * \param[in] id     The new variable's id.
 * \param[in] name   The new variable's name.
 * \return Updated number of elements, -1 means error
 */
int osl_arrays_add(osl_arrays_p arrays, int id, char* name) {

    if (arrays == NULL || name == NULL)
        return -1;

    OSL_realloc(arrays->id, int *, (size_t)(arrays->nb_names+1) * sizeof(int));
    OSL_realloc(arrays->names, char **, (size_t)(arrays->nb_names+1) * sizeof(char *));
    arrays->id[arrays->nb_names] = id;
    OSL_strdup(arrays->names[arrays->nb_names], name);
    arrays->nb_names++;

    return arrays->nb_names;
}


/**
 * osl_arrays_get_index_from_id function:
 * this function the index of a variable given its identifier
 *
 * \param[in] arrays The arrays structure to modify.
 * \param[in] id     The variable's id.
 * \return index of the variable, array->nb_names means error
 */
size_t osl_arrays_get_index_from_id(osl_arrays_p arrays, int id) {
    size_t i = 0;

    if (arrays == NULL)
        return 0;

    for (i=0; i< (size_t)arrays->nb_names; i++) {
        if(arrays->id[i]==id)
            break;
    }

    return i<(size_t)arrays->nb_names? i: (size_t)arrays->nb_names;
}

/**
 * osl_arrays_get_index_from_name function:
 * this function the index of a variable given its name
 *
 * \param[in] arrays The arrays structure to modify.
 * \param[in] name     The variable's name.
 * \return index of the variable, array->nb_names means error
 */
size_t osl_arrays_get_index_from_name(osl_arrays_p arrays, char* name) {
    size_t i = 0;

    if (arrays == NULL || name == NULL)
        return 0;

    for (i=0; i<(size_t)arrays->nb_names; i++) {
        if(!strcmp(arrays->names[i], name))
            break;
    }

    return i<(size_t)arrays->nb_names? i: (size_t)arrays->nb_names;;
}

/**
 * osl_arrays_interface function:
 * this function creates an interface structure corresponding to the arrays
 * extension and returns it).
 * \return An interface structure for the arrays extension.
 */
osl_interface_p osl_arrays_interface(void) {
    osl_interface_p interface = osl_interface_malloc();

    OSL_strdup(interface->URI, OSL_URI_ARRAYS);
    interface->idump  = (osl_idump_f)osl_arrays_idump;
    interface->sprint = (osl_sprint_f)osl_arrays_sprint;
    interface->sread  = (osl_sread_f)osl_arrays_sread;
    interface->malloc = (osl_malloc_f)osl_arrays_malloc;
    interface->free   = (osl_free_f)osl_arrays_free;
    interface->clone  = (osl_clone_f)osl_arrays_clone;
    interface->equal  = (osl_equal_f)osl_arrays_equal;

    return interface;
}


