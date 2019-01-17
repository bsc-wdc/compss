
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                           strings.c                             **
 **-----------------------------------------------------------------**
 **                   First version: 13/07/2011                     **
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
# include <ctype.h>
# include <string.h>

# include <osl/macros.h>
# include <osl/util.h>
# include <osl/interface.h>
# include <osl/strings.h>


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * osl_strings_idump function:
 * this function displays an array of strings into a file (file, possibly
 * stdout) in a way that trends to be understandable. It includes an
 * indentation level (level) in order to work with others
 * idump functions.
 * \param[in] file    The file where the information has to be printed.
 * \param[in] strings The array of strings that has to be printed.
 * \param[in] level   Number of spaces before printing, for each line.
 */
void osl_strings_idump(FILE * file, osl_strings_p strings, int level) {
    size_t i, nb_strings;
    int j;

    for (j = 0; j < level; j++)
        fprintf(file, "|\t");

    if (strings != NULL) {
        nb_strings = osl_strings_size(strings);
        fprintf(file, "+-- osl_strings_t:");
        for (i = 0; i < nb_strings; i++)
            fprintf(file, " %s", strings->string[i]);
        fprintf(file, "\n");
    } else
        fprintf(file, "+-- NULL strings\n");

    // A blank line.
    for (j = 0; j <= level; j++)
        fprintf(file, "|\t");
    fprintf(file, "\n");
}


/**
 * osl_strings_dump function:
 * this function prints the content of an osl_strings_t structure
 * (*strings) into a file (file, possibly stdout).
 * \param[in] file    The file where the information has to be printed.
 * \param[in] strings The strings structure which has to be printed.
 */
void osl_strings_dump(FILE * file, osl_strings_p strings) {
    osl_strings_idump(file, strings, 0);
}


/**
 * osl_strings_sprint function:
 * this function prints the content of an osl_strings_t structure
 * (*strings) into a string (returned) in the OpenScop textual format.
 * \param[in] strings The strings structure which has to be printed.
 * \return A string containing the OpenScop dump of the strings structure.
 */
char * osl_strings_sprint(osl_strings_p strings) {
    size_t i;
    size_t high_water_mark = OSL_MAX_STRING;
    char * string = NULL;
    char buffer[OSL_MAX_STRING];

    OSL_malloc(string, char *, high_water_mark * sizeof(char));
    string[0] = '\0';

    if (strings != NULL) {
        for (i = 0; i < osl_strings_size(strings); i++) {
            sprintf(buffer, "%s", strings->string[i]);
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
            if (i < osl_strings_size(strings) - 1)
                osl_util_safe_strcat(&string, " ", &high_water_mark);
        }
        sprintf(buffer, "\n");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
    } else {
        sprintf(buffer, "# NULL strings\n");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
    }

    return string;
}


/**
 * osl_strings_print function:
 * this function prints the content of an osl_strings_t structure
 * (*body) into a file (file, possibly stdout) in the OpenScop format.
 * \param[in] file    File where informations are printed.
 * \param[in] strings The strings whose information has to be printed.
 */
void osl_strings_print(FILE * file, osl_strings_p strings) {
    char * string;

    string = osl_strings_sprint(strings);
    if (string != NULL) {
        fprintf(file, "%s", string);
        free(string);
    }
}


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * osl_strings_sread function:
 * this function reads a strings structure from a string complying to the
 * OpenScop textual format and returns a pointer to this strings structure.
 * The input string should only contain the list of strings this function
 * has to read (comments at the end of the line are accepted). The input
 * parameter is updated to the position in the input string this function
 * reach right after reading the strings structure.
 * \param[in,out] input The input string where to find a strings structure.
 *                      Updated to the position after what has been read.
 * \return A pointer to the strings structure that has been read.
 */
osl_strings_p osl_strings_sread(char ** input) {
    char tmp[OSL_MAX_STRING];
    char * s;
    char ** string = NULL;
    size_t i, nb_strings;
    int count;
    osl_strings_p strings = NULL;

    // Skip blank/commented lines and spaces before the strings.
    osl_util_sskip_blank_and_comments(input);

    // Count the actual number of strings.
    nb_strings = 0;
    s = *input;
    while (1) {
        for (count = 0; *s && !isspace(*s) && *s != '#'; count++)
            s++;

        if (count != 0)
            nb_strings++;

        if ((!*s) || (*s == '#') || (*s == '\n'))
            break;
        else
            s++;
    }

    if (nb_strings > 0) {
        // Allocate the array of strings. Make it NULL-terminated.
        OSL_malloc(string, char **, sizeof(char *) * (nb_strings + 1));
        string[nb_strings] = NULL;

        // Read the desired number of strings.
        s = *input;
        for (i = 0; i < nb_strings; i++) {
            for (count = 0; *s && !isspace(*s) && *s != '#'; count++)
                tmp[count] = *(s++);
            tmp[count] = '\0';
            OSL_strdup(string[i], tmp);
            if (*s != '#')
                s++;
        }

        // Update the input pointer to the end of the strings structure.
        *input = s;

        // Build the strings structure
        strings = osl_strings_malloc();
        free(strings->string);
        strings->string = string;
    }

    return strings;
}


/**
 * osl_strings_read function.
 * this function reads a strings structure from a file (possibly stdin)
 * complying to the OpenScop textual format and returns a pointer to this
 * structure.
 * parameter nb_strings).
 * \param[in] file The file where to read the strings structure.
 * \return The strings structure that has been read.
 */
osl_strings_p osl_strings_read(FILE * file) {
    char buffer[OSL_MAX_STRING], * start;
    osl_strings_p strings;

    start = osl_util_skip_blank_and_comments(file, buffer);
    strings = osl_strings_sread(&start);

    return strings;
}


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/


/**
 * osl_strings_malloc function:
 * This function allocates the memory space for an osl_strings_t
 * structure and sets its fields with default values. Then it returns a
 * pointer to the allocated space.
 * \return A pointer to an empty strings structure with fields set to
 *         default values.
 */
osl_strings_p osl_strings_malloc(void) {
    osl_strings_p strings;

    OSL_malloc(strings, osl_strings_p, sizeof(osl_strings_t));
    OSL_malloc(strings->string, char**, sizeof(char*));
    strings->string[0] = NULL;

    return strings;
}


/**
 * osl_strings_free function:
 * this function frees the allocated memory for a strings data structure.
 * \param[in] strings The strings structure we want to free.
 */
void osl_strings_free(osl_strings_p strings) {
    int i;

    if (strings != NULL) {
        if (strings->string != NULL) {
            i = 0;
            while (strings->string[i] != NULL) {
                free(strings->string[i]);
                i++;
            }
            free(strings->string);
        }
        free(strings);
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_strings_clone function.
 * this function builds and return a "hard copy" (not a pointer copy) of an
 * strings structure provided as parameter.
 * \param[in] strings The strings structure to clone.
 * \return The clone of the strings structure.
 */
osl_strings_p osl_strings_clone(osl_strings_p strings) {
    size_t i, nb_strings;
    osl_strings_p clone = NULL;

    if (strings == NULL)
        return NULL;

    clone = osl_strings_malloc();
    if ((nb_strings = osl_strings_size(strings)) == 0)
        return clone;

    free(clone->string);
    OSL_malloc(clone->string, char **, (nb_strings + 1) * sizeof(char *));
    clone->string[nb_strings] = NULL;
    for (i = 0; i < nb_strings; i++)
        OSL_strdup(clone->string[i], strings->string[i]);

    return clone;
}

/**
 * osl_strings_find function.
 * this function finds the string in the strings.
 * \param[in,out] strings The strings structure.
 * \param[in]     string  The string to find in strings.
 * \return the index where is the string, osl_strings_size if not found
 */
size_t osl_strings_find(osl_strings_p strings, char const * const string) {
    size_t i;
    for (i = 0; i < osl_strings_size(strings); ++i) {
        if (strcmp(strings->string[i], string) == 0) {
            return i;
        }
    }
    return i;
}


/**
 * osl_strings_add function.
 * this function adds a copy of the string in the strings.
 * \param[in,out] strings The strings structure.
 * \param[in]     string  The string to add in strings.
 */
void osl_strings_add(osl_strings_p strings, char const * const string) {
    size_t original_size = osl_strings_size(strings);
    OSL_realloc(strings->string, char**, sizeof(char*) * (original_size + 1 + 1));
    strings->string[original_size + 1] = NULL;
    strings->string[original_size] = malloc(sizeof(char) * (strlen(string) + 1));
    strcpy(strings->string[original_size], string);
}


/**
 * osl_strings_equal function:
 * this function returns true if the two strings structures are the same
 * (content-wise), false otherwise.
 * \param[in] s1 The first strings structure.
 * \param[in] s2 The second strings structure.
 * \return 1 if s1 and s2 are the same (content-wise), 0 otherwise.
 */
int osl_strings_equal(osl_strings_p s1, osl_strings_p s2) {
    size_t i, nb_s1;

    if (s1 == s2)
        return 1;

    if (((s1 == NULL) && (s2 != NULL)) ||
            ((s1 != NULL) && (s2 == NULL)) ||
            ((nb_s1 = osl_strings_size(s1)) != osl_strings_size(s2)))
        return 0;

    for (i = 0; i < nb_s1; i++)
        if (strcmp(s1->string[i], s2->string[i]) != 0)
            return 0;

    return 1;
}


/**
 * osl_strings_size function:
 * this function returns the number of elements in the NULL-terminated
 * strings array of the strings structure.
 * \param[in] strings The strings structure we need to know the size.
 * \return The number of strings in the strings structure.
 */
size_t osl_strings_size(osl_const_strings_const_p strings) {
    size_t size = 0;

    if ((strings != NULL) && (strings->string != NULL)) {
        while (strings->string[size] != NULL) {
            size++;
        }
    }

    return size;
}


/**
 * osl_strings_encapsulate function:
 * this function builds a new strings structure to encapsulate the string
 * provided as a parameter (the reference to this string is used directly).
 * \param[in] string The string to encapsulate in a strings structure.
 * \return A new strings structure containing only the provided string.
 */
osl_strings_p osl_strings_encapsulate(char * string) {
    osl_strings_p capsule = osl_strings_malloc();
    free(capsule->string);
    OSL_malloc(capsule->string, char **, 2 * sizeof(char *));
    capsule->string[0] = string;
    capsule->string[1] = NULL;

    return capsule;
}


/**
 * osl_strings_interface function:
 * this function creates an interface structure corresponding to the strings
 * structure and returns it).
 * \return An interface structure for the strings structure.
 */
osl_interface_p osl_strings_interface(void) {
    osl_interface_p interface = osl_interface_malloc();

    OSL_strdup(interface->URI, OSL_URI_STRINGS);
    interface->idump  = (osl_idump_f)osl_strings_idump;
    interface->sprint = (osl_sprint_f)osl_strings_sprint;
    interface->sread  = (osl_sread_f)osl_strings_sread;
    interface->malloc = (osl_malloc_f)osl_strings_malloc;
    interface->free   = (osl_free_f)osl_strings_free;
    interface->clone  = (osl_clone_f)osl_strings_clone;
    interface->equal  = (osl_equal_f)osl_strings_equal;

    return interface;
}


/**
 * osl_strings_generate function:
 * this function generates a new strings structure containing
 * 'nb_strings' strings of the form "prefixXX" where XX goes from 1 to
 * nb_strings.
 * \param[in] prefix     The prefix of the generated strings.
 * \param[in] nb_strings The number of strings to generate.
 * \return A new strings structure containing generated strings.
 */
osl_strings_p osl_strings_generate(char * prefix, int nb_strings) {
    char ** strings = NULL;
    char buff[strlen(prefix) + 16]; // TODO: better (log10(INT_MAX) ?) :-D.
    int i;
    osl_strings_p generated;

    if (nb_strings) {
        OSL_malloc(strings, char **, sizeof(char *) * (size_t)(nb_strings + 1));
        strings[nb_strings] = NULL;
        for (i = 0; i < nb_strings; i++) {
            sprintf(buff, "%s%d", prefix, i + 1);
            OSL_strdup(strings[i], buff);
            if (strings[i] == NULL)
                OSL_error("memory overflow");
        }
    }

    generated = osl_strings_malloc();
    free(generated->string);
    generated->string = strings;
    return generated;
}

/**
 * \brief Concatenate two osl_strings into one. The parameter are cloned and not modified.
 *
 * \param dest[out] A pointer to the destination osl_strings.
 * \param str1[in] The first osl_strings.
 * \param str2[in] The second osl_strings.
 */
void osl_strings_add_strings(
    osl_strings_p * dest,
    osl_strings_p   str1,
    osl_strings_p   str2) {
    struct osl_strings * res = NULL;
    unsigned int i = 0;

    res = osl_strings_clone(str1);
    while (str2->string[i] != NULL) {
        osl_strings_add(res, str2->string[i]);
        i++;
    }

    *dest = res;
}

