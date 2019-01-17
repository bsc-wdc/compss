
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                           generic.c                             **
 **-----------------------------------------------------------------**
 **                   First version: 26/11/2010                     **
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
#include <osl/generic.h>
#include <osl/extensions/arrays.h>


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * osl_generic_idump function:
 * this function displays an osl_generic_t structure (*generic) into
 * a file (file, possibly stdout) in a way that trends to be understandable.
 * It includes an indentation level (level) in order to work with others
 * idump functions.
 * \param[in] file    File where informations are printed.
 * \param[in] generic The generic whose information has to be printed.
 * \param[in] level   Number of spaces before printing, for each line.
 */
void osl_generic_idump(FILE * file, osl_generic_p generic, int level) {
    int j, first = 1;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file,"|\t");

    if (generic != NULL)
        fprintf(file, "+-- osl_generic_t\n");
    else
        fprintf(file, "+-- NULL generic\n");

    while (generic != NULL) {
        if (!first) {
            // Go to the right level.
            for (j = 0; j < level; j++)
                fprintf(file, "|\t");
            fprintf(file, "|   osl_generic_t\n");
        } else {
            first = 0;
        }

        // A blank line
        for(j = 0; j <= level + 1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        osl_interface_idump(file, generic->interface, level + 1);

        if (generic->interface != NULL)
            generic->interface->idump(file, generic->data, level + 1);

        generic = generic->next;

        // Next line.
        if (generic != NULL) {
            for (j = 0; j <= level; j++)
                fprintf(file, "|\t");
            fprintf(file, "V\n");
        }
    }

    // The last line.
    for (j = 0; j <= level; j++)
        fprintf(file, "|\t");
    fprintf(file, "\n");
}


/**
 * osl_generic_dump function:
 * this function prints the content of an osl_generic_t structure
 * (*generic) into a file (file, possibly stdout).
 * \param[in] file    File where the information has to be printed.
 * \param[in] generic The generic structure to print.
 */
void osl_generic_dump(FILE * file, osl_generic_p generic) {
    osl_generic_idump(file, generic, 0);
}


/**
 * osl_generic_sprint function:
 * this function prints the content of an osl_generic_t structure
 * (*strings) into a string (returned) in the OpenScop textual format.
 * \param[in] generic  The generic structure which has to be printed.
 * \return A string containing the OpenScop dump of the generic structure.
 */
char * osl_generic_sprint(osl_generic_p generic) {
    size_t high_water_mark = OSL_MAX_STRING;
    char * string = NULL, * content;
    char buffer[OSL_MAX_STRING];

    OSL_malloc(string, char *, high_water_mark * sizeof(char));
    string[0] = '\0';

    while (generic != NULL) {
        if (generic->interface != NULL) {
            content = generic->interface->sprint(generic->data);
            if (content != NULL) {
                sprintf(buffer, "<%s>\n", generic->interface->URI);
                osl_util_safe_strcat(&string, buffer, &high_water_mark);
                osl_util_safe_strcat(&string, content, &high_water_mark);
                free(content);
                sprintf(buffer, "</%s>\n", generic->interface->URI);
                osl_util_safe_strcat(&string, buffer, &high_water_mark);
            }
        }
        generic = generic->next;
        if (generic != NULL) {
            sprintf(buffer, "\n");
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
        }
    }

    return string;
}


/**
 * osl_generic_print function:
 * this function prints the content of an osl_generic_t structure
 * (*generic) into a string (returned) in the OpenScop format.
 * \param[in] file    File where the information has to be printed.
 * \param[in] generic The generic structure to print.
 */
void osl_generic_print(FILE * file, osl_generic_p generic) {
    char * string;

    string = osl_generic_sprint(generic);
    if (string != NULL) {
        fprintf(file, "%s", string);
        free(string);
    }
}


/**
 * osl_generic_print_options_scoplib function:
 * this function prints the options sections (only arrays in the
 * SCoPLib format)
 * \param[in] file    File where the information has to be printed.
 * \param[in] generic The generic structure to print.
 */
void osl_generic_print_options_scoplib(FILE * file, osl_generic_p generic) {
    char * string;

    osl_generic_p arrays = osl_generic_lookup(generic, OSL_URI_ARRAYS);

    string = osl_arrays_sprint((osl_arrays_p) arrays);
    if (string != NULL) {
        fprintf(file, "<arrays>\n%s</arrays>\n", string);
        free(string);
    }
}


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/**
 * osl_generic_sread function:
 * this function reads a list of generic structure from a string complying to
 * the OpenScop textual format and returns a pointer to this generic structure.
 * The input parameter is updated to the position in the input string this
 * function reach right after reading the generic structure.
 * \param[in,out] input    The input string where to find a list of generic.
 *                         Updated to the position after what has been read.
 * \param[in]     registry The list of known interfaces (others are ignored).
 * \return A pointer to the generic information list that has been read.
 */
osl_generic_p osl_generic_sread(char ** input, osl_interface_p registry) {
    osl_generic_p generic = NULL, new;

    while (**input != '\0') {
        new = osl_generic_sread_one(input, registry);
        osl_generic_add(&generic, new);
    }

    return generic;
}


/**
 * osl_generic_sread_one function:
 * this function reads one generic structure from a string complying to the
 * OpenScop textual format and returns a pointer to this generic structure.
 * The input parameter is updated to the position in the input string this
 * function reach right after reading the generic structure.
 * \param[in,out] input    The input string where to find a generic.
 *                         Updated to the position after what has been read.
 * \param[in]     registry The list of known interfaces (others are ignored).
 * \return A pointer to the generic structure that has been read.
 */
osl_generic_p osl_generic_sread_one(char ** input, osl_interface_p registry) {
    char * tag;
    char * content, * temp;
    osl_generic_p generic = NULL;
    osl_interface_p interface;

    tag = osl_util_read_tag(NULL, input);
    if ((tag == NULL) || (strlen(tag) < 1) || (tag[0] == '/')) {
        OSL_debug("empty tag name or closing tag instead of an opening one");
        return NULL;
    }

    content = osl_util_read_uptoendtag(NULL, input, tag);
    interface = osl_interface_lookup(registry, tag);

    temp = content;
    if (interface == NULL) {
        OSL_warning("unsupported generic");
        fprintf(stderr, "[osl] Warning: unknown URI \"%s\".\n", tag);
    } else {
        generic = osl_generic_malloc();
        generic->interface = osl_interface_nclone(interface, 1);
        generic->data = interface->sread(&temp);
    }

    free(content);
    free(tag);
    return generic;
}


/**
 * osl_generic_read_one function:
 * this function reads one generic from a file (possibly stdin)
 * complying to the OpenScop textual format and a list of known interfaces.
 * It returns a pointer to the corresponding generic structure. If no
 * tag is found, an error is reported, in the case of an empty or closing tag
 * name the function returns the NULL pointer.
 * \param[in] file     The input file where to read a list of data.
 * \param[in] registry The list of known interfaces (others are ignored).
 * \return A pointer to the generic that has been read.
 */
osl_generic_p osl_generic_read_one(FILE * file, osl_interface_p registry) {
    char * tag;
    char * content, * temp;
    osl_generic_p generic = NULL;
    osl_interface_p interface;

    tag = osl_util_read_tag(file, NULL);
    if ((tag == NULL) || (strlen(tag) < 1) || (tag[0] == '/')) {
        OSL_debug("empty tag name or closing tag instead of an opening one");
        return NULL;
    }

    content = osl_util_read_uptoendtag(file, NULL, tag);
    interface = osl_interface_lookup(registry, tag);

    temp = content;
    if (interface == NULL) {
        OSL_warning("unsupported generic");
        fprintf(stderr, "[osl] Warning: unknown URI \"%s\".\n", tag);
    } else {
        generic = osl_generic_malloc();
        generic->interface = osl_interface_nclone(interface, 1);
        generic->data = interface->sread(&temp);
    }

    free(content);
    free(tag);
    return generic;
}


/**
 * osl_generic_read function:
 * this function reads a list of generics from a file (possibly stdin)
 * complying to the OpenScop textual format and a list of known interfaces.
 * It returns a pointer to the list of corresponding generic structures.
 * \param[in] file     The input file where to read a list of data.
 * \param[in] registry The list of known interfaces (others are ignored).
 * \return A pointer to the generic information list that has been read.
 */
osl_generic_p osl_generic_read(FILE * file, osl_interface_p registry) {
    char * generic_string, * temp;
    osl_generic_p generic_list;

    generic_string = osl_util_read_uptoendtag(file, NULL, OSL_URI_SCOP);
    temp = generic_string;
    generic_list = osl_generic_sread(&temp, registry);
    free(generic_string);
    return generic_list;
}


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/


/**
 * osl_generic_add function:
 * this function adds a generic node (it may be a list as well) to a list
 * of generics provided as parameter (list). The new node is inserted at
 * the end of the list.
 * \param[in] list    The list of generics to add a node (NULL if empty).
 * \param[in] generic The generic list to add to the initial list.
 */
void osl_generic_add(osl_generic_p * list, osl_generic_p generic) {
    osl_generic_p tmp = *list, check;

    if (generic != NULL) {
        // First, check that the generic list is OK.
        check = generic;
        while (check != NULL) {
            if ((check->interface == NULL) || (check->interface->URI == NULL))
                OSL_error("no interface or URI in a generic to add to a list");

            // TODO: move this to the integrity check.
            if (osl_generic_lookup(*list, check->interface->URI) != NULL)
                OSL_error("only one generic with a given URI is allowed");
            check = check->next;
        }

        if (*list != NULL) {
            while (tmp->next != NULL)
                tmp = tmp->next;
            tmp->next = generic;
        } else {
            *list = generic;
        }
    }
}

/**
 * osl_generic_remove_node function:
 * this functions removes a given generic from a generic list
 * \param[in] list    Address of a generic list
 * \param[in] generic Pointer to the generic to be removed
 *                    Assumes a single node is to be removed.
 */
void osl_generic_remove_node(osl_generic_p * list, osl_generic_p generic) {

    osl_generic_p tmp = NULL;

    if (generic != NULL) {

        if (*list != NULL) {
            //target is the first element of list
            if(*list==generic) {
                *list = generic->next;
                generic->next=NULL; //free below removes the whole list!
                osl_generic_free(generic);
                return;
            }

            //find target
            tmp = *list;
            while (tmp->next!=generic && tmp->next != NULL)
                tmp = tmp->next;

            if(tmp->next==generic) {
                tmp->next = generic->next;
                generic->next=NULL; //free below removes the whole list!
                osl_generic_free(generic);
            } else //target not found
                OSL_warning("generic not found in the list\n");
        }

    }
}

/**
 * osl_generic_remove function:
 * given a URI, this function removes that generic from the list
 * \param[in] list    Address of a generic list
 * \param[in] URI     Pointer to the URI string
 */
void osl_generic_remove(osl_generic_p *list, char * URI) {

    osl_generic_p tmp = *list;

    while(tmp != NULL) {
        if(osl_generic_has_URI(tmp, URI))
            break;
        tmp = tmp->next;
    }

    if(tmp!=NULL) {
        osl_generic_remove_node(list, tmp);
    }

}


/**
 * osl_generic_malloc function:
 * This function allocates the memory space for an osl_generic_t
 * structure and sets its fields with default values. Then it returns a
 * pointer to the allocated space.
 * \return A pointer to an empty generic structure with fields set to
 *         default values.
 */
osl_generic_p osl_generic_malloc(void) {
    osl_generic_p generic;

    OSL_malloc(generic, osl_generic_p, sizeof(osl_generic_t));
    generic->interface = NULL;
    generic->data      = NULL;
    generic->next      = NULL;

    return generic;
}


/**
 * osl_generic_free function:
 * This function frees the allocated memory for a generic structure.
 * \param[in] generic The pointer to the generic structure we want to free.
 */
void osl_generic_free(osl_generic_p generic) {
    osl_generic_p next;

    while (generic != NULL) {
        next = generic->next;
        if (generic->interface != NULL) {
            generic->interface->free(generic->data);
            osl_interface_free(generic->interface);
        } else {
            if (generic->data != NULL) {
                OSL_warning("unregistered interface, memory leaks are possible");
                free(generic->data);
            }
        }
        free(generic);
        generic = next;
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_generic_number function:
 * this function returns the number of statements in the generic list
 * provided as parameter.
 * \param[in] generic The first element of the generic list.
 * \return The number of statements in the generic list.
 */
int osl_generic_number(osl_generic_p generic) {
    int number = 0;

    while (generic != NULL) {
        number++;
        generic = generic->next;
    }
    return number;
}


/**
 * osl_generic_clone function:
 * This function builds and returns a "hard copy" (not a pointer copy) of an
 * osl_generic_t data structure.
 * \param[in] generic The pointer to the generic structure we want to clone.
 * \return A pointer to the clone of the input generic structure.
 */
osl_generic_p osl_generic_clone(osl_generic_p generic) {
    return osl_generic_nclone(generic, -1);
}


/**
 * \brief This function builds and returns a "hard copy" (not a pointer copy)
 * of the n first elements of an osl_generic_t list.
 *
 * \param generic The pointer to the generic structure we want to clone.
 * \param n       The number of nodes we want to copy (n<0 for infinity).
 * \return The clone of the n first nodes of the generic list.
 */
osl_generic_p osl_generic_nclone(osl_generic_p generic, int n) {
    osl_generic_p clone = NULL, new;
    osl_interface_p interface;
    void * x;

    if (n < 0) {
        n = osl_generic_count(generic);
    }

    while ((generic != NULL) && (n > 0)) {
        if (generic->interface != NULL) {
            x = generic->interface->clone(generic->data);
            interface = osl_interface_clone(generic->interface);
            new = osl_generic_malloc();
            new->interface = interface;
            new->data = x;
            osl_generic_add(&clone, new);
        } else {
            OSL_warning("unregistered interface, cloning ignored");
        }
        generic = generic->next;
        n--;
    }

    return clone;
}


/**
 * osl_generic_count function:
 * this function counts the number of elements in the generic list provided
 * as parameter (x) and returns this number.
 * \param[in] x The list of generics.
 * \return  The number of elements in the list.
 */
int osl_generic_count(osl_generic_p x) {
    int generic_number = 0;

    while (x != NULL) {
        generic_number++;
        x = x->next;
    }

    return generic_number;
}


/**
 * osl_generic_equal function:
 * this function returns true if the two generic structures are the same,
 * false otherwise. This functions considers two generic structures as equal
 * independently of the order of the nodes.
 * \param[in] x1 The first generic structure.
 * \param[in] x2 The second generic structure.
 * \return 1 if x1 and x2 are the same (content-wise), 0 otherwise.
 */
int osl_generic_equal(osl_generic_p x1, osl_generic_p x2) {
    int x1_generic_number, x2_generic_number;
    int found, equal;
    osl_generic_p backup_x2 = x2;

    if (x1 == x2)
        return 1;

    // Check whether the number of generics is the same or not.
    x1_generic_number = osl_generic_count(x1);
    x2_generic_number = osl_generic_count(x2);
    if (x1_generic_number != x2_generic_number)
        return 0;

    // Check that for each generic in x1 a similar generic is in x2.
    while (x1 != NULL) {
        x2 = backup_x2;
        found = 0;
        while ((x2 != NULL) && (found != 1)) {
            if (osl_interface_equal(x1->interface, x2->interface)) {
                if (x1->interface != NULL) {
                    equal = x1->interface->equal(x1->data, x2->data);
                } else {
                    OSL_warning("unregistered generic, "
                                "cannot state generic equality");
                    equal = 0;
                }

                if (equal == 0)
                    return 0;
                else
                    found = 1;
            }

            x2 = x2->next;
        }

        if (found != 1)
            return 0;

        x1 = x1->next;
    }

    return 1;
}


/**
 * osl_generic_has_URI function:
 * this function returns 1 if the generic provided as parameter has
 * a given URI, 0 other wise.
 * \param[in] x   The generic structure to test.
 * \param[in] URI The URI value to test.
 * \return 1 if x has the provided URI, 0 otherwise.
 */
int osl_generic_has_URI(osl_const_generic_const_p x, char const * const URI) {

    if ((x == NULL) ||
            (x->interface == NULL) ||
            (x->interface->URI == NULL) ||
            (strcmp(x->interface->URI, URI)))
        return 0;

    return 1;
}


/**
 * osl_generic_lookup function:
 * this function returns the first generic with a given URI in the
 * generic list provided as parameter and NULL if it doesn't find such
 * a generic.
 * \param[in] x   The generic list where to search a given generic URI.
 * \param[in] URI The URI of the generic we are looking for.
 * \return The first generic of the requested URI in the list.
 */
void * osl_generic_lookup(osl_generic_p x, char const * const URI) {
    while (x != NULL) {
        if (osl_generic_has_URI(x, URI))
            return x->data;

        x = x->next;
    }

    return NULL;
}


/**
 * osl_generic_shell function:
 * this function creates and returns a generic structure "shell" which
 * embed the data and interface provided as parameters.
 * \param[in] data      Data to put in the generic shell.
 * \param[in] interface Interface to put in the generic shell.
 * \return A new generic structure containing the data and interface.
 */
osl_generic_p osl_generic_shell(void * data, osl_interface_p interface) {
    osl_generic_p generic = NULL;

    if ((data == NULL) || (interface == NULL))
        OSL_warning("shell created with some empty elements inside");

    generic = osl_generic_malloc();
    generic->data = data;
    generic->interface = interface;
    return generic;
}
