
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                     extensions/symbols.c                        **
 **-----------------------------------------------------------------**
 **                   First version: 07/03/2012                     **
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
* Cedric Bastoul      <Cedric.Bastoul@u-psud.fr>                            *
* Louis-Noel Pouchet  <Louis-Noel.pouchet@inria.fr>                         *
* Prasanth Chatharasi <prasanth@iith.ac.in>                                 *
*                                                                           *
*****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <osl/macros.h>
#include <osl/util.h>
#include <osl/relation.h>
#include <osl/interface.h>
#include <osl/extensions/symbols.h>


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * osl_symbols_idump function:
 * this function displays an osl_symbols_t structure (*symbols) into a
 * file (file, possibly stdout) in a way that trends to be understandable. It
 * includes an indentation level (level) in order to work with others
 * idump functions.
 * \param[in] file    The file where the information has to be printed.
 * \param[in] symbols The symbols structure to print.
 * \param[in] level   Number of spaces before printing, for each line.
 */
void osl_symbols_idump(FILE * file, osl_symbols_p symbols, int level) {

    int i, j, first = 1, number = 1;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file, "|\t");

    if (symbols != NULL)
        fprintf(file, "+-- osl_symbols_t\n");
    else
        fprintf(file, "+-- NULL symbols\n");

    while (symbols != NULL) {
        if (!first) {
            // Go to the right level.
            for (j = 0; j < level; j++)
                fprintf(file, "|\t");
            fprintf(file, "|   osl_symbol_t (node %d)\n", number);
        } else {
            first = 0;
        }

        // A blank line.
        for (j = 0; j <= level+1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        // 1. Print the symbol kind.
        for (i = 0; i <= level; i++)
            fprintf(file, "|\t");
        if (symbols->type != OSL_UNDEFINED) {
            fprintf(file, "+-- Type: ");
            switch (symbols->type) {
            case OSL_SYMBOL_TYPE_ITERATOR :
                fprintf(file, "Iterator\n");
                break;
            case OSL_SYMBOL_TYPE_PARAMETER:
                fprintf(file, "Parameter\n");
                break;
            case OSL_SYMBOL_TYPE_ARRAY    :
                fprintf(file, "Array\n");
                break;
            case OSL_SYMBOL_TYPE_FUNCTION :
                fprintf(file, "Function\n");
                break;
            default :
                fprintf(file, "Unknown\n") ;
            }
        } else {
            fprintf(file, "+-- NULL type\n");
        }

        // A blank line.
        for(j = 0; j <= level + 1; j++)
            fprintf(file, "|\t") ;
        fprintf(file, "\n") ;

        // 2. Print the origin of the symbol.
        for (i = 0; i <= level; i++)
            fprintf(file, "|\t");
        if (symbols->generated != OSL_UNDEFINED)
            fprintf(file, "+-- Origin: %d\n", symbols->generated);
        else
            fprintf(file, "+-- Undefined origin\n");

        // A blank line.
        for(j = 0; j <= level + 1; j++)
            fprintf(file, "|\t") ;
        fprintf(file, "\n") ;

        // 3. Print the number of array dimensions for the symbol.
        for (i = 0; i <= level; i++)
            fprintf(file, "|\t");
        if (symbols->nb_dims != OSL_UNDEFINED)
            fprintf(file, "+-- Number of Dimensions: %d\n", symbols->nb_dims);
        else
            fprintf(file, "+-- Undefined number of dimensions\n");

        // A blank line.
        for(j = 0; j <= level + 1; j++)
            fprintf(file, "|\t") ;
        fprintf(file, "\n") ;

        // 4. Print the symbol identifier.
        osl_generic_idump(file, symbols->identifier, level + 1);

        // 5. Print the data type of the symbol.
        osl_generic_idump(file, symbols->datatype, level + 1);

        // 6. Print the scope of the symbol.
        osl_generic_idump(file, symbols->scope, level + 1);

        // 7. Print the extent of the symbol.
        osl_generic_idump(file, symbols->extent, level + 1);

        symbols = symbols->next;
        number++;
        // Next line.
        if (symbols != NULL) {
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
 * osl_symbols_dump function:
 * this function prints the content of an osl_symbols_t structure
 * (*symbols) into a file (file, possibly stdout).
 * \param[in] file    The file where the information has to be printed.
 * \param[in] symbols The symbols structure to print.
 */
void osl_symbols_dump(FILE * file, osl_symbols_p symbols) {
    osl_symbols_idump(file, symbols, 0);
}


/**
 * osl_symbols_sprint function:
 * this function prints the content of an osl_symbols_t structure
 * (*symbols) into a string (returned) in the OpenScop textual format.
 * \param[in] symbols The symbols structure to print.
 * \return A string containing the OpenScop dump of the symbols structure.
 */
char * osl_symbols_sprint(osl_symbols_p symbols) {
    int i = 1;
    size_t high_water_mark = OSL_MAX_STRING;
    char* string = NULL, *temp;
    char buffer[OSL_MAX_STRING];

    OSL_malloc(string, char *, high_water_mark * sizeof(char));
    string[0] = '\0';

    sprintf(buffer, "# Number of symbols\n%d\n",
            osl_symbols_get_nb_symbols(symbols));
    osl_util_safe_strcat(&string, buffer, &high_water_mark);

    while (symbols != NULL) {
        sprintf(buffer, "# ===========================================\n");
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
        sprintf(buffer, "# %d Data for symbol number %d \n", i, i);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        if (symbols->type == OSL_UNDEFINED) {
            sprintf(buffer, "# %d.1 Symbol type\nUndefined\n", i);
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
        } else {
            sprintf(buffer, "# %d.1 Symbol type\n", i);
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
            switch (symbols->type) {
            case OSL_SYMBOL_TYPE_ITERATOR :
                sprintf(buffer, "Iterator\n");
                break;
            case OSL_SYMBOL_TYPE_PARAMETER:
                sprintf(buffer, "Parameter\n");
                break;
            case OSL_SYMBOL_TYPE_ARRAY    :
                sprintf(buffer, "Array\n");
                break;
            case OSL_SYMBOL_TYPE_FUNCTION :
                sprintf(buffer, "Function\n");
                break;
            default :
                sprintf(buffer, "Undefined\n") ;
            }
            osl_util_safe_strcat(&string, buffer, &high_water_mark);
        }

        // Printing Generated Boolean flag
        sprintf(buffer, "\n# %d.2 Generated Boolean\n%d\n", i, symbols->generated);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        // Printing Number of dimensions
        sprintf(buffer,"\n# %d.3 Number of dimensions\n%d\n", i, symbols->nb_dims);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);

        // Printing Identifier
        sprintf(buffer, "\n# %d.4 Identifier\n", i);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
        temp = osl_generic_sprint(symbols->identifier);
        osl_util_safe_strcat(&string, temp, &high_water_mark);
        free(temp);

        // Printing Datatype
        sprintf(buffer, "\n# %d.5 Datatype\n", i);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
        temp = osl_generic_sprint(symbols->datatype);
        osl_util_safe_strcat(&string, temp, &high_water_mark);
        free(temp);

        // Printing Scope
        sprintf(buffer, "\n# %d.6 Scope\n", i);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
        temp = osl_generic_sprint(symbols->scope);
        osl_util_safe_strcat(&string, temp, &high_water_mark);
        free(temp);

        // Printing Extent
        sprintf(buffer, "\n# %d.7 Extent\n", i);
        osl_util_safe_strcat(&string, buffer, &high_water_mark);
        temp = osl_generic_sprint(symbols->extent);
        osl_util_safe_strcat(&string, temp, &high_water_mark);
        free(temp);

        symbols = symbols->next;
    }

    OSL_realloc(string, char *, (strlen(string) + 1) * sizeof(char));
    return string;
}


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/**
 * osl_symbols_sread function:
 * this function reads a symbols structure from a string complying to the
 * OpenScop textual format and returns a pointer to this symbols structure.
 * The input parameter is updated to the position in the input string this
 * function reach right after reading the symbols structure.
 * \param[in,out] input The input string where to find a symbols.
 *                      Updated to the position after what has been read.
 * \return A pointer to the symbols structure that has been read.
 */
osl_symbols_p osl_symbols_sread(char ** input) {
    int nb_symbols;
    char* type;
    osl_symbols_p symbols;
    osl_symbols_p head;
    osl_interface_p registry;

    if (*input == NULL) {
        OSL_debug("no symbols optional tag");
        return NULL;
    }

    if (strlen(*input) > OSL_MAX_STRING)
        OSL_error("symbols too long");

    // Find the number of names provided.
    nb_symbols = osl_util_read_int(NULL, input);

    if (nb_symbols == 0)
        return NULL;

    head = symbols = osl_symbols_malloc();
    registry = osl_interface_get_default_registry();

    while (nb_symbols != 0) {
        // Reading the type of symbol
        type = osl_util_read_string(NULL, input);
        if (type != NULL) {
            if (strcmp(type, "Iterator") == 0)
                symbols->type = OSL_SYMBOL_TYPE_ITERATOR;
            else if (strcmp(type, "Parameter") == 0)
                symbols->type = OSL_SYMBOL_TYPE_PARAMETER;
            else if (strcmp(type, "Array") == 0)
                symbols->type = OSL_SYMBOL_TYPE_ARRAY;
            else if (strcmp(type, "Function") == 0)
                symbols->type = OSL_SYMBOL_TYPE_FUNCTION;
            else
                symbols->type = OSL_UNDEFINED;
            free(type);
        }

        // Reading origin of symbol
        symbols->generated = osl_util_read_int(NULL, input);

        // Reading the number of dimensions of a symbol
        symbols->nb_dims = osl_util_read_int(NULL, input);

        // Reading identifier
        symbols->identifier = osl_generic_sread_one(input, registry);

        // Reading data type
        symbols->datatype = osl_generic_sread_one(input, registry);

        // Reading scope
        symbols->scope = osl_generic_sread_one(input, registry);

        // Reading extent
        symbols->extent = osl_generic_sread_one(input, registry);

        nb_symbols --;
        if (nb_symbols != 0) {
            symbols->next = osl_symbols_malloc ();
            symbols = symbols->next;
        }
    }

    osl_interface_free(registry);
    return head;
}


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/


/**
 * osl_symbols_malloc function:
 * this function allocates the memory space for an osl_symbols_t
 * structure and sets its fields with default values. Then it returns a
 * pointer to the allocated space.
 * \return A pointer to an empty symbols structure with fields set to
 *         default values.
 */
osl_symbols_p osl_symbols_malloc(void) {
    osl_symbols_p symbols;

    OSL_malloc(symbols, osl_symbols_p, sizeof(osl_symbols_t));
    symbols->type       = OSL_UNDEFINED;
    symbols->generated  = OSL_UNDEFINED;
    symbols->nb_dims    = OSL_UNDEFINED;
    symbols->identifier = NULL;
    symbols->datatype   = NULL;
    symbols->scope      = NULL;
    symbols->extent     = NULL;
    symbols->next       = NULL;

    return symbols;
}


/**
 * osl_symbols_free function:
 * this function frees the allocated memory for an osl_symbols_t
 * structure.
 * \param[in,out] symbols The pointer to the symbols structure to free.
 */
void osl_symbols_free(osl_symbols_p symbols) {
    osl_symbols_p tmp;

    while (symbols != NULL) {
        tmp = symbols->next;
        osl_generic_free(symbols->identifier);
        osl_generic_free(symbols->datatype);
        osl_generic_free(symbols->scope);
        osl_generic_free(symbols->extent);
        free(symbols);
        symbols = tmp;
    }
}


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


/**
 * osl_symbols_add function:
 * this function adds a scop "scop" at the end of the symbols list pointed
 * by "location".
 * \param[in,out] location  Address of the first element of the symbols list.
 * \param[in]     symbols   The symbols to add to the list.
 */
void osl_symbols_add(osl_symbols_p* location, osl_symbols_p symbols) {
    while (*location != NULL)
        location = &((*location)->next);

    *location = symbols;
}


/**
 * osl_symbols_nclone function:
 * This function builds and returns a "hard copy" (not a pointer copy) of the
 * n first elements of an osl_symbols_t list.
 * \param symbols The pointer to the symbols structure we want to clone.
 * \param n       The number of nodes we want to copy (-1 for infinity).
 * \return The clone of the n first nodes of the symbols list.
 */
osl_symbols_p osl_symbols_nclone(osl_symbols_p symbols, int n) {
    osl_symbols_p clone = NULL, new;
    int i = 0;

    while ((symbols != NULL) && ((n == -1) || (i < n))) {
        new             = osl_symbols_malloc();
        new->type       = symbols->type;
        new->generated  = symbols->generated;
        new->nb_dims    = symbols->nb_dims;
        new->identifier = osl_generic_clone(symbols->identifier);
        new->datatype   = osl_generic_clone(symbols->datatype);
        new->scope      = osl_generic_clone(symbols->scope);
        new->extent     = osl_generic_clone(symbols->extent);

        osl_symbols_add(&clone, new);
        symbols = symbols->next;
        i++;
    }

    return clone;
}


/**
 * osl_symbols_clone function:
 * this function builds and returns a "hard copy" (not a pointer copy) of an
 * osl_symbols_t data structure.
 * \param[in] symbols The pointer to the symbols structure to clone.
 * \return A pointer to the clone of the symbols structure.
 */
osl_symbols_p osl_symbols_clone(osl_symbols_p symbols) {

    return osl_symbols_nclone(symbols, -1);
}


/**
 * osl_symbols_equal function:
 * this function returns true if the two symbols structures are the same
 * (content-wise), false otherwise.
 * \param[in] c1  The first symbols structure.
 * \param[in] c2  The second symbols structure.
 * \return 1 if c1 and c2 are the same (content-wise), 0 otherwise.
 */
int osl_symbols_equal(osl_symbols_p c1, osl_symbols_p c2) {

    if (c1 == c2)
        return 1;

    if (((c1 == NULL) && (c2 != NULL)) || ((c1 != NULL) && (c2 == NULL)))
        return 0;

    if (c1->type == c2->type && c1->generated == c2->generated &&
            c1->nb_dims == c2->nb_dims) {
        if (osl_generic_equal(c1->identifier, c2->identifier)) {
            if (osl_generic_equal(c1->datatype, c2->datatype)) {
                if (osl_generic_equal(c1->scope, c2->scope)) {
                    if (osl_generic_equal(c1->extent, c2->extent)) {
                        return 1;
                    }
                }
            }
        }
    }

    return 0;
}


/**
 * osl_symbols_get_nb_symbols function:
 * this function returns the number of symbols in the symbol list provided
 * as input.
 * \param symbols The head of the symbol list.
 * \return The number of symbols in the symbol list.
 */
int osl_symbols_get_nb_symbols(osl_symbols_p symbols) {
    int nb_symbols = 0;

    while (symbols != NULL) {
        nb_symbols++;
        symbols = symbols->next;
    }
    return nb_symbols;
}


/**
 * osl_symbols_interface function:
 * this function creates an interface structure corresponding to the symbols
 * extension and returns it).
 * \return An interface structure for the symbols extension.
 */
osl_interface_p osl_symbols_interface(void) {
    osl_interface_p interface = osl_interface_malloc();

    OSL_strdup(interface->URI, OSL_URI_SYMBOLS);
    interface->idump  = (osl_idump_f)osl_symbols_idump;
    interface->sprint = (osl_sprint_f)osl_symbols_sprint;
    interface->sread  = (osl_sread_f)osl_symbols_sread;
    interface->malloc = (osl_malloc_f)osl_symbols_malloc;
    interface->free   = (osl_free_f)osl_symbols_free;
    interface->clone  = (osl_clone_f)osl_symbols_clone;
    interface->equal  = (osl_equal_f)osl_symbols_equal;
    return interface;
}
