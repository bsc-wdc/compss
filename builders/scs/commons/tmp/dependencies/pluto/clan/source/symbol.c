
/*+------- <| --------------------------------------------------------**
 **         A                     Clan                                **
 **---     /.\   -----------------------------------------------------**
 **   <|  [""M#                 symbol.c                              **
 **-   A   | #   -----------------------------------------------------**
 **   /.\ [""M#         First version: 01/05/2008                     **
 **- [""M# | #  U"U#U  -----------------------------------------------**
      | #  | #  \ .:/
      | #  | #___| #
******  | "--'     .-"  ******************************************************
*     |"-"-"-"-"-#-#-##   Clan : the Chunky Loop Analyzer (experimental)     *
****  |     # ## ######  *****************************************************
*      \       .::::'/                                                       *
*       \      ::::'/     Copyright (C) 2008 University Paris-Sud 11         *
*     :8a|    # # ##                                                         *
*     ::88a      ###      This is free software; you can redistribute it     *
*    ::::888a  8a ##::.   and/or modify it under the terms of the GNU Lesser *
*  ::::::::888a88a[]:::   General Public License as published by the Free    *
*::8:::::::::SUNDOGa8a::. Software Foundation, either version 2.1 of the     *
*::::::::8::::888:Y8888:: License, or (at your option) any later version.    *
*::::':::88::::888::Y88a::::::::::::...                                      *
*::'::..    .   .....   ..   ...  .                                          *
* This software is distributed in the hope that it will be useful, but       *
* WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY *
* or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License   *
* for more details.							      *
*                                                                            *
* You should have received a copy of the GNU Lesser General Public License   *
* along with software; if not, write to the Free Software Foundation, Inc.,  *
* 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA                     *
*                                                                            *
* Clan, the Chunky Loop Analyzer                                             *
* Written by Cedric Bastoul, Cedric.Bastoul@u-psud.fr                        *
*                                                                            *
******************************************************************************/


#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>

#include <osl/macros.h>
#include <osl/strings.h>
#include <osl/generic.h>
#include <osl/relation.h>
#include <osl/relation_list.h>
#include <osl/extensions/arrays.h>
#include <clan/macros.h>
#include <clan/symbol.h>


void yyerror(char*);


/*+****************************************************************************
 *                          Structure display function                        *
 ******************************************************************************/


/**
 * clan_symbol_print_structure function:
 * Displays a clan_symbol_t structure (*symbol) into a file (file, possibly
 * stdout) in a way that trends to be understandable without falling in a deep
 * depression or, for the lucky ones, getting a headache... It includes an
 * indentation level (level) in order to work with others print_structure
 * functions.
 * \param[in] file   File where informations are printed.
 * \param[in] symbol The symbol whose information have to be printed.
 * \param[in] level  Number of spaces before printing, for each line.
 */
void clan_symbol_print_structure(FILE* file, clan_symbol_p symbol, int level) {
    int i, j, first = 1, number = 1;

    if (symbol != NULL) {
        // Go to the right level.
        for(j = 0; j < level; j++)
            fprintf(file, "|\t");
        fprintf(file, "+-- clan_symbol_t (node %d)\n", number);
    } else {
        // Go to the right level.
        for(j = 0; j < level; j++)
            fprintf(file, "|\t");
        fprintf(file, "+-- NULL symbol\n");
    }

    while (symbol != NULL) {
        if (!first) {
            // Go to the right level.
            for (j = 0; j < level; j++)
                fprintf(file, "|\t");
            fprintf(file, "|   clan_symbol_t (node %d)\n", number);
        } else {
            first = 0;
        }

        // A blank line.
        for (j = 0; j <= level + 1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        // Go to the right level and print the key.
        for (j = 0; j <= level; j++)
            fprintf(file, "|\t");
        fprintf(file, "Key: %d\n", symbol->key);

        // A blank line.
        for (j = 0; j <= level+1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        // Print the identifier.
        for (i = 0; i <= level; i++)
            fprintf(file, "|\t");
        if (symbol->identifier != NULL)
            fprintf(file, "+-- Identifier: %s\n", symbol->identifier);
        else
            fprintf(file, "+-- No identifier\n");

        // A blank line.
        for(j = 0; j <= level + 1; j++)
            fprintf(file, "|\t") ;
        fprintf(file, "\n") ;

        // Go to the right level and print the type.
        for (j = 0; j <= level; j++)
            fprintf(file, "|\t") ;
        fprintf(file, "Type: ") ;
        switch (symbol->type) {
        case CLAN_TYPE_ITERATOR :
            fprintf(file, "Iterator\n");
            break;
        case CLAN_TYPE_PARAMETER:
            fprintf(file, "Parameter\n");
            break;
        case CLAN_TYPE_ARRAY    :
            fprintf(file, "Array\n");
            break;
        case CLAN_TYPE_FUNCTION :
            fprintf(file, "Function\n");
            break;
        default :
            fprintf(file, "Unknown\n") ;
        }

        // A blank line.
        for (j = 0; j <= level + 1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        // Go to the right level and print the rank.
        for (j = 0; j <= level; j++)
            fprintf(file, "|\t");
        fprintf(file, "Rank: %d\n", symbol->rank);

        // A blank line.
        for (j = 0; j <= level + 1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        symbol = symbol->next;
        number++;

        // Next line.
        if (symbol != NULL) {
            for (j = 0; j <= level; j++)
                fprintf(file, "|\t");
            fprintf(file, "V\n");
        }
    }

    // The last line.
    for(j = 0; j <= level; j++)
        fprintf(file, "|\t");
    fprintf(file, "\n");
}


/**
 * clan_symbol_print function:
 * This function prints the content of a clan_symbol_t structure (*symbol) into
 * a file (file, possibly stdout).
 * \param[in] file   File where informations are printed.
 * \param[in] symbol The symbol whose information have to be printed.
 */
void clan_symbol_print(FILE* file, clan_symbol_p symbol) {
    clan_symbol_print_structure(file, symbol, 0);
}


/*+****************************************************************************
 *                    Memory allocation/deallocation function                 *
 ******************************************************************************/


/**
 * clan_symbol_malloc function:
 * This function allocates the memory space for a clan_symbol_t structure and
 * sets its fields with default values. Then it returns a pointer to the
 * allocated space.
 * \return A newly allocated symbol set with default values.
 */
clan_symbol_p clan_symbol_malloc() {
    clan_symbol_p symbol;

    CLAN_malloc(symbol, clan_symbol_p, sizeof(clan_symbol_t));
    symbol->key        = CLAN_UNDEFINED;
    symbol->identifier = NULL;
    symbol->type       = CLAN_UNDEFINED;
    symbol->rank       = CLAN_UNDEFINED;
    symbol->next       = NULL;

    return symbol;
}


/**
 * clan_symbol_free function:
 * This function frees the allocated memory for a clan_symbol_t structure.
 * \param[in,out] symbol The pointer to the symbol we want to free.
 */
void clan_symbol_free(clan_symbol_p symbol) {
    clan_symbol_p next;

    while (symbol != NULL) {
        next = symbol->next;
        free(symbol->identifier);
        free(symbol);
        symbol = next;
    }
}


/*+****************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/


/**
 * clan_symbol_lookup function:
 * This function searches the symbol table for a symbol with the identifier
 * provided as parameter. It returns the pointer to the symbol if it already
 * exists inside the table, NULL otherwise.
 * \param[in] symbol     The first node of the list of symbols.
 * \param[in] identifier The identifier we are looking for.
 * \return The symbol corresponding to identifier, NULL if it doesn't exist.
 */
clan_symbol_p clan_symbol_lookup(clan_symbol_p symbol, char* identifier) {
    while (symbol != NULL) {
        if (strcmp(symbol->identifier, identifier) == 0)
            return symbol;
        else
            symbol = symbol->next;
    }
    return NULL;
}


/**
 * clan_symbol_lookup_by_key function:
 * This function searches the symbol table for a symbol with the key
 * provided as parameter. It returns the pointer to the symbol if it already
 * exists inside the table, NULL otherwise.
 * \param[in] symbol The first node of the list of symbols.
 * \param[in] key    The key of the searched symbol.
 * \return The symbol corresponding to the key, or NULL if it doesn't exist.
 */
clan_symbol_p clan_symbol_lookup_by_key(clan_symbol_p symbol, int key) {
    while (symbol != NULL) {
        if (symbol->key == key)
            return symbol;
        else
            symbol = symbol->next;
    }
    return NULL;
}


/**
 * clan_symbol_generate_new_key function:
 * this function generates a key which is not yet present in the table.
 * \param[in] table The first element of the symbol table.
 * \return A key which would be convenient for a new symbol.
 */
static
int clan_symbol_generate_new_key(clan_symbol_p table) {
    int key = CLAN_KEY_START;

    while (table != NULL) {
        if (table->key >= key)
            key = table->key + 1;
        table = table->next;
    }
    return key;
}


/**
 * clan_symbol_push_at_end function
 * this function adds a symbol at the end of the symbol table whose address
 * is provided as a parameter. If the symbol table is empty (NULL), the new
 * node will become its first element.
 * \param[in,out] table  The address of the symbol table.
 * \param[in]     symbol The symbol to add to the table.
 */
void clan_symbol_push_at_end(clan_symbol_p* table, clan_symbol_p symbol) {
    clan_symbol_p tmp = *table;

    // We put the symbol at the end of the table.
    if (*table == NULL) {
        *table = symbol;
    } else {
        while (tmp->next != NULL)
            tmp = tmp->next;
        tmp->next = symbol;
    }
}


/**
 * clan_symbol_add function:
 * This function adds a new clan_symbol_t in the symbol table whose address
 * is provided as a parameter. If the symbol table is empty (NULL), the new
 * node will become its first element. A new node is added only if an
 * existing node with the same identifier does not already exist. It returns
 * the pointer to the symbol table node corresponding to the identifier.
 * \param[in,out] table      The address of the symbol table.
 * \param[in]     identifier The identifier of the symbol we want to add.
 * \param[in]     type       The new symbol type.
 */
clan_symbol_p clan_symbol_add(clan_symbol_p* table, char* identifier,
                              int type) {
    clan_symbol_p symbol;

    // If the identifier is already in the table, do nothing.
    symbol = clan_symbol_lookup(*table, identifier);
    if (symbol != NULL)
        return symbol;

    // Else, we allocate and fill a new clan_symbol_t node.
    symbol = clan_symbol_malloc();
    symbol->key = clan_symbol_generate_new_key(*table);
    symbol->identifier = strdup(identifier);
    symbol->type = type;

    // We put the new symbol at the end of the table.
    clan_symbol_push_at_end(table, symbol);

    return symbol;
}


/**
 * clan_symbol_get_key function:
 * This function returns the key of the symbol with identifier "identifier"
 * in the symbol table whose first element is "symbol". If the symbol with
 * the specified identifier is not found, it returns CLAN_UNDEFINED.
 * \param[in] symbol     The first node of the list of symbols.
 * \param[in] identifier The identifier we want to know the key.
 * \return The key corresponding to the identifier or CLAN_UNDEFINED.
 */
int clan_symbol_get_key(clan_symbol_p symbol, char* identifier) {
    while (symbol != NULL) {
        if (strcmp(symbol->identifier,identifier) == 0)
            return symbol->key;
        else
            symbol = symbol->next;
    }
    return CLAN_UNDEFINED;
}


/**
 * clan_symbol_get_rank function:
 * This function returns the rank of the symbol with identifier "identifier"
 * in the symbol table whose first element is "symbol". If the symbol with
 * the specified identifier is not found, it returns -1.
 * \param[in] symbol     The first node of the list of symbols.
 * \param[in] identifier The identifier we want to know the key.
 * \return The rank corresponding to the identifier or CLAN_UNDEFINED.
 */
int clan_symbol_get_rank(clan_symbol_p symbol, char* identifier) {
    while (symbol != NULL) {
        if (strcmp(symbol->identifier,identifier) == 0)
            return symbol->rank;
        else
            symbol = symbol->next;
    }
    return CLAN_UNDEFINED;
}


/**
 * clan_symbol_get_type function:
 * This function returns the type of the symbol with identifier "identifier"
 * in the symbol table whose first element is "symbol". If the symbol with
 * the specified identifier is not found, it returns -1.
 * \param[in] symbol     The first node of the list of symbols.
 * \param[in] identifier The identifier we want to know the type.
 * \return The type of the symbol corresponding to the identifier.
 */
int clan_symbol_get_type(clan_symbol_p symbol, char* identifier) {
    while (symbol != NULL) {
        if (strcmp(symbol->identifier,identifier) == 0)
            return symbol->type;
        else
            symbol = symbol->next;
    }
    return CLAN_UNDEFINED;
}


/**
 * clan_symbol_array_to_strings function:
 * this functions builds (and returns a pointer to) an osl_strings_t
 * structure containing the symbol strings contained in an array of
 * symbols of length nb. The symbol string order is the same as the one
 * in the symbol array.
 * \param[in] sarray The symbol array.
 * \param[in] size   The size of the symbol array.
 * \param[in] depths The depth of each xfor loop.
 * \param[in] labels The labels (i.e., the symbol version) of each xfor loop.
 * \return An osl_strings_t containing all the symbol strings.
 */
osl_strings_p clan_symbol_array_to_strings(clan_symbol_p* sarray, int size,
        int* depths, int* labels) {
    int i, j, xfor_index = 0, xfor_index_reuse = 0;
    clan_symbol_p symbol;
    osl_strings_p strings = osl_strings_malloc();

    // Fill the array of strings.
    for (i = 0; i < size; i++) {
        symbol = sarray[i];
        // If symbol has a non-NULL next field, it means it corresponds to
        // an xfor index that we have to select conveniently:
        if (symbol->next != NULL) {
            // -1. Select the convenient symbol thanks to the xfor label.
            for (j = 0; j < labels[xfor_index]; j++)
                symbol = symbol->next;

            // -2. Increment the number of times we used that symbol.
            xfor_index_reuse++;

            // -3. If we reached the current xfor depth, the next xfor index
            //     will be found in the next xfor loop nest.
            if (xfor_index_reuse >= depths[xfor_index]) {
                xfor_index++;
                xfor_index_reuse = 0;
            }
        }
        osl_strings_add(strings, symbol->identifier);
    }

    return strings;
}


/**
 * clan_symbol_nb_of_type function:
 * this function returns the number of symbols of a given type in the
 * symbol table.
 * \param[in] symbol The top of the symbol table.
 * \param[in] type   The type of the elements.
 * \return The number of symbols of the provoded type in the symbol table.
 */
int clan_symbol_nb_of_type(clan_symbol_p symbol, int type) {
    int nb = 0;

    while (symbol != NULL) {
        if (symbol->type == type)
            nb++;
        symbol = symbol->next;
    }

    return nb;
}


/**
 * clan_symbol_to_strings function:
 * this function builds (and returns a pointer to) an osl_generic_t
 * structure containing the symbol strings of a given type in the
 * symbol table. The osl_generic_t is a shell for an osl_strings_t
 * which actually stores the symbol strings. The symbol strings are sorted
 * in the same order as they appear in the symbol table. If there is no
 * corresponding symbol in the table, it returns NULL.
 * \param[in] symbol The top of the symbol table.
 * \param[in] type   The type of the elements.
 * \return An osl_generic_t with the symbol strings of the given type.
 */
osl_generic_p clan_symbol_to_strings(clan_symbol_p symbol, int type) {
    osl_strings_p strings = NULL;
    osl_generic_p generic;

    if (clan_symbol_nb_of_type(symbol, type) == 0)
        return NULL;

    // We scan the table a second time to fill the identifier array
    // Not optimal to act this way but overkills are worse!
    strings = osl_strings_malloc();
    while (symbol != NULL) {
        if (symbol->type == type) {
            osl_strings_add(strings, symbol->identifier);
        }
        symbol = symbol->next;
    }

    // Embed the strings in a generic shell.
    generic = osl_generic_shell(strings, osl_strings_interface());
    return generic;
}


/**
* clan_symbol_clone_one function:
* this function clones one symbol, i.e., it returns the clone of the symbol
* provided as an argument only, with a next field set to NULL.
* \param symbol The symbol to clone.
* \return The clone of the symbol (and this symbol only).
*/
clan_symbol_p clan_symbol_clone_one(clan_symbol_p symbol) {
    clan_symbol_p clone = clan_symbol_malloc();

    if (symbol->identifier != NULL)
        clone->identifier = strdup(symbol->identifier);
    clone->type = symbol->type;
    clone->rank = symbol->rank;

    return clone;
}


/**
 * clan_symbol_to_arrays function:
 * this function generates an arrays extension from the symbol table
 * passed as an argument. It embeds it in an osl_generic_t structure
 * before returning it.
 * \param[in] symbol The symbol table.
 * \return An arrays structure with all the arrays of the symbol table.
 */
osl_generic_p clan_symbol_to_arrays(clan_symbol_p symbol) {
    int i;
    int nb_arrays = 0;
    osl_arrays_p arrays = NULL;
    osl_generic_p generic = NULL;
    clan_symbol_p top = symbol;

    // A first scan to know how many arrays there are.
    while (symbol != NULL) {
        nb_arrays++;
        symbol = symbol->next;
    }

    // Build the arrays extension.
    if (nb_arrays > 0) {
        arrays = osl_arrays_malloc();
        CLAN_malloc(arrays->id, int*, nb_arrays * sizeof(int));
        CLAN_malloc(arrays->names, char**, nb_arrays * sizeof(char *));
        arrays->nb_names = nb_arrays;
        symbol = top;
        i = 0;
        while (symbol != NULL) {
            arrays->id[i] = symbol->key;
            CLAN_strdup(arrays->names[i], symbol->identifier);
            i++;
            symbol = symbol->next;
        }

        // Embed the arrays in a generic shell.
        generic = osl_generic_shell(arrays, osl_arrays_interface());
    }

    return generic;
}


/**
 * clan_symbol_new_iterator function:
 * this function return 1 if it succeeds to register (or to update) an
 * iterator in the symbol table and to add it to the iterator array. It
 * returns 0 otherwise. The reason for failure can be that the symbol
 * is already in use for something else than an iterator.
 * \param[in,out] table The symbol table.
 * \param[in,out] array The iterator array.
 * \param[in]     id    The textual name of the iterator.
 * \param[in]     depth The current loop depth.
 * \return 1 on success, 0 on failure.
 */
int clan_symbol_new_iterator(clan_symbol_p* table, clan_symbol_p* array,
                             char* id, int depth) {
    clan_symbol_p symbol;
    symbol = clan_symbol_add(table, id, CLAN_TYPE_ITERATOR);

    // Ensure that the returned symbol was either a new one, or of the same type.
    if (symbol->type != CLAN_TYPE_ITERATOR) {
        yyerror("a loop iterator was previously used for something else");
        return 0;
    }

    // Update the rank, in case the symbol already exists.
    if (symbol->rank != depth + 1)
        symbol->rank = depth + 1;

    clan_symbol_push_at_end(&array[depth], clan_symbol_clone_one(symbol));
    return 1;
}


/**
 * clan_symbol_update_type function:
 * this function returns 1 if it succeeds to modify the type of a symbol
 * in the symbol table. The modified symbol corresponds to the reference
 * accessed in the last access relation of an access list provided as
 * parameter. It returns 0 if it fails. The reasons for a failure can be
 * either to try to modify an iterator type or a parameter type since they
 * are supposed to be dead-ends.
 * \param[in,out] table  The first symbol of the symbol table (one element
 *                       may be modified).
 * \param[in]     access A list of access relations.
 * \param[in]     type   The new type for the symbol we want to update.
 * \return 1 on success, 0 on failure.
 */
int clan_symbol_update_type(clan_symbol_p table, osl_relation_list_p access,
                            int type) {
    int key;
    int relation_type;
    clan_symbol_p symbol;

    if (table == NULL)
        CLAN_error("cannot even try to update type: NULL symbol table");
    if (access == NULL)
        CLAN_error("cannot even try to update type: NULL access list");

    // We will only consider the last reference in the list.
    while (access->next != NULL)
        access = access->next;

    // Get the key (with some cheating with the relation type to be able to use
    // osl_relation_get_array_id), find the corresponding symbol and update.
    relation_type = access->elt->type;
    access->elt->type = OSL_TYPE_READ;
    key = osl_relation_get_array_id(access->elt);
    access->elt->type = relation_type;
    symbol = clan_symbol_lookup_by_key(table, key);
    if (symbol == NULL)
        CLAN_error("no symbol corresponding to the key");

    if ((symbol->type == CLAN_TYPE_ITERATOR) && (type != CLAN_TYPE_ITERATOR)) {
        yyerror("illegal use of an iterator (update or reference) in a statement");
        return 0;
    }

    if ((symbol->type == CLAN_TYPE_PARAMETER) && (type != CLAN_TYPE_PARAMETER)) {
        yyerror("illegal use of a parameter (update or reference) in a statement");
        return 0;
    }

    symbol->type = type;
    return 1;
}
