
/*+------- <| --------------------------------------------------------**
 **         A                     Clan                                **
 **---     /.\   -----------------------------------------------------**
 **   <|  [""M#                 symbol.h                              **
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


#ifndef CLAN_SYMBOL_H
# define CLAN_SYMBOL_H

# if defined(__cplusplus)
extern "C"
{
# endif

struct osl_strings;
struct osl_generic;
struct osl_relation_list;

/**
 * The clan_symbol_t structure is a node of the symbol table of the parser.
 */
struct clan_symbol {
    int key;                  /**< Unique symbol key */
    char* identifier;         /**< Symbol identifier */
    int type;                 /**< Symbol type (variable, iterator...) */
    int rank;                 /**< Depth for iterators, number for parameters */
    struct clan_symbol* next; /**< Next symbol in the symbol table */
};
typedef struct clan_symbol  clan_symbol_t;
typedef struct clan_symbol* clan_symbol_p;


/*+****************************************************************************
 *                          Structure display function                        *
 ******************************************************************************/
void                clan_symbol_print_structure(FILE*, clan_symbol_p, int);
void                clan_symbol_print(FILE*, clan_symbol_p);


/*+****************************************************************************
 *                    Memory allocation/deallocation function                 *
 ******************************************************************************/
clan_symbol_p       clan_symbol_malloc();
void                clan_symbol_free(clan_symbol_p);


/*+****************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/
clan_symbol_p       clan_symbol_lookup(clan_symbol_p, char*);
clan_symbol_p       clan_symbol_lookup_by_key(clan_symbol_p, int);
void                clan_symbol_push_at_end(clan_symbol_p*, clan_symbol_p);
clan_symbol_p       clan_symbol_add(clan_symbol_p*, char*, int);
int                 clan_symbol_get_rank(clan_symbol_p, char*);
int                 clan_symbol_get_type(clan_symbol_p, char*);
struct osl_strings* clan_symbol_array_to_strings(clan_symbol_p*,
        int, int*, int*);
int                 clan_symbol_nb_of_type(clan_symbol_p, int);
struct osl_generic* clan_symbol_to_strings(clan_symbol_p, int);
clan_symbol_p       clan_symbol_clone_one(clan_symbol_p);
struct osl_generic* clan_symbol_to_arrays(clan_symbol_p);
int                 clan_symbol_new_iterator(clan_symbol_p*, clan_symbol_p*,
        char*, int);
int                 clan_symbol_update_type(clan_symbol_p,
        struct osl_relation_list*, int);

# if defined(__cplusplus)
}
# endif
#endif /* define CLAN_SYMBOL_H */
