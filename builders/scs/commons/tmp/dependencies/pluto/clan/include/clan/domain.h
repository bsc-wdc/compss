
/*+------- <| --------------------------------------------------------**
 **         A                     Clan                                **
 **---     /.\   -----------------------------------------------------**
 **   <|  [""M#                 domain.h                              **
 **-   A   | #   -----------------------------------------------------**
 **   /.\ [""M#         First version: 26/09/2014                     **
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


#ifndef CLAN_DOMAIN_H
# define CLAN_DOMAIN_H

# include <clan/symbol.h>
# include <clan/options.h>

# if defined(__cplusplus)
extern "C"
{
# endif


struct osl_relation_list;
struct osl_relation;


/**
 * The clan_domain_t structure stores a (NULL-terminated linked)
 * list of constraint sets, each of them being a relation list.
 */
struct clan_domain {
    struct osl_relation_list* constraints; /**< An element of the domain list. */
    struct clan_domain* next;              /**< Pointer to the next element. */
};
typedef struct clan_domain  clan_domain_t;
typedef struct clan_domain* clan_domain_p;


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/
void          clan_domain_idump(FILE*, clan_domain_p, int);
void          clan_domain_dump(FILE*, clan_domain_p);


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/
clan_domain_p clan_domain_malloc();
void          clan_domain_free(clan_domain_p);


/*+****************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/
clan_domain_p clan_domain_clone(clan_domain_p);
void          clan_domain_push(clan_domain_p*, clan_domain_p);
clan_domain_p clan_domain_pop(clan_domain_p*);
void          clan_domain_dup(clan_domain_p*);
void          clan_domain_drop(clan_domain_p*);
void          clan_domain_and(clan_domain_p, struct osl_relation*);
void          clan_domain_stride(clan_domain_p, int, int);
void          clan_domain_for(clan_domain_p, int, clan_symbol_p,
                              struct osl_relation*, struct osl_relation*,
                              int, clan_options_p);
void          clan_domain_xfor(clan_domain_p, int, clan_symbol_p,
                               struct osl_relation_list*, struct osl_relation_list*,
                               int*, clan_options_p);

# if defined(__cplusplus)
}
# endif
#endif /* define CLAN_DOMAIN_H */
