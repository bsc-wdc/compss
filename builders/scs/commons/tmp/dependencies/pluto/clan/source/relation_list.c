
/*+------- <| --------------------------------------------------------**
 **         A                     Clan                                **
 **---     /.\   -----------------------------------------------------**
 **   <|  [""M#              relation_list.c                          **
 **-   A   | #   -----------------------------------------------------**
 **   /.\ [""M#         First version: 16/04/2011                     **
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
#include <string.h>
#include <ctype.h>

#include <osl/macros.h>
#include <osl/relation_list.h>
#include <clan/relation.h>
#include <clan/relation_list.h>


/*+****************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/


/**
 * clan_relation_lit_compact function:
 * This function compacts a relation list such that each relation inside
 * uses the right number of columns (during construction we used
 * CLAN_MAX_DEPTH, CLAN_MAX_LOCAL_DIMS and CLAN_MAX_PARAMETERS to define
 * relation and vector sizes). It modifies directly the relation list
 * provided as parameter.
 * \param[in,out] list          The relation list to compact.
 * \param[in]     nb_parameters The true number of parameters in the SCoP.
 */
void clan_relation_list_compact(osl_relation_list_p list, int nb_parameters) {
    while (list != NULL) {
        clan_relation_compact(list->elt, nb_parameters);
        list = list->next;
    }
}


/**
 * clan_relation_list_define_type function:
 * this function sets the type of each relation in the relation list to the
 * one provided as parameter, only if it is undefined.
 * \param[in,out] list The list of relations to set the type.
 * \param[in]     type The type.
 */
void clan_relation_list_define_type(osl_relation_list_p list, int type) {
    osl_relation_p relation;

    while (list != NULL) {
        if (list->elt != NULL) {
            relation = list->elt;
            while (relation != NULL) {
                if (relation->type == OSL_UNDEFINED)
                    relation->type = type;
                relation = relation->next;
            }
        }
        list = list->next;
    }
}


/**
 * clan_relation_list_nb_elements function:
 * this function returns the number of elements in a relation list.
 * \param[in] list The list of relation we want to count the nb of elements.
 * \return The number of elements in the input relation list.
 */
int clan_relation_list_nb_elements(osl_relation_list_p list) {
    int nb_elements = 0;

    while (list != NULL) {
        nb_elements++;
        list = list->next;
    }
    return nb_elements;
}
