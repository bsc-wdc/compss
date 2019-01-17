
/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (                   util.c                                **
 **----  \#/  --------------------------------------------------------**
 **    .-"#'-.        First version: june 7th 2012                    **
 **--- |"-.-"| -------------------------------------------------------**
       |     |
       |     |
******** |     | *************************************************************
* CAnDL  '-._,-' the Chunky Analyzer for Dependences in Loops (experimental) *
******************************************************************************
*                                                                            *
* Copyright (C) 2003-2008 Cedric Bastoul                                     *
*                                                                            *
* This is free software; you can redistribute it and/or modify it under the  *
* terms of the GNU General Public License as published by the Free Software  *
* Foundation; either version 2 of the License, or (at your option) any later *
* version.                                                                   *
*                                                                            *
* This software is distributed in the hope that it will be useful, but       *
* WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY *
* or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License   *
* for more details.                                                          *
*                                                                            *
* You should have received a copy of the GNU General Public License along    *
* with software; if not, write to the Free Software Foundation, Inc.,        *
* 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA                     *
*                                                                            *
* CAnDL, the Chunky Dependence Analyzer                                      *
* Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                         *
*                                                                            *
******************************************************************************/

/*
 * author Joel Poudroux and Cedric Bastoul
 */

#include <stdio.h>
#include <stdlib.h>
#include <osl/statement.h>
#include <osl/relation.h>
#include <osl/macros.h>
#include <osl/extensions/dependence.h>
#include <osl/relation_list.h>
#include <osl/scop.h>
#include <candl/macros.h>
#include <candl/util.h>
#include <candl/statement.h>


/**
 * candl_util_relation_get_line function:
 * It returns the first line where the value is
 * different from zero in the `column'. `column' is between 0 and
 * nb_columns-1
 * Because the lines in the scattering or access matrix may have not
 * been ordered, we have to search the corresponding line, so you
 * can use this function for that.
 * \param[in] relation
 * \param[in] column        Line to search
 * \return                  Return the real line
 */
int candl_util_relation_get_line(osl_relation_p relation, int column) {
    if ((column < 0) || (column >= (relation->nb_columns-1)))
        return -1;
    int i;
    int precision = relation->precision;
    for (i = 0 ; i < relation->nb_rows ; i++) {
        if (!osl_int_zero(precision, relation->m[i][column + 1])) {
            break;
        }
    }
    return (i == relation->nb_rows ? -1 : i );
}



/* Check if two scop can be compared (same number of statements and
 * same access array/domain in the same order)
 *
 * \param[in] s1  first scop to compare
 * \param[in] s2  second scop to compare
 * \return        1 if two scops equal, 0 otherwise
 */
int candl_util_check_scop(osl_scop_p s1, osl_scop_p s2) {

    osl_statement_p it1 = s1->statement, it2 = s2->statement;
    for (; it1 != NULL && it2 != NULL ; it1 = it1->next, it2 = it2->next) {
        if (!osl_relation_list_equal(it1->access, it2->access))
            return 0;
        if (!osl_relation_equal(it1->domain, it2->domain))
            return 0;
    }

    /* Different number of statements */
    if ((it1 == NULL || it2 == NULL) && it1 != it2)
        return 0;

    return 1;
}

/* Extends the candl_util_check_scop() functionality to a list of scops
 * Compares each scop in s1 to the corresponding element in list s2
 * same access array/domain in the same order)
 *
 * \param[in] s1  first scop List to compare
 * \param[in] s2  second scop List to compare
 * \return    1 if two scops lists equal, 0 otherwise
 */
int candl_util_check_scop_list(osl_scop_p s1, osl_scop_p s2) {

    while ((s1!=NULL) && (s2!=NULL)) {
        if(!candl_util_check_scop(s1, s2))
            return 0;

        s1 = s1->next;
        s2 = s2->next;
    }

    /* Different number of scops */
    if ((s1 == NULL || s2 == NULL) && s1 != s2)
        return 0;

    /*scop lists can be compared*/
    return 1;
}

/* Return the number access array which have the type `type'
 */
static int count_nb_access(osl_statement_p st, int type) {
    osl_relation_list_p access = st->access;
    int count = 0;
    for (; access != NULL ; access = access->next)
        if (access->elt->type == type)
            count ++;
    return count;
}


/**
 * candl_util_statement_commute function:
 * This function returns 1 if the two statements given as parameter commute,
 * 0 otherwise. It uses the statement type information to answer the question.
 * - 09/12/2005: first version.
 */
int candl_util_statement_commute(osl_statement_p statement1,
                                 osl_statement_p statement2) {
    candl_statement_usr_p usr1, usr2;
    int type1, type2;
    int id1, id2;

    usr1  = statement1->usr;
    usr2  = statement2->usr;
    type1 = usr1->type;
    type2 = usr2->type;

    /* In the case of self-dependence, a statement commutes with hitself if
     * it is a reduction.
     */
    if ((statement1 == statement2) &&
            ((type1 == OSL_DEPENDENCE_P_REDUCTION) ||
             (type1 == OSL_DEPENDENCE_M_REDUCTION) ||
             (type1 == OSL_DEPENDENCE_T_REDUCTION)))
        return 1;

    /* Two statement commute when they are a reduction of the same type (or if
     * their left and right members are the same, but it's not exploited here).
     * The type may differ if it is either minus or plus-reduction. Furthermore,
     * they have to write onto the same array (and only one array).
     */
    if ((type1 == OSL_DEPENDENCE_P_REDUCTION && type2 == OSL_DEPENDENCE_P_REDUCTION) ||
            (type1 == OSL_DEPENDENCE_M_REDUCTION && type2 == OSL_DEPENDENCE_M_REDUCTION) ||
            (type1 == OSL_DEPENDENCE_T_REDUCTION && type2 == OSL_DEPENDENCE_T_REDUCTION) ||
            (type1 == OSL_DEPENDENCE_P_REDUCTION && type2 == OSL_DEPENDENCE_M_REDUCTION) ||
            (type1 == OSL_DEPENDENCE_M_REDUCTION && type2 == OSL_DEPENDENCE_P_REDUCTION)) {
        /* Here we check that there is one, only one and the same array. */
        if (count_nb_access(statement1, OSL_TYPE_WRITE) > 1 ||
                count_nb_access(statement2, OSL_TYPE_WRITE) > 1)
            return 0;

        /* search the first osl_write access */
        osl_relation_list_p access1 = statement1->access;
        osl_relation_list_p access2 = statement2->access;
        for (; access1 != NULL && access2 != NULL ;
                access1 = access1->next, access2 = access2->next)
            if (access1->elt->type == OSL_TYPE_WRITE)
                break;

        if (access1 == NULL || access2 == NULL ||
                access2->elt->type != OSL_TYPE_WRITE ||
                access2->elt->nb_output_dims != access1->elt->nb_output_dims) {
            osl_statement_dump(stderr, statement1);
            osl_statement_dump(stderr, statement2);
            CANDL_error("These statements haven't the same access array or access is NULL");
        }

        /* Check if the first dim (the Arr column) is the same */
        id1 = osl_relation_get_array_id(access1->elt);
        id2 = osl_relation_get_array_id(access2->elt);
        if (id1 != id2)
            return 0;

        return 1;
    }

    return 0;
}
