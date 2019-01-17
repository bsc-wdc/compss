

/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (                   usr.c                                 **
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

#include <stdlib.h>
#include <string.h>
#include <osl/scop.h>
#include <osl/statement.h>
#include <osl/extensions/dependence.h>
#include <osl/relation.h>
#include <candl/macros.h>
#include <candl/statement.h>
#include <candl/util.h>

#define CANDL_STATEMENT_USR_INDEX_MAX_LOOP_DEPTH 128


/**
 * candl_statement_usr_init_all function:
 * Init each candl_statement_usr structure of statements
 */
void candl_statement_usr_init_all(osl_scop_p scop) {

    /* TODO
     * that statements must be sorted to compute the statement label
     * the problem is if the scop is reordered, the second transformed scop
     * must be aligned with it
    */

    osl_statement_p iter;
    osl_relation_p scattering;
    candl_statement_usr_p stmt_usr;
    int i, j, k;
    int row;
    int precision = scop->context->precision;
    int count = 0; /* counter for statements */

    /* Initialize structures used in iterator indices computation. */
    int val;
    int max = 0;
    int cur_index[CANDL_STATEMENT_USR_INDEX_MAX_LOOP_DEPTH];
    int last[CANDL_STATEMENT_USR_INDEX_MAX_LOOP_DEPTH];
    for (i = 0; i < CANDL_STATEMENT_USR_INDEX_MAX_LOOP_DEPTH; ++i) {
        cur_index[i] = i;
        last[i] = 0;
    }

    /* Add useful information in the usr field of each statements */
    for (iter = scop->statement ; iter != NULL ; iter = iter->next) {
        scattering = iter->scattering;

        stmt_usr = (candl_statement_usr_p) malloc(sizeof(candl_statement_usr_t));
        stmt_usr->depth      = scattering->nb_output_dims/2;
        stmt_usr->label      = count;
        stmt_usr->type       = OSL_DEPENDENCE_ASSIGNMENT;
        stmt_usr->usr_backup = iter->usr;
        stmt_usr->index      = (stmt_usr->depth ?
                                (int*) malloc(stmt_usr->depth * sizeof(int)) :
                                NULL);

        /* Compute the value of the iterator indices.
         * extracted from the last candl
         */
        for (j = 0; j < stmt_usr->depth; ++j) {
            row = candl_util_relation_get_line(scattering, j*2);
            val = osl_int_get_si(precision,
                                 scattering->m[row][scattering->nb_columns - 1]);
            if (last[j] < val) {
                last[j] = val;
                for (k = j + 1; k < CANDL_STATEMENT_USR_INDEX_MAX_LOOP_DEPTH; ++k)
                    last[k] = 0;
                for (k = j; k < CANDL_STATEMENT_USR_INDEX_MAX_LOOP_DEPTH; ++k)
                    cur_index[k] = max + (k - j) + 1;
                break;
            }
        }
        for (j = 0; j < stmt_usr->depth; ++j)
            stmt_usr->index[j] = cur_index[j];

        if (j>0)
            max = max < cur_index[j - 1] ? cur_index[j - 1] : max;

        iter->usr = stmt_usr;
        count++;
    }
}


/**
 * candl_usr_free function:
 */
void candl_statement_usr_cleanup(osl_statement_p statement) {
    candl_statement_usr_p stmt_usr;
    stmt_usr = statement->usr;
    if (stmt_usr) {
        if (stmt_usr->index)
            free(stmt_usr->index);
        statement->usr = stmt_usr->usr_backup;
        free(stmt_usr);
    }
}

/**
 * @brief Clone the statement usr structure.
 * @param [in] Original structure.
 * @return     A freeable copy of the original structure.
 */
candl_statement_usr_p candl_statement_usr_clone(candl_statement_usr_p usr) {
    candl_statement_usr_p copy;

    CANDL_malloc(copy, candl_statement_usr_p, sizeof(candl_statement_usr_t));
    copy->label = usr->label;
    copy->depth = usr->depth;
    copy->type  = usr->type;
    copy->usr_backup = usr->usr_backup;
    CANDL_malloc(copy->index, int *, sizeof(int) * copy->depth);
    memcpy(copy->index, usr->index, sizeof(int) * copy->depth);
    return copy;
}

/**
 * @brief Find the first statement having a particular label in usr field given
 * the list of statements.  Statements must have usr field initialized with
 * candl_statement_usr instances.
 * @param statement  Head of the statement list.
 * @param label      Label to find.
 * @return           The first statement if found, NULL otherwise.
 */
osl_statement_p candl_statement_find_label(osl_statement_p statement,
        int label) {
    for ( ; statement != NULL; statement = statement->next) {
        candl_statement_usr_p stmt_usr = (candl_statement_usr_p) statement->usr;
        if (stmt_usr && stmt_usr->label == label)
            return statement;
    }
    return NULL;
}

