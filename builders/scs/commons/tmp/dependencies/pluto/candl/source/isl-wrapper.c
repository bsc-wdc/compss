
/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (                 isl-wrapper.c                           **
 **----  \#/  --------------------------------------------------------**
 **    .-"#'-.        First version: January 31st 2011                **
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
* terms of the GNU Lesser General Public License as published by the Free    *
* Software Foundation; either version 3 of the License, or (at your option)  *
* any later version.                                                         *
*                                                                            *
* This software is distributed in the hope that it will be useful, but       *
* WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY *
* or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License   *
* for more details.                                                          *
*                                                                            *
* You should have received a copy of the GNU Lesser General Public License   *
* along with software; if not, write to the Free Software Foundation, Inc.,  *
* 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA                     *
*                                                                            *
* CAnDL, the Chunky Dependence Analyzer                                      *
* Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                         *
*                                                                            *
******************************************************************************/
/**
 * \file isl-wrapper.c
 * \author Sven Verdoolaege and Louis-Noel Pouchet
 */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <osl/relation.h>

#ifdef CANDL_SUPPORTS_ISL

#undef Q
#include <isl/constraint.h>
#include <isl/map.h>
#include <isl/map.h>
#include <isl/set.h>
#include <isl/dim.h>
#include <isl/seq.h>
#include <isl/ctx.h>


/// WARNING: This is hard-coding that ISL uses GMP.


/**
 * isl_constraint_read_from_matrix:
 * Convert a single line of a matrix to a isl_constraint.
 * Returns a pointer to the constraint if successful; NULL otherwise.
 */
static
struct isl_constraint*
isl_constraint_read_from_matrix(struct isl_dim* dim, Entier* row) {
    struct isl_constraint* constraint;
    int j;
    int nvariables = isl_dim_size(dim, isl_dim_set);
    int nparam = isl_dim_size(dim, isl_dim_param);
    mpz_t val;
    mpz_init(val);

    if (CANDL_get_si(row[0]) == 0)
        constraint = isl_equality_alloc(dim);
    else
        constraint = isl_inequality_alloc(dim);

    for (j = 0; j < nvariables; ++j) {
        mpz_set_si(val, CANDL_get_si(row[1 + j]));
        isl_constraint_set_coefficient(constraint, isl_dim_out, j, val);
    }

    for (j = 0; j < nparam; ++j) {
        mpz_set_si(val, CANDL_get_si(row[1 + nvariables + j]));
        isl_constraint_set_coefficient(constraint, isl_dim_param, j, val);
    }

    mpz_set_si(val, CANDL_get_si(row[1 + nvariables + nparam]));
    isl_constraint_set_constant(constraint, val);

    mpz_clear(val);

    return constraint;
}


struct isl_set*
isl_set_from_piplib_matrix(struct isl_ctx* ctx,
                           osl_relation_p matrix,
                           int nparam) {
    PipMatrix* pmatrix = pip_relation2matrix(matrix);
    struct isl_dim* dim;
    struct isl_basic_set* bset;
    int i;
    unsigned nrows, ncolumns;

    nrows = pmatrix->NbRows;
    ncolumns = pmatrix->NbColumns;
    int nvariables = ncolumns - 2 - nparam;

    dim = isl_dim_set_alloc(ctx, nparam, nvariables);

    bset = isl_basic_set_universe(isl_dim_copy(dim));

    for (i = 0; i < nrows; ++i) {
        Entier* row = pmatrix->p[i];
        struct isl_constraint* constraint =
            isl_constraint_read_from_matrix(isl_dim_copy(dim), row);
        bset = isl_basic_set_add_constraint(bset, constraint);
    }

    isl_dim_free(dim);

    return isl_set_from_basic_set(bset);
}


static
int count_cst(__isl_take isl_constraint *c, void *user) {
    (*((int*)user))++;

    return 0;
}


static
int copy_cst_to_mat(__isl_take isl_constraint *c, void *user) {
    // 1- Get the first free row of the matrix.
    int pos;
    PipMatrix* mat = (PipMatrix*)user;
    for (pos = 0; pos < mat->NbRows &&
            CANDL_get_si(mat->p[pos][0]) != -1; ++pos)
        ;

    // 2- Set the eq/ineq bit.
    if (isl_constraint_is_equality(c))
        CANDL_set_si(mat->p[pos][0], 0);
    else
        CANDL_set_si(mat->p[pos][0], 1);

    // 3- Set all coefficients.
    isl_int val;
    isl_int_init(val);
    int j;
    int nb_vars = isl_constraint_dim(c, isl_dim_set);
    for (j = 0; j < nb_vars; ++j) {
        isl_constraint_get_coefficient(c, isl_dim_set, j, &val);
        CANDL_set_si(mat->p[pos][j + 1], isl_int_get_si(val));
    }
    int nb_param = isl_constraint_dim(c, isl_dim_param);
    for (j = 0; j < nb_param; ++j) {
        isl_constraint_get_coefficient(c, isl_dim_param, j, &val);
        CANDL_set_si(mat->p[pos][j + nb_vars + 1], isl_int_get_si(val));
    }
    isl_constraint_get_constant(c, &val);
    CANDL_set_si(mat->p[pos][mat->NbColumns - 1], isl_int_get_si(val));

    isl_int_clear(val);

    return 0;
}


int bset_get(__isl_take isl_basic_set *bset, void *user) {
    *((struct isl_basic_set**)user) = bset;

    return 0;
}


osl_relation_p
isl_set_to_piplib_matrix(struct isl_ctx* ctx,
                         struct isl_set* set,
                         int nparam) {
    struct isl_basic_set* bset = NULL;
    // There is only one basic set in this set.
    isl_set_foreach_basic_set(set, bset_get, &bset);
    if (bset == NULL)
        return NULL;

    // 1- Count the number of eq/ineq.
    int count = 0;
    isl_basic_set_foreach_constraint(bset, count_cst, &count);

    // 2- Count the dimensions.
    int nb_vars = isl_basic_set_dim(bset, isl_dim_set);

    // 3- Allocate output matrix, and prepare it.
    PipMatrix* res = pip_matrix_alloc(count, nb_vars + nparam + 2);
    int i;
    for (i = 0; i < count; ++i)
        CANDL_set_si(res->p[i][0], -1);

    // 4- Convert each constraint to a row of the matrix.
    isl_basic_set_foreach_constraint(bset, copy_cst_to_mat, res);

    osl_relation_p tmp = pip_matrix2relation(res);
    pip_matrix_free(res);

    return tmp;
}


#endif
