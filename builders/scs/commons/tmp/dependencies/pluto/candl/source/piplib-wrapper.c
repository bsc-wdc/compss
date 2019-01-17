
/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (               piplib-wrapper.c                          **
 **----  \#/  --------------------------------------------------------**
 **    .-"#'-.        First version: January 31st 2012                **
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
 * \file piplib-wrapper.c
 * \author Louis-Noel Pouchet
 */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <candl/candl.h>
#include <osl/macros.h> /* Need OSL_PRECISION for compatibility with piplib */
#include <osl/relation.h>
#include <candl/macros.h>
#include <candl/piplib.h>


/**
 * pip_relation2matrix function :
 * This function is used to keep the compatibility with Piplib
 */
PipMatrix* pip_relation2matrix(osl_relation_p in) {
    int i, j, precision;
    PipMatrix *out;

    if (in == NULL)
        return NULL;

#ifdef CANDL_LINEAR_VALUE_IS_INT
    precision = OSL_PRECISION_SP;
#elif defined(CANDL_LINEAR_VALUE_IS_LONGLONG)
    precision = OSL_PRECISION_DP;
#elif defined(CANDL_LINEAR_VALUE_IS_MP)
    precision = OSL_PRECISION_MP;
#endif

    if (precision != in->precision)
        CANDL_error("Precision not compatible with piplib ! (pip_relation2matrix)");

    out = pip_matrix_alloc(in->nb_rows, in->nb_columns);

    for (i = 0 ; i < in->nb_rows ; i++) {
        for (j = 0 ; j < in->nb_columns ; j++) {
#if defined(CANDL_LINEAR_VALUE_IS_INT)
            CANDL_assign(out->p[i][j], in->m[i][j].sp);
#elif defined(CANDL_LINEAR_VALUE_IS_LONGLONG)
            CANDL_assign(out->p[i][j], in->m[i][j].dp);
#elif defined(CANDL_LINEAR_VALUE_IS_MP)
            CANDL_assign(out->p[i][j], *((mpz_t*)in->m[i][j].mp));
#endif
        }
    }

    return out;
}


/**
 * pip_matrix2relation function :
 * This function is used to keep the compatibility with Piplib
 */
osl_relation_p pip_matrix2relation(PipMatrix* in) {
    int i, j, precision;
    osl_relation_p out;
    osl_int_t temp;

    if (in == NULL)
        return NULL;

#if defined(CANDL_LINEAR_VALUE_IS_INT)
    precision = OSL_PRECISION_SP;
#elif defined(CANDL_LINEAR_VALUE_IS_LONGLONG)
    precision = OSL_PRECISION_DP;
#elif defined(CANDL_LINEAR_VALUE_IS_MP)
    precision = OSL_PRECISION_MP;
#endif

    out = osl_relation_pmalloc(precision, in->NbRows, in->NbColumns);
    osl_int_init(precision, &temp);

    for (i = 0 ; i < in->NbRows ; i++) {
        for (j = 0 ; j < in->NbColumns ; j++) {
#ifdef CANDL_LINEAR_VALUE_IS_INT
            temp.sp = in->p[i][j];
#elif defined(CANDL_LINEAR_VALUE_IS_LONGLONG)
            temp.dp = in->p[i][j];
#elif defined(CANDL_LINEAR_VALUE_IS_MP)
            mpz_set(*((mpz_t*)temp.mp), in->p[i][j]);
#endif
            osl_int_assign(precision, &out->m[i][j], temp);
        }
    }

    osl_int_clear(precision, &temp);
    return out;
}

int pip_has_rational_point(osl_relation_p system,
                           osl_relation_p context,
                           int conservative) {
// FIXME : compatibility with osl
//#ifdef CANDL_HAS_PIPLIB_HYBRID
//  return piplib_hybrid_has_rational_point(system, context, conservative);
//#else
    PipOptions* options;
    int ret = 0;
    options = pip_options_init ();
    options->Simplify = 1;
    options->Urs_parms = -1;
    options->Urs_unknowns = -1;
    options->Nq = 0;
    PipQuast* solution = pip_solve_osl(system, context, -1, options);
    if ((solution != NULL) &&
            ((solution->list != NULL) || (solution->condition != NULL)))
        ret = 1;
    pip_options_free(options);
    pip_quast_free(solution);
    return ret;
//#endif
}


/**
 * pip_solve_osl function :
 * A pip_solve with osl_relation_p instead of PipMatrix
 */
PipQuast* pip_solve_osl(osl_relation_p inequnk, osl_relation_p ineqpar,
                        int Bg, PipOptions *options) {
    PipMatrix *pip_unk  = pip_relation2matrix(inequnk);
    PipMatrix *pip_par  = pip_relation2matrix(ineqpar);
    PipQuast  *solution = pip_solve(pip_unk, pip_par, Bg, options);
    if (pip_unk) pip_matrix_free(pip_unk);
    if (pip_par) pip_matrix_free(pip_par);
    return solution;
}


/**
 * Return true if the 'size' first elements of 'l1' and 'l2' are equal.
 * if size is negative, go through all elements (but stop at the firsts equal).
 */
int piplist_are_equal(PipList* l1, PipList* l2, int size) {
    if (l1 == NULL && l2 == NULL)
        return 1;
    if (l1 == NULL || l2 == NULL)
        return 0;
    if (l1->vector == NULL && l2->vector == NULL)
        return 1;
    if (l1->vector == NULL || l2->vector == NULL)
        return 0;

    int count = 0;
    while(l1 && l2 && ((size < 0) || (count < size))) {
        if (l1->vector == NULL && l2->vector == NULL)
            return 1;
        if (l1->vector == NULL || l2->vector == NULL)
            return 0;
        if (l1->vector->nb_elements != l2->vector->nb_elements)
            return 0;
        int j;
        for (j = 0; j < l1->vector->nb_elements; ++j)
            if (! CANDL_eq(l1->vector->the_vector[j],
                           l2->vector->the_vector[j]) ||
                    ! CANDL_eq(l1->vector->the_deno[j],
                               l2->vector->the_deno[j]))
                return 0;
        l1 = l1->next;
        l2 = l2->next;
        count++;
    }

    return 1;
}

/**
 * Return true if the 'size' first variables in a quast are strictly
 * equal.
 * if 'size' is negative, ALL the variables in a quast need to be strictly equal.
 */
int quast_are_equal (PipQuast* q1, PipQuast* q2, int size) {
    if (q1 == NULL && q2 == NULL)
        return 1;
    if (q1 == NULL || q2 == NULL)
        return 0;

    // Inspect conditions.
    if (q1->condition != NULL && q2->condition != NULL) {
        PipList c1;
        c1.next = NULL;
        c1.vector = q1->condition;
        PipList c2;
        c2.next = NULL;
        c2.vector = q2->condition;
        if (! piplist_are_equal(&c1, &c2, size))
            return 0;
        return quast_are_equal(q1->next_then, q2->next_then, size) &&
               quast_are_equal(q1->next_else, q2->next_else, size);
    }
    if (q1->condition != NULL || q2->condition != NULL)
        return 0;
    return piplist_are_equal(q1->list, q2->list, size);
}


/*
 * Converts all conditions where the path does not lead to a solution
 * The return is a upip_quast_to_polyhedranion of polyhedra
 * extracted from pip_quast_to_polyhedra
 */
osl_relation_p pip_quast_no_solution_to_polyhedra(PipQuast *quast, int nvar,
        int npar) {
    osl_relation_p ep;
    osl_relation_p tp;
    osl_relation_p qp;
    osl_relation_p iter;
    int precision;
    int j;
    if (quast == NULL)
        return NULL;

#if defined(CANDL_LINEAR_VALUE_IS_INT)
    precision = OSL_PRECISION_SP;
#elif defined(CANDL_LINEAR_VALUE_IS_LONGLONG)
    precision = OSL_PRECISION_DP;
#elif defined(CANDL_LINEAR_VALUE_IS_MP)
    precision = OSL_PRECISION_MP;
#endif

    if (quast->condition != NULL) {
        tp = pip_quast_no_solution_to_polyhedra(quast->next_then, nvar, npar);
        ep = pip_quast_no_solution_to_polyhedra(quast->next_else, nvar, npar);

        /* Each of the matrices in the then tree needs to be augmented with
         * the condition */
        for (iter = tp ; iter != NULL ; iter = iter->next) {
            int nrows = iter->nb_rows;
            osl_int_set_si(precision, &iter->m[nrows][0], 1);
            for (j = 1; j < 1 + nvar; j++)
                osl_int_set_si(precision, &iter->m[nrows][j], 0);
            for (j = 0; j < npar + 1; j++)
                osl_int_set_si(precision, &iter->m[nrows][1 + nvar + j],
                               CANDL_get_si(quast->condition->the_vector[j]));
            (iter->nb_rows)++;
        }

        for (iter = ep; iter != NULL ; iter = iter->next) {
            int nrows = iter->nb_rows;
            /* Inequality */
            osl_int_set_si(precision, &iter->m[nrows][0], 1);
            for (j = 1; j < 1 + nvar; j++)
                osl_int_set_si(precision, &iter->m[nrows][j], 0);
            for (j = 0; j < npar + 1; j++)
                osl_int_set_si(precision, &iter->m[nrows][1 + nvar + j],
                               -CANDL_get_si(quast->condition->the_vector[j]));
            osl_int_decrement(precision,
                              &iter->m[nrows][iter->nb_columns - 1],
                              iter->m[nrows][iter->nb_columns - 1]);
            (iter->nb_rows)++;
        }

        /* union of tp and ep */
        if (tp) {
            qp = tp;
            for (iter = tp ; iter->next != NULL ; iter = iter->next)
                ;
            iter->next = ep;
        } else {
            qp = ep;
        }

        return qp;

    }

    if (quast->list != NULL)
        return NULL;

    /* quast condition is NULL */
    osl_relation_p lwmatrix = osl_relation_pmalloc(precision, nvar+npar+1,
                              nvar+npar+2);
    lwmatrix->nb_rows = 0;
    lwmatrix->nb_parameters = npar;

    return lwmatrix;
}


/*
 * Converts a PIP quast to a union of polyhedra
 */
osl_relation_p pip_quast_to_polyhedra(PipQuast *quast, int nvar, int npar) {
    // originaly used for lastwriter
    // july 5th 2012 : extracted from dependence.c

    osl_relation_p ep;
    osl_relation_p tp;
    osl_relation_p qp;
    osl_relation_p iter;
    int precision;
    int j;
    if (quast == NULL)
        return NULL;

#if defined(CANDL_LINEAR_VALUE_IS_INT)
    precision = OSL_PRECISION_SP;
#elif defined(CANDL_LINEAR_VALUE_IS_LONGLONG)
    precision = OSL_PRECISION_DP;
#elif defined(CANDL_LINEAR_VALUE_IS_MP)
    precision = OSL_PRECISION_MP;
#endif

    if (quast->condition != NULL) {
        tp = pip_quast_to_polyhedra(quast->next_then, nvar, npar);
        ep = pip_quast_to_polyhedra(quast->next_else, nvar, npar);

        /* Each of the matrices in the then tree needs to be augmented with
         * the condition */
        for (iter = tp ; iter != NULL ; iter = iter->next) {
            int nrows = iter->nb_rows;
            osl_int_set_si(precision, &iter->m[nrows][0], 1);
            for (j = 1; j < 1 + nvar; j++)
                osl_int_set_si(precision, &iter->m[nrows][j], 0);
            for (j = 0; j < npar + 1; j++)
                osl_int_set_si(precision, &iter->m[nrows][1 + nvar + j],
                               CANDL_get_si(quast->condition->the_vector[j]));
            (iter->nb_rows)++;
        }

        /* JP : july 5th 2012:
         * Fix negation of a constraint in adding -1 to the constant
         */

        for (iter = ep; iter != NULL ; iter = iter->next) {
            int nrows = iter->nb_rows;
            /* Inequality */
            osl_int_set_si(precision, &iter->m[nrows][0], 5);
            for (j = 1; j < 1 + nvar; j++)
                osl_int_set_si(precision, &iter->m[nrows][j], 0);
            for (j = 0; j < npar + 1; j++)
                osl_int_set_si(precision, &iter->m[nrows][1 + nvar + j],
                               -CANDL_get_si(quast->condition->the_vector[j]));
            osl_int_decrement(precision,
                              &iter->m[nrows][iter->nb_columns - 1],
                              iter->m[nrows][iter->nb_columns - 1]);
            (iter->nb_rows)++;
        }

        /* union of tp and ep */
        if (tp) {
            qp = tp;
            for (iter = tp ; iter->next != NULL ; iter = iter->next)
                ;
            iter->next = ep;
        } else {
            qp = ep;
        }

        return qp;

    } else {
        /* quast condition is NULL */
        osl_relation_p lwmatrix = osl_relation_pmalloc(precision, nvar+npar+1,
                                  nvar+npar+2);
        PipList *vecList = quast->list;

        int count=0;
        while (vecList != NULL) {
            /* Equality */
            osl_int_set_si(precision, &lwmatrix->m[count][0], 0);
            for (j=0; j < nvar; j++)
                if (j == count)
                    osl_int_set_si(precision, &lwmatrix->m[count][j + 1], 1);
                else
                    osl_int_set_si(precision, &lwmatrix->m[count][j + 1], 0);

            for (j=0; j < npar; j++)
                osl_int_set_si(precision, &lwmatrix->m[count][j + 1 + nvar],
                               -CANDL_get_si(vecList->vector->the_vector[j]));
            /* Constant portion */
            if (quast->newparm != NULL)
                /* Don't handle newparm for now */
                osl_int_set_si(precision, &lwmatrix->m[count][npar + 1 + nvar],
                               -CANDL_get_si(vecList->vector->the_vector[npar+1]));
            else
                osl_int_set_si(precision, &lwmatrix->m[count][npar + 1 + nvar],
                               -CANDL_get_si(vecList->vector->the_vector[npar]));

            count++;

            vecList = vecList->next;
        }
        lwmatrix->nb_rows = count;
        lwmatrix->nb_parameters = npar;

        return lwmatrix;
    }
}
