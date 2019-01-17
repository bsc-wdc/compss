
/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (                     ddv.c                               **
 **----  \#/  --------------------------------------------------------**
 **    .-"#'-.         First version: February 4th 2010               **
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
 * \file ddv.c
 * \author Louis-Noel Pouchet
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <osl/statement.h>
#include <osl/scop.h>
#include <osl/extensions/dependence.h>
#include <candl/macros.h>
#include <candl/ddv.h>
#include <candl/statement.h>
#include <candl/piplib.h>
#include <candl/piplib-wrapper.h>

#ifndef min
# define min(x,y) (x < y ? x : y)
#endif
#ifndef max
# define max(x,y) (x > y ? x : y)
#endif


/******************************************************************************
 *                         Memory deallocation function                       *
 ******************************************************************************/

/**
 * candl_ddv_malloc: Allocate an empty ddv.
 *
 *
 */
CandlDDV*
candl_ddv_malloc() {
    CandlDDV* dv = (CandlDDV*) malloc(sizeof(CandlDDV));
    dv->next = NULL;
    dv->data = NULL;
    dv->length = 0;

    return dv;
}


/**
 * candl_ddv_alloc: Allocate a ddv for a loop of depth 'size'.
 *
 *
 */
CandlDDV*
candl_ddv_alloc(int size) {
    CandlDDV* dv = candl_ddv_malloc();

    dv->data = (s_dv_descriptor*) malloc(size * sizeof(s_dv_descriptor));
    dv->length = size;

    int i;
    for (i = 0; i < size; ++i) {
        dv->data[i].type = candl_dv_star;
        dv->data[i].value = 0;
    }

    return dv;
}


/**
 * candl_ddv_free: Free a ddv.
 *
 *
 */
void
candl_ddv_free(CandlDDV* dv) {
    CandlDDV* tmp;
    CandlDDV* next;

    for (tmp = dv; tmp; ) {
        next = tmp->next;
        if (tmp->data)
            free(tmp->data);
        free(tmp);
        tmp = next;
    }

}


/**
 * candl_ddv_set_type_at: Set the type of a ddv component. Type is one of
 * '=', '>', '<', '*' or 'constant' as defined by the enum e_dv_type.
 *
 *
 */
void
candl_ddv_set_type_at(CandlDDV* dv, e_dv_type type, int pos) {
    dv->data[pos].type = type;
}

/**
 * candl_ddv_set_value_at: Set the scalar value of a ddv
 * component. This is meaningful only if the type of this component is
 * 'constant'.
 *
 *
 */
void
candl_ddv_set_value_at(CandlDDV* dv, int value, int pos) {
    dv->data[pos].value = value;
}

/**
 * candl_ddv_set_type: Set the type of the dependence in
 * CANDL_UNDEFINED, CANDL_RAW, CANDL_WAR, CANDL_WAW, CANDL_RAR.
 *
 */
void
candl_ddv_set_deptype(CandlDDV* dv, int type) {
    dv->deptype = type;
}


/******************************************************************************
 *                          Structure display function                        *
 ******************************************************************************/


/**
 * candl_ddv_print: print a ddv.
 *
 */
void
candl_ddv_print(FILE* out, CandlDDV* dv) {
    if (!dv) {
        fprintf(out, "(null ddv)\n");
        return;
    }

    int i;
    fprintf(out, "loop_id=%d, (", dv->loop_id);
    for (i = 0; i < dv->length; ++i) {
        if (dv->data[i].type == candl_dv_star)
            fprintf(out, "*");
        else if (dv->data[i].type == candl_dv_eq)
            fprintf(out, "=");
        else if (dv->data[i].type == candl_dv_plus)
            fprintf(out, ">");
        else if (dv->data[i].type == candl_dv_minus)
            fprintf(out, "<");
        else if (dv->data[i].type == candl_dv_scalar)
            fprintf(out, "%d", dv->data[i].value);
        if (i < dv->length - 1)
            fprintf(out, ", ");
        else
            fprintf(out, ")\n");
    }
}


/******************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/


/**
 * Recursively count the number of leaves in a QUAST.
 *
 */
static
int count_quast_leaves(PipQuast* q) {
    if (!q)
        return 0;
    if (q->condition == NULL)
        return 1;
    else
        return count_quast_leaves(q->next_else) + count_quast_leaves(q->next_then);
}


/**
 * Recursively traverse a QUAST, and put all leaves into a flat array.
 *
 */
static
void get_quast_leaves(PipQuast* q, PipList** leaves) {
    if (!q)
        return;
    if (q->condition == NULL) {
        int i;
        for (i = 0; leaves[i]; ++i)
            ;
        leaves[i] = q->list;
    } else {
        get_quast_leaves(q->next_else, leaves);
        get_quast_leaves(q->next_then, leaves);
    }
}


/**
 * candl_ddv_constant_val: returns true iff all possible values of the
 * minimization of the first variable of the system is a scalar constant
 * (not parametric), and has the same value for all conditions of the QUAST.
 * The scalar constant is put in the 'val' argument.
 *
 */
static
int
candl_ddv_constant_val(osl_relation_p system, int* val, int nb_par) {
    PipOptions * options;
    PipQuast * solution;
    osl_relation_p context;
    int is_constant_val = 1;
    int cst;
    int cst_base = 42;
    int first = 1;
    int i, j;
    int precision = system->precision;

    // 1- Comute the lexmin of the system, to get a QUAST.
    options = pip_options_init();
    options->Simplify = 1;
    options->Urs_parms = -1;
    options->Urs_unknowns = -1;
    options->Nq = 0;

    context = osl_relation_pmalloc(precision, 0, nb_par + 2);
    solution = pip_solve_osl(system, context, -1, options);

    if ((solution != NULL) &&
            ((solution->list != NULL) || (solution->condition != NULL))) {
        // 2- Traverse all leaves, ensure they have the same value.
        int nb_leaves = count_quast_leaves(solution);
        PipList* leaveslist[nb_leaves + 1];
        for (i = 0; i < nb_leaves + 1; ++i)
            leaveslist[i] = NULL;
        get_quast_leaves(solution, leaveslist);

        for (i = 0; i < nb_leaves; ++i) {
            PipList* list = leaveslist[i];
            if (list && list->vector) {
                PipVector* vect = list->vector;

                // FIXME : check if precision is correct to use piplib

                for (j = 0; j < nb_par; ++j)
                    if (!CANDL_zero_p(vect->the_vector[j])) {
                        is_constant_val = 0;
                        break;
                    }
                if (is_constant_val) {
                    cst = CANDL_get_si(vect->the_vector[vect->nb_elements-1]);
                    if (first) {
                        first = 0;
                        cst_base = cst;
                    } else if (! first && cst != cst_base) {
                        is_constant_val = 0;
                        break;
                    }
                } else
                    break;
            }
        }
    }

    pip_quast_free(solution);
    pip_options_free(options);
    osl_relation_free(context);

    if (is_constant_val)
        *val = cst_base;

    return is_constant_val;
}


/**
 * Creates a ddv of size equal to the maximal loop depth of the source
 * and target of the statement, according to the dependence polyhedron.
 *
 */
static
CandlDDV*
candl_ddv_create_from_dep(osl_dependence_p dep, int loop_id, int ddv_size) {
    osl_relation_p mat = dep->domain;
    osl_statement_p src = dep->stmt_source_ptr;
    candl_statement_usr_p usr = src->usr;
    CandlDDV* dv = candl_ddv_alloc(ddv_size);
    int precision = src->domain->precision;
    int i, j;
    int pos;

    dv->loop_id = loop_id;
    dv->deptype = dep->type;

    // Create the template of the system to operate on.
    // Add one dimension at the beginning, and one row for the extra constraint.
    int nb_par = src->domain->nb_parameters;
    osl_relation_p testsyst = osl_relation_pmalloc(precision,
                              mat->nb_rows + 1,
                              mat->nb_columns + 1);
    for (pos = 0; pos < mat->nb_rows; ++pos) {
        osl_int_assign(precision, &testsyst->m[pos][0], mat->m[pos][0]);
        for (j = 1; j < mat->nb_columns; ++j)
            osl_int_assign(precision, &testsyst->m[pos][j + 1],mat->m[pos][j]);
    }

    int has_eq, has_pos, has_neg, has_cst;
    int scal1, scal2;
    for (i = 1; i <= ddv_size; ++i) {
        // Test for '='.
        osl_int_set_si(precision, &testsyst->m[pos][0], 0);
        osl_int_set_si(precision, &testsyst->m[pos][1 + i], 1);
        osl_int_set_si(precision, &testsyst->m[pos][1 + i + usr->depth], -1);
        osl_int_set_si(precision, &testsyst->m[pos][testsyst->nb_columns-1], 0);
        has_eq = pip_has_rational_point(testsyst, NULL, 1);

        // Test for '>'.
        osl_int_set_si(precision, &testsyst->m[pos][0], 1);
        osl_int_set_si(precision, &testsyst->m[pos][1 + i], 1);
        osl_int_set_si(precision, &testsyst->m[pos][1 + i + usr->depth], -1);
        osl_int_set_si(precision, &testsyst->m[pos][testsyst->nb_columns-1], -1);
        has_pos = pip_has_rational_point(testsyst, NULL, 1);

        // Test for '<'.
        osl_int_set_si(precision, &testsyst->m[pos][0], 1);
        osl_int_set_si(precision, &testsyst->m[pos][1 + i], -1);
        osl_int_set_si(precision, &testsyst->m[pos][1 + i + usr->depth], 1);
        osl_int_set_si(precision, &testsyst->m[pos][testsyst->nb_columns-1], -1);
        has_neg = pip_has_rational_point(testsyst, NULL, 1);

        // Test for constant distance.
        // min(x_R^i - x_S^i)
        // max(x_R^i - x_S^i) = - min(- x_R^i + x_S^i)
        osl_int_set_si(precision, &testsyst->m[pos][0], 0);
        osl_int_set_si(precision, &testsyst->m[pos][1], 1);
        osl_int_set_si(precision, &testsyst->m[pos][1 + i], -1);
        osl_int_set_si(precision, &testsyst->m[pos][1 + i + usr->depth], 1);
        osl_int_set_si(precision, &testsyst->m[pos][testsyst->nb_columns-1], 0);
        has_cst = candl_ddv_constant_val(testsyst, &scal1, nb_par);

        if (has_cst) {
            osl_int_set_si(precision, &testsyst->m[pos][1 + i], 1);
            osl_int_set_si(precision, &testsyst->m[pos][1 + i + usr->depth], -1);
            osl_int_set_si(precision, &testsyst->m[pos][1], 1);
            has_cst = candl_ddv_constant_val(testsyst, &scal2, nb_par);
            scal2 *= -1;
            if (has_cst)
                has_cst = (scal1 == scal2);
        }

        if (has_cst && scal1 != 0) {
            candl_ddv_set_type_at(dv, candl_dv_scalar, i - 1);
            candl_ddv_set_value_at(dv, scal1, i - 1);
        } else if (has_pos && has_neg)
            candl_ddv_set_type_at(dv, candl_dv_star, i - 1);
        else if (has_pos)
            candl_ddv_set_type_at(dv, candl_dv_plus, i - 1);
        else if (has_neg)
            candl_ddv_set_type_at(dv, candl_dv_minus, i - 1);
        else if (has_eq) {
            candl_ddv_set_type_at(dv, candl_dv_eq, i - 1);
            candl_ddv_set_value_at(dv, 0, i - 1);
        }

        // Reset the constraint.
        osl_int_set_si(precision, &testsyst->m[pos][0], 0);
        osl_int_set_si(precision, &testsyst->m[pos][1], 0);
        osl_int_set_si(precision, &testsyst->m[pos][1 + i], 0);
        osl_int_set_si(precision, &testsyst->m[pos][1 + i + usr->depth], 0);
        osl_int_set_si(precision, &testsyst->m[pos][testsyst->nb_columns-1], 0);
    }

    return dv;
}


/**
 * candl_ddv_extract_in_loop: Create a chained list of dependence distance
 * vectors, from the list of polyhedral dependences. Only dependences
 * involving statements surrounded by loop 'loop_id' are considered. One
 * ddv is generated per polyhedral dependence.
 *
 */
CandlDDV*
candl_ddv_extract_in_loop(osl_scop_p scop, osl_dependence_p deps,
                          int loop_id) {
    osl_dependence_p tmp;
    candl_statement_usr_p src_usr, dst_usr;
    CandlDDV* head = NULL;
    CandlDDV* last = NULL;
    int i;

    for (tmp = deps; tmp; tmp = tmp->next) {
        osl_statement_p src = tmp->stmt_source_ptr;
        osl_statement_p dst = tmp->stmt_target_ptr;
        src_usr = src->usr;
        dst_usr = dst->usr;

        // Iterate on all dependences that lie within the given loop.
        for (i = 0; i < min(src_usr->depth, dst_usr->depth); ++i)
            if (src_usr->index[i] == dst_usr->index[i] &&
                    dst_usr->index[i] == loop_id)
                break;
        if (i == min(src_usr->depth, dst_usr->depth))
            continue;
        // The dependence involves statement carried by the loop.
        // Convert it into dependence vector.
        CandlDDV* dv = candl_ddv_create_from_dep(tmp, loop_id, i + 1);

        // Append the dependence vector to the list.
        if (head == NULL)
            head = last = dv;
        else {
            last->next = dv;
            last = dv;
        }
    }

    return head;
}



/**
 * candl_loops_are_permutable: output 1 if the 2 loops are permutable.
 * Assume loop1 and loop2 are nested.
 *
 */
int
candl_loops_are_permutable(osl_scop_p scop, osl_dependence_p deps,
                           int loop_id1, int loop_id2) {
    CandlDDV* l1 = candl_ddv_extract_in_loop(scop, deps, loop_id1);
    CandlDDV* l2 = candl_ddv_extract_in_loop(scop, deps, loop_id2);

    // One of the loop do not carry any dependence.
    if (l1 == NULL || l2 == NULL)
        return 1;

    CandlDDV* dv;
    if (l1->length > l2->length)
        dv = l1;
    else
        dv = l2;

    int loop_dim_1 = -1;
    int loop_dim_2 = -1;
    int i;
    candl_statement_usr_p usr;
    osl_statement_p stm = scop->statement;
    for (; stm != NULL; stm = stm->next) {
        usr = stm->usr;
        for (i = 0; i < usr->depth; ++i)
            if (usr->index[i] == loop_id1)
                loop_dim_1 = i;
            else if (usr->index[i] == loop_id2)
                loop_dim_2 = i;
        if (loop_dim_1 != -1 && loop_dim_2 != -1)
            break;
    }

    int is_pos_1 = 0;
    int is_neg_1 = 0;
    int is_pos_2 = 0;
    int is_neg_2 = 0;
    int is_star = 0;
    for (; dv; dv = dv->next) {
        switch (dv->data[loop_dim_1].type) {
        case candl_dv_plus:
            is_pos_1 = 1;
            break;
        case candl_dv_minus:
            is_neg_1 = 1;
            break;
        case candl_dv_scalar:
            if (dv->data[loop_dim_1].value > 0)
                is_pos_1 = 1;
            else if (dv->data[loop_dim_1].value < 0)
                is_neg_1 = 1;
            break;
        case candl_dv_star:
            is_star = 1;
            break;
        default:
            break;
        }
        switch (dv->data[loop_dim_2].type) {
        case candl_dv_plus:
            is_pos_2 = 1;
            break;
        case candl_dv_minus:
            is_neg_2 = 1;
            break;
        case candl_dv_scalar:
            if (dv->data[loop_dim_2].value > 0)
                is_pos_2 = 1;
            else if (dv->data[loop_dim_2].value < 0)
                is_neg_2 = 1;
            break;
        case candl_dv_star:
            is_star = 1;
            break;
        default:
            break;
        }
        if ((is_pos_1 && is_neg_1) || (is_pos_2 && is_neg_2) || is_star) {
            candl_ddv_free(l1);
            candl_ddv_free(l2);
            return 0;
        }
    }

    candl_ddv_free(l1);
    candl_ddv_free(l2);


    return ! ((is_pos_1 && is_neg_2) || (is_neg_1 && is_pos_2));
}

