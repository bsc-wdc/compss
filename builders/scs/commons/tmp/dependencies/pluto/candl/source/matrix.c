
/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (                    matrix.c                             **
 **----  \#/  --------------------------------------------------------**
 **    .-"#'-.        First version: december 9th 2005                **
 **--- |"-.-"| -------------------------------------------------------**
 |     |
 |     |
******** |     | *************************************************************
* CAnDL  '-._,-' the Chunky Analyzer for Dependences in Loops (experimental) *
******************************************************************************
*                                                                            *
* Copyright (C) 2005 Cedric Bastoul                                          *
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
/* CAUTION: the english used for comments is probably the worst you ever read,
 *          please feel free to correct and improve it !
 */

#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <osl/macros.h>
#include <osl/relation.h>
#include <osl/extensions/dependence.h>
#include <candl/macros.h>
#include <candl/matrix.h>
#include <candl/violation.h>
#include <candl/piplib.h>
#include <candl/piplib-wrapper.h>


/**
 * candl_matrix_violation function :
 * this function builds the constraint system corresponding to a violation of a
 * dependence, for a given transformation couple at a given depth.
 * - dependence is the constraint system of a dependence between two
 statements,
 * - t_source is the transformation function for the source statement,
 * - t_target is the transformation function for the target statement,
 * - dimension is the transformation dimension checked for legality,
 * - nb_par is the number of parameters.
 ***
 * - 13/12/2005: first version (extracted from candl_violation).
 */
candl_violation_p candl_matrix_violation(osl_dependence_p dependence,
        osl_relation_p source,
        osl_relation_p target,
        int dimension, int nb_par) {
    candl_violation_p violation;
    osl_relation_p system;
    int i, j, k, c;
    int constraint = 0;
    int precision = dependence->domain->precision;
    int nb_rows, nb_columns;
    int nb_output_dims, nb_input_dims, nb_local_dims;
    int ind_source_output_scatt;
    int ind_target_output_scatt;
    int ind_source_local_scatt;
    int ind_target_local_scatt;
    int ind_params;

    /* Create a new violation structure */
    violation = candl_violation_malloc();

    violation->source_nb_output_dims_scattering = source->nb_output_dims;
    violation->target_nb_output_dims_scattering = target->nb_output_dims;
    violation->source_nb_local_dims_scattering  = source->nb_local_dims;
    violation->target_nb_local_dims_scattering  = target->nb_local_dims;

    /* Compute the system size */
    nb_local_dims  = dependence->domain->nb_local_dims +
                     violation->source_nb_local_dims_scattering +
                     violation->target_nb_local_dims_scattering;
    nb_output_dims = dependence->domain->nb_output_dims +
                     violation->source_nb_output_dims_scattering;
    nb_input_dims  = dependence->domain->nb_input_dims +
                     violation->target_nb_output_dims_scattering;

    nb_columns = nb_output_dims + nb_input_dims + nb_local_dims + nb_par + 2;
    nb_rows    = dependence->domain->nb_rows +
                 source->nb_rows + target->nb_rows +
                 dimension;

    system = osl_relation_pmalloc(precision, nb_rows, nb_columns);

    /* Compute some indexes */
    ind_source_output_scatt = 1 + dependence->domain->nb_output_dims;
    ind_target_output_scatt = ind_source_output_scatt + source->nb_output_dims +
                              dependence->domain->nb_input_dims;
    ind_source_local_scatt  = ind_target_output_scatt + target->nb_output_dims +
                              dependence->domain->nb_local_dims;
    ind_target_local_scatt  = ind_source_local_scatt + source->nb_local_dims +
                              dependence->domain->nb_local_dims;
    ind_params              = ind_target_local_scatt + target->nb_local_dims;

    /* 1. Copy the dependence domain */
    for (i = 0 ; i < dependence->domain->nb_rows ; i++) {
        /* eq/in */
        osl_int_assign(precision,
                       &system->m[constraint][0],
                       dependence->domain->m[i][0]);
        /* output dims */
        k = 1;
        j = 1;
        for (c = dependence->domain->nb_output_dims ; c > 0 ; c--, k++, j++)
            osl_int_assign(precision,
                           &system->m[constraint][k],
                           dependence->domain->m[i][j]);
        /* input dims */
        k += source->nb_output_dims;
        for (c = dependence->domain->nb_input_dims ; c > 0 ; c--, k++, j++)
            osl_int_assign(precision,
                           &system->m[constraint][k],
                           dependence->domain->m[i][j]);
        /* source local dims */
        k += target->nb_output_dims;
        for (c = dependence->source_nb_local_dims_domain +
                 dependence->source_nb_local_dims_access ; c > 0 ; c--, k++, j++)
            osl_int_assign(precision,
                           &system->m[constraint][k],
                           dependence->domain->m[i][j]);
        /* target local dims */
        k += source->nb_local_dims;
        for (c = dependence->target_nb_local_dims_domain +
                 dependence->target_nb_local_dims_access ; c > 0 ; c--, k++, j++)
            osl_int_assign(precision,
                           &system->m[constraint][k],
                           dependence->domain->m[i][j]);
        /* params + const */
        k = ind_params;
        for (c = nb_par+1 ; c > 0 ; c--, k++, j++)
            osl_int_assign(precision,
                           &system->m[constraint][k],
                           dependence->domain->m[i][j]);
        constraint++;
    }

    /* 2. Copy the source scattering */
    for (i = 0 ; i < source->nb_rows ; i++) {
        /* eq/in */
        osl_int_assign(precision,
                       &system->m[constraint][0],
                       source->m[i][0]);
        /* output dims */
        k = ind_source_output_scatt;
        j = 1;
        for (c = source->nb_output_dims ; c > 0 ; c--, k++, j++)
            osl_int_assign(precision,
                           &system->m[constraint][k],
                           source->m[i][j]);
        /* input dims (linked with the output dims of domain) */
        k = 1;
        for (c = source->nb_input_dims ; c > 0 ; c--, k++, j++)
            osl_int_assign(precision,
                           &system->m[constraint][k],
                           source->m[i][j]);
        /* local dims */
        k = ind_source_local_scatt;
        for (c = source->nb_local_dims ; c > 0 ; c--, k++, j++)
            osl_int_assign(precision,
                           &system->m[constraint][k],
                           source->m[i][j]);
        /* params + const */
        k = ind_params;
        for (c = nb_par+1 ; c > 0 ; c--, k++, j++)
            osl_int_assign(precision,
                           &system->m[constraint][k],
                           source->m[i][j]);
        constraint++;
    }

    /* 2. Copy the target scattering */
    for (i = 0 ; i < target->nb_rows ; i++) {
        /* eq/in */
        osl_int_assign(precision,
                       &system->m[constraint][0],
                       target->m[i][0]);
        /* output dims */
        k = ind_target_output_scatt;
        j = 1;
        for (c = target->nb_output_dims ; c > 0 ; c--, k++, j++) {
            osl_int_assign(precision,
                           &system->m[constraint][k],
                           target->m[i][j]);
            osl_int_oppose(precision,
                           &system->m[constraint][k],
                           system->m[constraint][k]);
        }
        /* input dims (linked with the output dims of domain) */
        k = 1 + nb_output_dims;
        for (c = target->nb_input_dims ; c > 0 ; c--, k++, j++) {
            osl_int_assign(precision,
                           &system->m[constraint][k],
                           target->m[i][j]);
            osl_int_oppose(precision,
                           &system->m[constraint][k],
                           system->m[constraint][k]);
        }
        /* local dims */
        k = ind_target_local_scatt;
        for (c = target->nb_local_dims ; c > 0 ; c--, k++, j++) {
            osl_int_assign(precision,
                           &system->m[constraint][k],
                           target->m[i][j]);
            osl_int_oppose(precision,
                           &system->m[constraint][k],
                           system->m[constraint][k]);
        }
        /* params + const */
        k = ind_params;
        for (c = nb_par+1 ; c > 0 ; c--, k++, j++) {
            osl_int_assign(precision,
                           &system->m[constraint][k],
                           target->m[i][j]);
            osl_int_oppose(precision,
                           &system->m[constraint][k],
                           system->m[constraint][k]);
        }
        constraint++;
    }

    /* 3. We set the equality constraints */
    k = ind_source_output_scatt;
    j = ind_target_output_scatt;
    for (i = 1; i < dimension; i++, k++, j++) {
        /* source */
        osl_int_set_si(precision, &system->m[constraint][k], 1);
        /* target */
        osl_int_set_si(precision, &system->m[constraint][j], -1);
        constraint++;
    }

    /* 4. We set the target < source constraint. */
    osl_int_set_si(precision, &system->m[constraint][0], 1);
    /* source */
    osl_int_set_si(precision, &system->m[constraint][k], 1);
    /* target */
    osl_int_set_si(precision, &system->m[constraint][j], -1);
    /* We subtract 1 to the scalar to achieve >0 constraint. */
    osl_int_decrement(precision,
                      &system->m[constraint][nb_columns - 1],
                      system->m[constraint][nb_columns - 1]);

    system->nb_output_dims = nb_output_dims;
    system->nb_input_dims = nb_input_dims;
    system->nb_parameters = nb_par;
    system->nb_local_dims = nb_local_dims;
    system->type = OSL_UNDEFINED;

    violation->domain = system;

    return violation;
}


/**
 * candl_matrix_check_point function:
 * This function checks if there is an integral point in the set of
 * constraints, provided a given domain (possibly NULL).
 *
 * FIXME : is it the same as pip_has_rational_point ?
 * here options->Nq = 1 (default)
 */
int
candl_matrix_check_point(osl_relation_p domain,
                         osl_relation_p context) {
// FIXME : compatibility with osl
//#ifdef CANDL_HAS_PIPLIB_HYBRID
//  return piplib_hybrid_has_integer_point (domain, context, 0);
//#else
    PipOptions* options;
    PipQuast* solution;
    int ret = 0;
    options = pip_options_init();
    options->Simplify = 1;
    options->Urs_parms = -1;
    options->Urs_unknowns = -1;

    solution = pip_solve_osl(domain, context, -1, options);

    if ((solution != NULL) &&
            ((solution->list != NULL) || (solution->condition != NULL)))
        ret = 1;
    pip_options_free(options);
    pip_quast_free(solution);

    return ret;
//#endif
}

