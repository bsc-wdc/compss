
/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (                  violation.c                            **
 **----  \#/  --------------------------------------------------------**
 **    .-"#'-.        First version: december 12th 2005               **
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
#include <string.h>
#include <osl/macros.h>
#include <osl/relation.h>
#include <osl/statement.h>
#include <osl/scop.h>
#include <osl/extensions/dependence.h>
#include <candl/dependence.h>
#include <candl/label_mapping.h>
#include <candl/macros.h>
#include <candl/matrix.h>
#include <candl/piplib.h>
#include <candl/piplib-wrapper.h>
#include <candl/scop.h>
#include <candl/statement.h>
#include <candl/violation.h>


/******************************************************************************
 *                          Structure display function                        *
 ******************************************************************************/


/**
 * candl_violation_idump function:
 * Displays a CandlViolation structure (violation) into a file (file,
 * possibly stdout) in a way that trends to be understandable without falling
 * in a deep depression or, for the lucky ones, getting a headache... It
 * includes an indentation level (level) in order to work with others
 * idump functions.
 * - 18/09/2003: first version.
 */
void candl_violation_idump(FILE *file, candl_violation_p violation,
                           int level) {
    int j, first=1;
    osl_dependence_p next=NULL;

    if (violation != NULL) { /* Go to the right level. */
        for(j=0; j<level; j++)
            fprintf(file,"|\t");
        fprintf(file,"+-- CandlViolation\n");
    } else {
        for(j=0; j<level; j++)
            fprintf(file,"|\t");
        fprintf(file,"+-- NULL dependence violation\n");
    }

    while (violation != NULL) {
        if (!first) { /* Go to the right level. */
            for(j=0; j<level; j++)
                fprintf(file,"|\t");
            fprintf(file,"|   CandlViolation\n");
        } else {
            first = 0;
        }

        /* A blank line. */
        for(j=0; j<=level+1; j++)
            fprintf(file,"|\t");
        fprintf(file,"\n");

        /* Go to the right level and print the dimension. */
        for(j=0; j<=level; j++)
            fprintf(file,"|\t");
        fprintf(file,"Dimension: %d\n",violation->dimension);

        /* A blank line. */
        for(j=0; j<=level+1; j++)
            fprintf(file,"|\t");
        fprintf(file,"\n");

        /* Print the dependence. */
        if (violation->dependence != NULL) {
            next = violation->dependence->next; /* To not print the whole list... */
            violation->dependence->next = NULL; /* I know it's not beautiful :-/ ! */
        }
        osl_dependence_idump(file,violation->dependence,level+1);
        if (violation->dependence != NULL)
            violation->dependence->next = next;

        /* Print the dependence polyhedron. */
        osl_relation_idump(file, violation->domain, level+1);

        violation = violation->next;

        /* Next line. */
        if (violation != NULL) {
            for (j=0; j<=level; j++)
                fprintf(file,"|\t");
            fprintf(file,"V\n");
        }
    }

    /* The last line. */
    for(j=0; j<=level; j++)
        fprintf(file,"|\t");
    fprintf(file,"\n");
}


/* candl_violation_print function:
 * This function prints the content of a CandlViolation structure
 * (violation) into a file (file, possibly stdout).
 */
void candl_violation_dump(FILE * file, candl_violation_p violation) {
    candl_violation_idump(file,violation,0);
}


/* candl_violation_pprint function:
 * This function prints the content of a CandlViolation structure (violation)
 * into a file (file, possibly stdout) as a Graphviz input file.
 * See http://www.graphviz.org
 * - 12/12/2005: first version.
 */
void candl_violation_pprint(FILE * file, candl_violation_p violation) {
    int i=0;
    osl_dependence_p dependence;
    candl_statement_usr_p s_usr;
    candl_statement_usr_p t_usr;

    fprintf(file,"digraph G {\n");

    fprintf(file,"# Legality Violation Graph\n");
    fprintf(file,"# Generated by Candl "CANDL_RELEASE" "CANDL_VERSION" bits\n");
    if (violation == NULL)
        fprintf(file,"# Congratulations: the transformation is legal !\n");

    while (violation != NULL) {
        dependence = violation->dependence;
        s_usr = dependence->stmt_source_ptr->usr;
        t_usr = dependence->stmt_target_ptr->usr;

        fprintf(file,"  S%d -> S%d [label=\" ", s_usr->label,
                t_usr->label);
        switch (dependence->type) {
        case OSL_UNDEFINED :
            fprintf(file,"UNSET");
            break;
        case OSL_DEPENDENCE_RAW   :
            fprintf(file,"RAW")  ;
            break;
        case OSL_DEPENDENCE_WAR   :
            fprintf(file,"WAR")  ;
            break;
        case OSL_DEPENDENCE_WAW   :
            fprintf(file,"WAW")  ;
            break;
        case OSL_DEPENDENCE_RAR   :
            fprintf(file,"RAR")  ;
            break;
        default :
            fprintf(file,"unknown");
        }
        fprintf(file," depth %d, ref %d->%d, viol %d \"];\n",
                dependence->depth,
                dependence->ref_source,
                dependence->ref_target,
                violation->dimension);
        violation = violation->next;
        i++;
    }

    if (i>4)
        fprintf(file,"# Number of edges = %i\n}\n",i);
    else
        fprintf(file,"}\n");
}


/* candl_violation_view function:
 * This function uses dot (see http://www.graphviz.org) and gv (see
 * http://wwwthep.physik.uni-mainz.de/~plass/gv) tools to display the
 * violation graph.
 * - 20/03/2006: first version.
 */
void candl_violation_view(candl_violation_p violation) {
    FILE * temp_output;
    temp_output = fopen(CANDL_TEMP_OUTPUT,"w+");
    candl_violation_pprint(temp_output,violation);
    fclose(temp_output);
    /* check the return to remove the warning compilation */
    if(system("dot -Tps "CANDL_TEMP_OUTPUT" | gv - && rm -f "CANDL_TEMP_OUTPUT" &"))
        ;
}


/******************************************************************************
 *                         Memory deallocation function                       *
 ******************************************************************************/

/* candl_violation_free function:
 * This function frees the allocated memory for a CandlViolation structure.
 * - 18/09/2003: first version.
 */
void candl_violation_free(candl_violation_p violation) {
    candl_violation_p next;
    while (violation != NULL) {
        next = violation->next;
        osl_relation_free(violation->domain);
        free(violation);
        violation = next;
    }
}


/******************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/


/**
 * candl_violation_malloc function:
 * This function allocates the memory space for a CandlViolation structure
 * and sets its fields with default values. Then it returns a pointer to the
 * allocated space.
 * - 07/12/2005: first version.
 */
candl_violation_p candl_violation_malloc() {
    candl_violation_p violation;

    CANDL_malloc(violation, candl_violation_p, sizeof(candl_violation_t));

    /* We set the various fields with default values. */
    violation->dependence = NULL;
    violation->domain     = NULL;
    violation->next       = NULL;
    violation->dimension  = OSL_UNDEFINED;
    violation->source_nb_output_dims_scattering = -1;
    violation->target_nb_output_dims_scattering = -1;
    violation->source_nb_local_dims_scattering = -1;
    violation->target_nb_local_dims_scattering = -1;

    return violation;
}


/* cloog_violation_add function:
 * This function adds a CloogViolation structure (violation) at a given place
 * (now) of a NULL terminated list of CloogViolation structures. The beginning
 * of this list is (start). This function updates (now) to the end of the loop
 * list (loop), and updates (start) if the added element is the first one -that
 * is when (start) is NULL-.
 * - 12/12/2005: first version (from candl_dependence_add,
 *                              currently osl_dependence_add).
 */
void candl_violation_add(candl_violation_p* start,
                         candl_violation_p* now,
                         candl_violation_p violation) {
    if (violation != NULL) {
        if (*start == NULL) {
            *start = violation;
            *now = *start;
        } else {
            (*now)->next = violation;
            *now = (*now)->next;
        }

        while ((*now)->next != NULL)
            *now = (*now)->next;
    }
}


/**
 * \brief build the list of violated dependences by a program transformation
 * candidate, according to some user options.   The scop must be initialized
 * with proper usr fields via ::candl_scop_usr_init function.
 * \deprecated Use ::candl_violation, this function is intended to temporary
 * remedy the API change.
 * \warning This function ignores unions of relations.
 * \param [in,out] orig_scop       The original scop.
 * \param [in]     orig_dependence The dependence graph of the original scop.
 * \param [in]     test_scop       The candidate scop with new scheduling.
 * \param [in]     options         Analysis options.
 * \returns the linked list of violations.
 **
 * - 12/12/2005: first version.
 * - 01/05/2015: renamed in a process of adding relation union support.
 */
candl_violation_p candl_violation_single(osl_scop_p orig_scop,
        osl_dependence_p orig_dependence,
        osl_scop_p test_scop,
        candl_options_p options) {
    osl_statement_p source, target, iter;
    osl_statement_p *stmts = NULL;
    osl_relation_p t_source, t_target;
    candl_statement_usr_p s_usr, t_usr;
    candl_violation_p violation = NULL, now, new;
    PipOptions *pip_options;
    PipQuast *solution;
    int i;
    int nb_par = orig_scop->context->nb_parameters;
    int nb_stmts = 0;
    int dimension, max_dimension, violated;

    /* If there is no scop or transformation, we consider this legal. */
    if (test_scop == NULL)
        return NULL;

    /* Temporary array to access faster at the `label'th statement */
    for (iter = test_scop->statement ; iter != NULL ;
            iter = iter->next, nb_stmts++)
        ;
    CANDL_malloc(stmts, osl_statement_p*, sizeof(osl_statement_p) * nb_stmts);
    for (i = 0, iter = test_scop->statement ; iter != NULL ;
            iter = iter->next, i++) {
        stmts[i] = iter;
    }

    pip_options = pip_options_init();
    pip_options->Simplify = 1;

    /* We check every edge of the dependence graph. */
    while (orig_dependence != NULL) {
        source = orig_dependence->stmt_source_ptr;
        target = orig_dependence->stmt_target_ptr;
        s_usr  = source->usr;
        t_usr  = target->usr;

        /* We find the source transformation matrix. */
        t_source = stmts[s_usr->label]->scattering;

        /* We find the target transformation matrix. */
        t_target = stmts[t_usr->label]->scattering;

        /* The maximal dimension we have to check for legality. */
        max_dimension = CANDL_min(t_source->nb_output_dims,t_target->nb_output_dims);

        /* We check each dimension for legality. */
        for (dimension = 1; dimension <= max_dimension; dimension++) {
            violated = 0;

            /* We build the constraint system corresponding to that
             * violation then check if there is an integral point inside,
             * if yes there is actually a dependence violation and we
             * will add this one to the list.
             */
            new = candl_matrix_violation(orig_dependence, t_source,
                                         t_target, dimension,
                                         nb_par);
            solution = pip_solve_osl(new->domain, orig_scop->context, -1, pip_options);

            if ((solution != NULL) &&
                    ((solution->list != NULL) || (solution->condition != NULL)))
                violated = 1;

            pip_quast_free(solution);

            if (violated) {

                /* We set the various fields with corresponding values. */
                new->dependence = orig_dependence;
                new->dimension = dimension;
                candl_violation_add(&violation,&now,new);

                if (!options->fullcheck) {
                    pip_options_free(pip_options);
                    if(stmts)
                        free(stmts);
                    return violation;
                }
            } else if (new) {
                candl_violation_free(new);
            }
        }
        orig_dependence = orig_dependence->next;
    }

    free(stmts);

    pip_options_free(pip_options);

    return violation;
}

// Return a list of violations for a particular dependence (check for violation
// at each depth).  Source and target statements may differ from those used in
// the dependence as far as their dimensionality match.  This is an internal
// function that is aware of statement remapping due to union removal.
static candl_violation_p violation_helper(osl_dependence_p orig_dependence,
        osl_relation_p context, osl_statement_p source, osl_statement_p target,
        int nb_parameters, candl_options_p options, PipOptions * pip_options) {
    int dimension;
    int max_dimension;
    candl_violation_p new;
    candl_violation_p now;
    candl_violation_p violation = NULL;
    int violated;
    PipQuast * solution;

    max_dimension = CANDL_min(source->scattering->nb_output_dims,
                              target->scattering->nb_output_dims);
    for (dimension = 1; dimension <= max_dimension; dimension++) {
        violated = 0;

        /* We build the constraint system corresponding to that
         * violation then check if there is an integral point inside,
         * if yes there is actually a dependence violation and we
         * will add this one to the list.
         */
        new = candl_matrix_violation(orig_dependence,
                                     source->scattering,
                                     target->scattering,
                                     dimension,
                                     nb_parameters);
        solution = pip_solve_osl(new->domain, context, -1, pip_options);

        if ((solution != NULL) &&
                ((solution->list != NULL) || (solution->condition != NULL)))
            violated = 1;

        pip_quast_free(solution);


        if (violated) {
            /* We set the various fields with corresponding values. */
            new->dependence = orig_dependence;
            new->dimension = dimension;
            candl_violation_add(&violation, &now, new);

            if (!options->fullcheck) {
                return violation;
            }
        } else if (new) {
            candl_violation_free(new);
        }
    }
    return violation;
}

/**
 * \brief Append the given appendix, possibly a list, to the end of the of the
 * violations list.
 * \param [in,out] violation  A pointer, possibly null, to the end of the list.
 * \param [in]     appendix   An element to append to the list.
 */
void candl_violation_append(candl_violation_p *violation,
                            candl_violation_p appendix) {
    candl_violation_p vptr;
    if (*violation == NULL) {
        *violation = appendix;
    } else {
        for (vptr = *violation; vptr->next != NULL; vptr = vptr->next) {
            vptr->next = appendix;
        }
    }
}

/**
 * \brief Compute violations of dependences in the orig_scop introduced by the
 * scheduling of the test_scop.  The input scops must have the same number of
 * statements.  Statements' domains must be equal and stored in the same order
 * in both scops.  Dependence graph of the original scop may be obtained
 * through parameter.
 * Contrary to the ::candl_violation_single, scops should not have the usr data
 * structure initialized in advance.
 * \warning The input scop <b>may be modified</b>, namely its access relations
 * will be rewritten and respective pointers invalidated, in case when options
 * include any scalar manipulation.
 * \param [in,out] orig_scop  The original scop.
 * \param [in]     test_scop  The transformed scop with a different scheduling.
 * \param [out]    dependence The dependence graph of the original scop.
 *                            Transfers ownership to the caller.
 * \param [in]     options    Analysis options.
 * \return                    List of dependence violations, \c NULL if empty.
 */
candl_violation_p candl_violation(osl_scop_p orig_scop,
                                  osl_scop_p test_scop,
                                  osl_dependence_p *dependence,
                                  candl_options_p options) {
    osl_scop_p test_scop_nounion;
    osl_scop_p orig_scop_nounion;
    candl_label_mapping_p test_mapping;
    candl_label_mapping_p orig_mapping;
    osl_dependence_p orig_dependence;
    osl_dependence_p dependence_ptr;
    int label_source, label_target;
    int unmapped_label_source, unmapped_label_target;
    int nb_parameters;
    candl_label_mapping_p test_source_mapping;
    candl_label_mapping_p test_target_mapping;
    osl_statement_p source;
    osl_statement_p target;
    candl_violation_p violation = NULL;
    candl_violation_p new_violation;
    candl_violation_p violation_end_ptr;
    int violated;
    PipOptions *pip_options;

    if (test_scop == NULL)
        return NULL;

    nb_parameters = orig_scop->context->nb_parameters;

    candl_scop_usr_init(test_scop);
    candl_scop_usr_init(orig_scop);

    if (!options->unions) {
        orig_dependence = candl_dependence_single(orig_scop, options);
        violation = candl_violation_single(orig_scop, orig_dependence, test_scop,
                                           options);
        candl_dependence_init_fields(orig_scop, orig_dependence);
        candl_scop_usr_cleanup(test_scop);
        candl_scop_usr_cleanup(orig_scop);
        if (dependence != NULL)
            *dependence = orig_dependence;
        else
            osl_dependence_free(orig_dependence);
        return violation;
    }

    test_scop_nounion = candl_scop_remove_unions(test_scop);
    candl_scop_usr_init(test_scop_nounion);
    test_mapping = candl_scop_label_mapping(test_scop_nounion);

    orig_scop_nounion = candl_scop_remove_unions(orig_scop);
    candl_scop_usr_init(orig_scop_nounion);
    orig_mapping = candl_scop_label_mapping(orig_scop_nounion);

    orig_dependence = candl_dependence_single(orig_scop_nounion, options);
    dependence_ptr = orig_dependence;
    // Copy accesses after scalar operations.
    if (options->scalar_renaming || options->scalar_expansion ||
            options->scalar_privatization) {
        candl_scop_copy_access(orig_scop, orig_scop_nounion, orig_mapping);
    }

    pip_options = pip_options_init();
    pip_options->Simplify = 1;

    violated = 0;
    for ( ; orig_dependence != NULL; orig_dependence = orig_dependence->next) {
        label_source = orig_dependence->label_source;
        label_target = orig_dependence->label_target;
        // Find all statements created from a single statement with unions and check
        // for violation in each combination of orig/test scatterings.
        // Domain unions are already taken into account by dependence computation
        // since it had created one dependence per domain relation combination.
        unmapped_label_source = candl_label_mapping_find_original(orig_mapping,
                                label_source);
        unmapped_label_target = candl_label_mapping_find_original(orig_mapping,
                                label_target);
        for (test_source_mapping = test_mapping; test_source_mapping != NULL;
                test_source_mapping = test_source_mapping->next) {
            if (test_source_mapping->original == unmapped_label_source) {
                source = candl_statement_find_label(test_scop_nounion->statement,
                                                    test_source_mapping->mapped);
                for (test_target_mapping = test_mapping; test_target_mapping != NULL;
                        test_target_mapping = test_target_mapping->next) {
                    if (test_target_mapping->original == unmapped_label_target) {
                        target = candl_statement_find_label(test_scop_nounion->statement,
                                                            test_target_mapping->mapped);
                        new_violation = violation_helper(orig_dependence,
                                                         orig_scop->context, source, target, nb_parameters,
                                                         options, pip_options);
                        candl_violation_add(&violation, &violation_end_ptr, new_violation);
                        if (new_violation && !options->fullcheck) {
                            violated = 1;
                            break;
                        }
                    }
                }
                if (violated)
                    break;
            }
        }
        if (violated)
            break;
    }

    pip_options_free(pip_options);
    candl_dependence_remap(dependence_ptr, orig_scop, orig_mapping);
    candl_dependence_init_fields(orig_scop, dependence_ptr);
    if (dependence != NULL)
        *dependence = dependence_ptr;
    else
        osl_dependence_free(dependence_ptr);
    candl_scop_usr_cleanup(orig_scop_nounion);
    candl_scop_usr_cleanup(test_scop_nounion);
    osl_scop_free(test_scop_nounion);
    osl_scop_free(orig_scop_nounion);
    candl_label_mapping_free(test_mapping);
    candl_label_mapping_free(orig_mapping);
    candl_scop_usr_cleanup(test_scop);
    candl_scop_usr_cleanup(orig_scop);
    return violation;
}

