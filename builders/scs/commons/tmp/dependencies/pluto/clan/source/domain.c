
/*+------- <| --------------------------------------------------------**
 **         A                     Clan                                **
 **---     /.\   -----------------------------------------------------**
 **   <|  [""M#                 domain.c                              **
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


#include <stdlib.h>
#include <stdio.h>

#include <osl/macros.h>
#include <osl/relation_list.h>
#include <clan/macros.h>
#include <clan/relation.h>
#include <clan/relation_list.h>
#include <clan/domain.h>
#include <clan/vector.h>


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/


/**
 * clan_domain_idump function:
 * Displays a clan_domain_t structure into a file (file, possibly stdout).
 * \param[in] file   File where informations are printed.
 * \param[in] domain The clan_domain whose information has to be printed.
 * \param[in] level  Number of spaces before printing, for each line.
 */
void clan_domain_idump(FILE* file, clan_domain_p domain, int level) {
    int j, first = 1;

    // Go to the right level.
    for (j = 0; j < level; j++)
        fprintf(file,"|\t");

    if (domain != NULL)
        fprintf(file, "+-- clan_domain_t\n");
    else
        fprintf(file, "+-- NULL clan_domain_t\n");

    while (domain != NULL) {
        if (!first) {
            // Go to the right level.
            for (j = 0; j < level; j++)
                fprintf(file, "|\t");
            fprintf(file, "|   clan_domain_t\n");
        } else
            first = 0;

        // A blank line.
        for (j = 0; j <= level + 1; j++)
            fprintf(file, "|\t");
        fprintf(file, "\n");

        // Print a set of constraint sets (a relation list).
        osl_relation_list_idump(file, domain->constraints, level + 1);

        domain = domain->next;

        // Next line.
        if (domain != NULL) {
            for (j = 0; j <= level; j++)
                fprintf(file, "|\t");
            fprintf(file, "V\n");
        }
    }

    // The last line.
    for (j = 0; j <= level; j++)
        fprintf(file, "|\t");
    fprintf(file, "\n");
}


/**
 * clan_domain_dump function:
 * This function prints the content of a clan_domain_t into
 * a file (file, possibly stdout).
 * \param[in] file File where informations are printed.
 * \param[in] list The relation whose information has to be printed.
 */
void clan_domain_dump(FILE* file, clan_domain_p list) {
    clan_domain_idump(file, list, 0);
}


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/


/**
 * clan_domain_malloc function:
 * This function allocates the memory space for a clan_domain_t structure and
 * sets its fields with default values. Then it returns a pointer to the
 * allocated space.
 * \return A pointer to an empty domain with fields set to default values.
 */
clan_domain_p clan_domain_malloc() {
    clan_domain_p res;

    OSL_malloc(res, clan_domain_p, sizeof(clan_domain_t));
    res->constraints  = NULL;
    res->next = NULL;

    return res;
}


/**
 * clan_domain_free function:
 * This function frees the allocated memory for a clan_domain_t structure, and
 * all the domains stored in the list.
 * \param[in,out] list The pointer to the domain list we want to free.
 */
void clan_domain_free(clan_domain_p list) {
    clan_domain_p tmp;

    if (list == NULL)
        return;

    while (list != NULL) {
        osl_relation_list_free(list->constraints);
        tmp = list->next;
        free(list);
        list = tmp;
    }
}


/*+****************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/


/**
 * clan_domain_clone function:
 * This functions builds and returns a quasi-"hard copy" (not a pointer copy)
 * of a clan_domain_t data structure provided as parameter.
 * \param[in] list The pointer to the domain list we want to copy.
 * \return A pointer to the full copy of the domain list in parameter.
 */
clan_domain_p clan_domain_clone(clan_domain_p list) {
    clan_domain_p clone = NULL, node, previous = NULL;
    int first = 1;

    while (list != NULL) {
        node = clan_domain_malloc();
        node->constraints = osl_relation_list_clone(list->constraints);

        if (first) {
            first = 0;
            clone = node;
            previous = node;
        } else {
            previous->next = node;
            previous = previous->next;
        }

        list = list->next;
    }

    return clone;
}


/**
 * clan_domain_push function:
 * this function sees a list of domains as a stack of domains and performs
 * the push operation onto this stack.
 * \param[in,out] head Pointer to the head of the domain stack.
 * \param[in,out] node Domain node to add to the stack. Its next field is
 *                     updated to the previous head of the stack.
 */
void clan_domain_push(clan_domain_p* head, clan_domain_p node) {
    if (node != NULL) {
        node->next = *head;
        *head = node;
    }
}


/**
 * clan_domain_pop function:
 * this function sees a list of domains as a stack of domains and
 * performs the pop operation onto this stack.
 * \param[in,out] head Pointer to the head of the domain stack. It is updated
 *                to the previous element in the stack (NULL if there is none).
 * \return The top element of the stack (detached from the list).
 */
clan_domain_p clan_domain_pop(clan_domain_p* head) {
    clan_domain_p top = NULL;

    if (*head != NULL) {
        top = *head;
        *head = (*head)->next;
        top->next = NULL;
    }

    return top;
}


/**
 * clan_domain_dup function:
 * this function sees a list of domains as a stack of domains and
 * performs the dup operation (duplicate the top element) onto  this stack.
 * \param[in,out] head Pointer to the head of the domain stack. It is
 *                     updated to the new element after duplication.
 */
void clan_domain_dup(clan_domain_p* head) {
    clan_domain_p top = clan_domain_pop(head);
    clan_domain_push(head, clan_domain_clone(top));
    clan_domain_push(head, top);
}


/**
 * clan_domain_drop function:
 * this function sees a list of domains as a stack of domains and
 * performs the drop operation (pop and destroy popped element) onto
 * this stack.
 * \param[in,out] head Pointer to the head of the domain stack. It is
 *                     updated to the previous element in the stack (NULL
 *                     if there is none).
 */
void clan_domain_drop(clan_domain_p* head) {
    clan_domain_p top = clan_domain_pop(head);
    clan_domain_free(top);
}


/**
 * clan_domain_and function:
 * this function adds new constraints to every set of constraint set of the
 * domain.
 * \param[in,out] domain          The set of constraint set to update.
 * \param[in]     new_constraints The constraints to every constraint set.
 */
void clan_domain_and(clan_domain_p domain, osl_relation_p new_constraints) {
    osl_relation_list_p base_constraints = domain->constraints;

    while (base_constraints != NULL) {
        clan_relation_and(base_constraints->elt, new_constraints);
        base_constraints = base_constraints->next;
    }
}

/**
 * clan_domain_stride function:
 * this function applies the contribution of a loop stride to every set of
 * constraint set of the domain.
 * \param[in,out] domain The set of constraint set to update.
 * \param[in]     depth  The loop depth corresponding to the stride.
 * \param[in]     stride The loop stride value.
 */
void clan_domain_stride(clan_domain_p domain, int depth, int stride) {
    osl_relation_list_p base_constraints = domain->constraints;
    osl_relation_p stride_constraints;

    if ((stride != 1) && (stride != -1)) {
        while (base_constraints != NULL) {
            stride_constraints = clan_relation_stride(base_constraints->elt,
                                 depth, stride);
            osl_relation_free(base_constraints->elt);
            domain->constraints->elt = stride_constraints;
            base_constraints = base_constraints->next;
        }
    }
}


/**
 * clan_domain_for function:
 * this function adds the contribution of a for loop to a domain.
 * \param[in,out] domain         The set of constraint set to update.
 * \param[in]     depth          The loop depth.
 * \param[in]     iterator       The loop iterator symbol.
 * \param[in]     initialization The loop initialiation right part constraints.
 * \param[in]     condition      The loop condition part constraints.
 * \param[in]     stride         The loop stride value.
 * \param[in]     options        Clan's options.
 */
void clan_domain_for(clan_domain_p domain,
                     int depth,
                     clan_symbol_p iterator,
                     osl_relation_p initialization,
                     osl_relation_p condition,
                     int stride,
                     clan_options_p options) {
    osl_vector_p iterator_term;
    osl_relation_p iterator_relation;
    osl_relation_p init_constraints;

    // Generate the set of constraints contributed by the initialization
    // (nb: it could not be done before because we need to know the stride).
    iterator_term = clan_vector_term(iterator, 0, NULL, options->precision);
    osl_int_set_si(options->precision, &iterator_term->v[depth], 1);
    iterator_relation = osl_relation_from_vector(iterator_term);
    if (stride > 0) {
        init_constraints = clan_relation_greater(iterator_relation,
                           initialization, 0);
    } else {
        init_constraints = clan_relation_greater(initialization,
                           iterator_relation, 0);
    }
    osl_vector_free(iterator_term);
    osl_relation_free(iterator_relation);

    // Add the contribution of the initialization to the current domain.
    clan_domain_and(domain, init_constraints);

    // Add the contribution of the condition to the current domain.
    if (!options->noloopcontext)
        clan_relation_loop_context(condition, init_constraints, depth);
    clan_domain_and(domain, condition);

    // Add the contribution of the stride to the current domain.
    clan_domain_stride(domain, depth, stride);

    osl_relation_free(init_constraints);
}


/**
 * clan_domain_xfor function:
 * this function adds the contribution of a xfor loop to a domain.
 * \param[in,out] domain         The set of constraint set to update.
 * \param[in]     depth          The loop depth.
 * \param[in]     iterator       The loop iterator symbols.
 * \param[in]     initialization The loop initialiation right part constraints.
 * \param[in]     condition      The loop condition part constraints.
 * \param[in]     stride         The loop stride values.
 * \param[in]     options        Clan's options.
 */
void clan_domain_xfor(clan_domain_p domain,
                      int depth,
                      clan_symbol_p iterator,
                      osl_relation_list_p initialization,
                      osl_relation_list_p condition,
                      int* stride,
                      clan_options_p options) {
    int i;
    int nb_indices = clan_relation_list_nb_elements(initialization);
    int nb_constraint_sets = clan_relation_list_nb_elements(domain->constraints);
    osl_relation_list_p base, new = NULL;
    clan_domain_p shell = NULL;

    // Three possible cases:
    // 1. the number of constraint sets in the domain is 1 ("first xfor"),
    //    then we duplicate the constraint sets to match the number of xfor
    //    indices, then each constraint set receives the contribution of the
    //    corresponding xfor index,
    // 2. the number of constraints sets in the domain is equal to
    //    the number of xfor indices, then each constraint set
    //    receives the contribution of the corresponding xfor index,
    // 3. an error is reported (this should be checked in the parser for
    //    reporting through yyerror).

    if (nb_constraint_sets == 1) {
        for (i = 0; i < nb_indices - 1; i++) {
            osl_relation_list_dup(&domain->constraints);
        }
    } else if (nb_constraint_sets != nb_indices) {
        CLAN_error("incorrect number of indices in an xfor loop");
    }

    // -1. Prepare a domain "shell" to process the xfor indices one by one.
    shell = clan_domain_malloc();
    shell->constraints = osl_relation_list_malloc();
    base = domain->constraints;
    // -2. For each xfor index
    for (i = 0; i < nb_indices; i++) {
        // -2.1 Put the corresponding base constraints in the domain shell.
        shell->constraints->elt = osl_relation_clone(base->elt);
        // -2.2 Apply the contribution of the loop to it.
        clan_domain_for(shell, depth, iterator, initialization->elt,
                        condition->elt, stride[i], options);
        // -2.3 Add the final constraint set to a new list.
        osl_relation_list_add(&new, shell->constraints);
        // -2.4 Prepare the variables to process the next xfor index.
        iterator = iterator->next;
        initialization = initialization->next;
        condition = condition->next;
        shell->constraints = osl_relation_list_malloc();
        base = base->next;
    }
    // -3. Replace the original set of constraint set with the new one.
    osl_relation_list_free(domain->constraints);
    domain->constraints = new;

    clan_domain_free(shell);
}
