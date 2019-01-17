
/*+------- <| --------------------------------------------------------**
 **         A                     Clan                                **
 **---     /.\   -----------------------------------------------------**
 **   <|  [""M#                 relation.c                              **
 **-   A   | #   -----------------------------------------------------**
 **   /.\ [""M#         First version: 30/04/2008                     **
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
#include <osl/int.h>
#include <osl/relation.h>
#include <clan/macros.h>
#include <clan/options.h>
#include <clan/relation.h>

int clan_parser_nb_ld(void);
void clan_parser_add_ld(void);


/*+****************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/


/**
 * clan_relation_tag_array function:
 * this function tags a relation to explicit it is describing the array index of
 * a given array. This means using OpenScop representation that the very first
 * output dimension will correspond to the constraint dim = array_id. It updates
 * directly the relation provided as parameter. A new row and a new column are
 * inserted to the existing relation and the number of output dimensions is
 * incremented.
 * \param[in,out] relation The relation to tag.
 * \param[in]     array    The array number.
 */
void clan_relation_tag_array(osl_relation_p relation, int array) {
    if (relation == NULL)
        CLAN_error("relation cannot be array-tagged");

    osl_relation_insert_blank_row(relation, 0);
    osl_relation_insert_blank_column(relation, 1);
    osl_int_set_si(relation->precision, &relation->m[0][1], -1);
    osl_int_set_si(relation->precision,
                   &relation->m[0][relation->nb_columns - 1], array);
    relation->nb_output_dims++;
}


/**
 * clan_relation_build_context function:
 * this function builds a context relation with 'nb_parameters' parameters.
 * Depending on the bounded_context option, the context has no contraint or
 * each parameter is set to be >= -1.
 * \param[in] nb_parameters The number of parameters for the context.
 * \param[in] options       The global options of Clan.
 * \return A context relation with 'nb_parameters' parameters.
 */
osl_relation_p clan_relation_build_context(int nb_parameters,
        clan_options_p options) {
    int i;
    osl_relation_p context = NULL;

    if (options->bounded_context) {
        context = osl_relation_pmalloc(options->precision,
                                       nb_parameters, nb_parameters + 2);
        for (i = 0; i < nb_parameters; i++) {
            osl_int_set_si(options->precision, &context->m[i][0], 1);
            osl_int_set_si(options->precision, &context->m[i][i + 1], 1);
            osl_int_set_si(options->precision,
                           &context->m[i][context->nb_columns - 1], 1);
        }
    } else {
        context = osl_relation_pmalloc(options->precision, 0, nb_parameters + 2);
    }
    osl_relation_set_type(context, OSL_TYPE_CONTEXT);
    osl_relation_set_attributes(context, 0, 0, 0, nb_parameters);
    return context;
}


/**
 * clan_relation_scattering function:
 * this function builds the scattering relation for the clan_statement_t
 * structures thanks to the parser current state of parser_scattering (rank)
 * and parser_depth (depth). The input scattering vector has 2depth+1
 * elements. Each even element gives the "position" of the statement for
 * every loop depth (see Feautrier's demonstration of existence of a schedule
 * for any SCoP or CLooG's manual for original scattering function to
 * understand if necessary). Each  This function just "expands" this
 * vector to a (2*n+1)-dimensional schedule for a statement at depth n and
 * returns it. Each odd element gives the loop direction: 1 for forward
 * (meaning the loop stride is positive) -1 for backward (the loop stride
 * is negative).
 * \param[in] vector    The statement position / loop direction vector.
 * \param[in] depth     The depth of the statement.
 * \param[in] precision The precision of the relation elements.
 */
osl_relation_p clan_relation_scattering(int* vector, int depth,
                                        int precision) {
    int i, j, nb_rows, nb_columns;
    int beta_col, alpha_col;
    osl_relation_p scattering;

    nb_rows    = (2 * depth + 1);
    nb_columns = (2 * depth + 1) + (depth) + (CLAN_MAX_PARAMETERS) + 2;
    scattering = osl_relation_pmalloc(precision, nb_rows, nb_columns);
    osl_relation_set_type(scattering, OSL_TYPE_SCATTERING);
    osl_relation_set_attributes(scattering, 2 * depth + 1, depth, 0,
                                CLAN_MAX_PARAMETERS);

    // The output dimension identity
    for (i = 0; i < 2 * depth + 1; i++)
        osl_int_set_si(precision, &scattering->m[i][i + 1], -1);

    // The beta and alpha.
    j = 0;
    beta_col = nb_columns - 1;
    for (i = 0; i < depth; i++) {
        alpha_col = (2 * depth + 1) + i + 1;
        osl_int_set_si(precision, &scattering->m[j][beta_col], vector[j]);
        osl_int_set_si(precision, &scattering->m[j+1][alpha_col], vector[j+1]);
        j += 2;
    }
    osl_int_set_si(precision, &scattering->m[nb_rows-1][beta_col], vector[j]);

    return scattering;
}


/**
 * clan_relation_new_output_vector function:
 * this function adds a new output dimension and a new
 * constraint corresponding to the new output dimension to an existing
 * relation. The new output dimension is added after the existing output
 * dimensions. It is supposed to be equal to the vector expression passed
 * as an argument. The input relation is direcly updated.
 * \param[in,out] relation The relation to add a new output dimension.
 * \param[in]     vector   The expression the new output dimension is equal to.
 */
void clan_relation_new_output_vector(osl_relation_p relation,
                                     osl_vector_p vector) {
    int i, new_col, new_row;

    if (relation == NULL)
        CLAN_error("cannot add a new output dimension to a NULL relation");
    else if (vector == NULL)
        CLAN_error("cannot add a NULL expression of an output dimension");
    else if (relation->precision != vector->precision)
        CLAN_error("incompatible precisions");

    if (relation->nb_output_dims == OSL_UNDEFINED)
        new_col = 1;
    else
        new_col = relation->nb_output_dims + 1;
    new_row = relation->nb_rows;

    if ((relation->nb_columns - (new_col - 1)) != vector->size)
        CLAN_error("incompatible sizes");
    else if (!osl_int_zero(vector->precision, vector->v[0]))
        CLAN_error("the output dimension expression should be an equality");

    // Prepare the space for the new output dimension and the vector.
    osl_relation_insert_blank_column(relation, new_col);
    osl_relation_insert_blank_row(relation, new_row);
    relation->nb_output_dims = new_col;

    // Insert the new output dimension.
    osl_int_set_si(relation->precision, &relation->m[new_row][new_col], -1);
    for (i = 1; i < vector->size; i++)
        osl_int_assign(relation->precision,
                       &relation->m[new_row][new_col + i], vector->v[i]);
}


/**
 * clan_relation_new_output_scalar function:
 * this function adds a new output dimension and a new
 * constraint corresponding to the new output dimension to an existing
 * relation. The new output dimension is added after the existing output
 * dimensions. It is supposed to be equal to a scalar value passed
 * as an argument. The input relation is direcly updated.
 * \param[in,out] relation The relation to add a new output dimension.
 * \param[in]     scalar   The scalar the new output dimension is equal to.
 */
void clan_relation_new_output_scalar(osl_relation_p relation, int scalar) {
    int new_col, new_row;

    if (relation == NULL)
        CLAN_error("cannot add a new output dimension to a NULL relation");

    if (relation->nb_output_dims == OSL_UNDEFINED)
        new_col = 1;
    else
        new_col = relation->nb_output_dims + 1;
    new_row = relation->nb_rows;

    // Prepare the space for the new output dimension and the vector.
    osl_relation_insert_blank_column(relation, new_col);
    osl_relation_insert_blank_row(relation, new_row);
    relation->nb_output_dims = new_col;

    // Insert the new output dimension.
    osl_int_set_si(relation->precision, &relation->m[new_row][new_col], -1);
    osl_int_set_si(relation->precision,
                   &relation->m[new_row][relation->nb_columns - 1], scalar);
}


/**
 * clan_relation_compact function:
 * This function compacts a relation such that it uses the right number
 * of columns (during construction we used CLAN_MAX_DEPTH, CLAN_MAX_LOCAL_DIMS
 * and CLAN_MAX_PARAMETERS to define relation and vector sizes). It modifies
 * directly the relation provided as parameter.
 * \param[in,out] relation      The relation to compact.
 * \param[in]     nb_parameters The true number of parameters in the SCoP.
 */
void clan_relation_compact(osl_relation_p relation,
                           int nb_parameters) {
    int i, j, nb_columns;
    int nb_output_dims, nb_input_dims, nb_local_dims, nb_out_in_loc;
    osl_relation_p compacted;

    while (relation != NULL) {
        nb_output_dims = relation->nb_output_dims;
        nb_input_dims  = relation->nb_input_dims;
        nb_local_dims  = relation->nb_local_dims;
        nb_out_in_loc  = nb_output_dims + nb_input_dims + nb_local_dims;

        nb_columns = nb_out_in_loc  + nb_parameters + 2;
        compacted = osl_relation_pmalloc(relation->precision,
                                         relation->nb_rows, nb_columns);

        for (i = 0; i < relation->nb_rows; i++) {
            // We copy the equ/inequ tag, the output and input coefficients.
            for (j = 0; j <= nb_output_dims + nb_input_dims; j++)
                osl_int_assign(relation->precision,
                               &compacted->m[i][j], relation->m[i][j]);

            // Then we copy the local dimension coefficients.
            for (j = 0; j < nb_local_dims; j++)
                osl_int_assign(relation->precision,
                               &compacted->m[i][nb_output_dims + nb_input_dims + 1 + j],
                               relation->m[i][CLAN_MAX_DEPTH + 1 + j]);

            // Then we copy the parameter coefficients.
            for (j = 0; j < nb_parameters; j++)
                osl_int_assign(relation->precision,
                               &compacted->m[i][j + nb_out_in_loc + 1],
                               relation->m[i][relation->nb_columns - CLAN_MAX_PARAMETERS -1 + j]);

            // Lastly the scalar coefficient.
            osl_int_assign(relation->precision,
                           &compacted->m[i][nb_columns - 1],
                           relation->m[i][relation->nb_columns - 1]);
        }

        osl_relation_free_inside(relation);

        // Replace the inside of relation.
        relation->nb_rows       = compacted->nb_rows;
        relation->nb_columns    = compacted->nb_columns;
        relation->m             = compacted->m;
        relation->nb_parameters = nb_parameters;

        // Free the compacted "container".
        free(compacted);

        relation = relation->next;
    }
}


/**
 * clan_relation_greater function:
 * this function generates a relation corresponding to the equivalent
 * constraint set of two linear expressions sets involved in a
 * condition "greater (or equal) to". min and max are two linear expression
 * sets, e.g. (a, b, c) for min and (d, e) for max, where a, b, c, d and e
 * are linear expressions. This function creates the constraint set
 * corresponding to, e.g., min(a, b, c) > max(d, e), i.e.:
 * (a > d && a > e && b > d && b > e && c > d && c > e). It may also create
 * the constraint set corresponding to min(a, b, c) >= max(d, e).
 * The relations max and min are not using the OpenScop/PolyLib format:
 * each row corresponds to a linear expression as usual but the first
 * element is not devoted to the equality/inequality marker but to store
 * the value of the ceild or floord divisor. Hence, each row may correspond
 * to a ceil of a linear expression divided by an integer in min or
 * to a floor of a linear expression divided by an integer in max.
 * \param[in] min    Set of (ceild) linear expressions corresponding
 *                   to the minimum of the (ceild) linear expressions.
 * \param[in] max    Set of (floord) linear expressions corresponding
 *                   to the maximum of the (floord) linear expressions.
 * \param[in] strict 1 if the condition is min > max, 0 if it is min >= max.
 * \return A set of linear constraints corresponding to min > (or >=) max.
 */
osl_relation_p clan_relation_greater(osl_relation_p min, osl_relation_p max,
                                     int strict) {
    int imin, imax, j, precision;
    int a, b;
    osl_relation_p r;
    osl_int_t b_min, a_max;

    if ((min == NULL) || (max == NULL) || (strict < 0) || (strict > 1) ||
            (min->nb_columns != max->nb_columns))
        CLAN_error("cannot compose relations");

    precision = min->precision;
    osl_int_init(precision, &a_max);
    osl_int_init(precision, &b_min);
    r = osl_relation_pmalloc(precision,
                             min->nb_rows * max->nb_rows, min->nb_columns);

    // For each row of min
    for (imin = 0; imin < min->nb_rows; imin++) {
        // For each row of max
        // We have a couple min/a >= max/b to translate to b*min - a*max >= 0
        //      or a couple min/a > max/b  to translate to b*min - a*max - 1 >= 0
        // TODO: here a and b are > 0, this may be generalized to avoid a
        //       problem if the grammar is updated. Plus it's debatable to use
        //       b*min - a*max -a*b >= 0 in the second case.

        // -1. Find a
        a = osl_int_get_si(precision, min->m[imin][0]);
        a = (a == 0) ? 1 : a;

        for (imax = 0; imax < max->nb_rows; imax++) {
            // -2. Find b
            b = osl_int_get_si(precision, max->m[imax][0]);
            b = (b == 0) ? 1 : b;

            // -3. Compute b*min - a*max to the new relation.
            for (j = 1; j < max->nb_columns; j++) {
                // -3.1. Compute b*min
                osl_int_mul_si(precision, &b_min, min->m[imin][j], b);

                // -3.2. Compute a*max
                osl_int_mul_si(precision, &a_max, max->m[imax][j], a);

                // -3.3. Compute b*min - a*max
                osl_int_sub(precision,
                            &r->m[imin * max->nb_rows + imax][j], b_min, a_max);
            }

            // -4. Add -1 if the condition is min/a > max/b, add 0 otherwise.
            osl_int_add_si(precision,
                           &r->m[imin * max->nb_rows + imax][max->nb_columns - 1],
                           r->m[imin * max->nb_rows + imax][max->nb_columns - 1],
                           -strict);
            // -5. Set the equality/inequality marker to inequality.
            osl_int_set_si(precision, &r->m[imin * max->nb_rows + imax][0], 1);
        }
    }

    osl_int_clear(precision, &a_max);
    osl_int_clear(precision, &b_min);
    return r;
}


/**
 * clan_relation_negate_inequality function:
 * this function replaces an inequality constraint in a relation with its
 * negation (e.g., i >= 0 will become i < 0). Note that it does not check
 * that the constraint is actually an inequality.
 * \param[in,out] relation The relation where to oppose a constraint.
 * \param[in]     row      The row corresponding to the constraint to oppose.
 */
static
void clan_relation_negate_inequality(osl_relation_p relation, int row) {
    int i;

    // Oppose all constraint elements.
    for (i = 1; i < relation->nb_columns; i++)
        osl_int_oppose(relation->precision,
                       &relation->m[row][i], relation->m[row][i]);

    // The constant term - 1.
    osl_int_decrement(relation->precision,
                      &relation->m[row][relation->nb_columns - 1],
                      relation->m[row][relation->nb_columns - 1]);
}


/**
 * clan_relation_extract_constraint function:
 * this function creates and returns a new relation from a single constraint
 * of a relation. The constraint corresponds to a specified row of the
 * constraint matrix of the first element of the specified relation union.
 * \param[in] relation The input relation.
 * \param[in] row      The row corresponding to the constraint to extract.
 * \return A new relation with the extracted constraint only.
 */
static
osl_relation_p clan_relation_extract_constraint(osl_relation_p relation,
        int row) {
    int i, precision = relation->precision;
    osl_relation_p constraint;

    constraint = osl_relation_pmalloc(precision, 1, relation->nb_columns);
    constraint->type           = relation->type;
    constraint->nb_output_dims = relation->nb_output_dims;
    constraint->nb_input_dims  = relation->nb_input_dims;
    constraint->nb_local_dims  = relation->nb_local_dims;
    constraint->nb_parameters  = relation->nb_parameters;

    for (i = 0; i < relation->nb_columns; i++)
        osl_int_assign(precision, &constraint->m[0][i], relation->m[row][i]);

    return constraint;
}


/**
 * clan_relation_is_equality function:
 * this function returns 1 if a given row of a given relation corresponds
 * to an equality constraint, 0 otherwise (which means it corresponds to
 * an inequality constraint).
 * \param[in] relation The input relation.
 * \param[in] row      The row corresponding to the constraint to check.
 * \return 1 if the constraint is an equality, 0 if it is an inequality.
 */
static
int clan_relation_is_equality(osl_relation_p relation, int row) {

    return (osl_int_zero(relation->precision, relation->m[row][0])) ? 1 : 0;
}


/**
 * clan_relation_tag_inequality function:
 * this function tags a given constraint of a given relation as being an
 * inequality >=0. This means in the PolyLib format, to set to 1 the very
 * first entry of the constraint row. It modifies directly the relation
 * provided as an argument.
 * \param relation The relation which includes a constraint to be tagged.
 * \param row      The row corresponding to the constraint to tag.
 */
static
void clan_relation_tag_inequality(osl_relation_p relation, int row) {
    if ((relation == NULL) || (relation->nb_rows < row))
        CLAN_error("the constraint cannot be inquality-tagged");
    osl_int_set_si(relation->precision, &relation->m[row][0], 1);
}


/**
 * clan_relation_tag_equality function:
 * this function tags a given constraint of a given relation as being an
 * equality == 0. This means in the PolyLib format, to set to 0 the very
 * first entry of the constraint row. It modifies directly the relation
 * provided as an argument.
 * \param relation The relation which includes a constraint to be tagged.
 * \param row      The row corresponding to the constraint to tag.
 */
static
void clan_relation_tag_equality(osl_relation_p relation, int row) {
    if ((relation == NULL) || (relation->nb_rows < row))
        CLAN_error("the constraint cannot be equality-tagged");
    osl_int_set_si(relation->precision, &relation->m[row][0], 0);
}


/**
 * clan_relation_constraint_not function:
 * this function returns the negative form of one constraint in a
 * relation (seen as a constraint set).
 * \param[in] relation The relation set where is the constraint to negate.
 * \param[in] row      The row number of the constraint to negate.
 * \return A new relation containing the negation of the constraint.
 */
static
osl_relation_p clan_relation_constraint_not(osl_relation_p relation, int row) {
    osl_relation_p tmp, tmp_eq = NULL;

    if (row > relation->nb_rows)
        return NULL;

    // Extract the constraint.
    tmp = clan_relation_extract_constraint(relation, row);

    // Negate it (inequality-style): a >= 0 becomes a < 0, i.e., -a - 1 >= 0.
    clan_relation_negate_inequality(tmp, 0);

    // If the constraint is an equality we need to build an union.
    // a == 0 becomes a > 0 || a < 0, i.e., a - 1 >= 0 || -a - 1 >= 0.
    if (clan_relation_is_equality(relation, row)) {

        tmp_eq = clan_relation_extract_constraint(relation, row);
        osl_int_decrement(relation->precision,
                          &tmp_eq->m[0][tmp_eq->nb_columns - 1],
                          tmp_eq->m[0][tmp_eq->nb_columns - 1]);

        // Set the two constraints as inequalities and build the union.
        clan_relation_tag_inequality(tmp, 0);
        clan_relation_tag_inequality(tmp_eq, 0);
        tmp->next = tmp_eq;
    }

    return tmp;
}


/**
 * clan_relation_not function:
 * this function returns the negative form of a relation (union).
 * \param relation The relation to oppose.
 * \return A new relation corresponding to the negative form of the input.
 */
osl_relation_p clan_relation_not(osl_relation_p relation) {
    int i;
    osl_relation_p not_constraint;
    osl_relation_p not = NULL, part;

    while (relation != NULL) {
        // Build the negation of one relation union part.
        part = NULL;
        for (i = 0; i < relation->nb_rows; i++) {
            not_constraint = clan_relation_constraint_not(relation, i);
            osl_relation_add(&part, not_constraint);
        }

        // AND it to the previously negated parts.
        if (not == NULL) {
            not = part;
        } else {
            clan_relation_and(not, part);
            osl_relation_free(part);
        }
        relation = relation->next;
    }

    return not;
}


/**
 * clan_relation_and function:
 * this function inserts the src constraints rows into every parts of dest.
 * If src is an union, the function creates exactly the right number
 * of new unions.
 * \param dest modified relation which contains the result.
 * \param src relation to be inserted.
 */
void clan_relation_and(osl_relation_p dest, osl_relation_p src) {
    osl_relation_p next_dest,
                   next_src,
                   dup_dest,
                   next_mem = NULL;

    // initializing
    next_src = src;
    next_dest = dest;
    dup_dest = osl_relation_clone(dest);
    if (dest == NULL || src == NULL)
        return;

    // For each union
    while (next_src != NULL) {
        // Add in each unions
        while(next_dest != NULL) {
            osl_relation_insert_constraints(next_dest, next_src, next_dest->nb_rows);
            next_mem = next_dest;
            next_dest = next_dest->next;
        }
        if (next_src->next != NULL)
            next_mem->next = osl_relation_clone(dup_dest);
        else
            next_mem->next = NULL;

        // Next union
        next_src = next_src->next;
        next_dest = next_mem->next;
    }
    osl_relation_free(dup_dest);
}


/**
 * clan_relation_existential function:
 * this function returns 1 if the relation involves an existential
 * quantifier (its coefficient is not zero), 0 otherwise.
 * \param[in] relation The relation to check.
 * \return 1 if the relation uses an existential quantifier, 0 otherwise.
 */
int clan_relation_existential(osl_relation_p relation) {
    int i, j;

    while (relation != NULL) {
        for (i = 0; i < relation->nb_rows; i++) {
            for (j = CLAN_MAX_DEPTH + 1;
                    j < CLAN_MAX_DEPTH + CLAN_MAX_LOCAL_DIMS + 1;
                    j++) {
                if (!osl_int_zero(relation->precision, relation->m[i][j]))
                    return 1;
            }
        }
        relation = relation->next;
    }

    return 0;
}


/**
 * clan_relation_oppose_row function:
 * this function multiplies by -1 every element (except the
 * equality/inequality marker) of a given row in a given relation part.
 * \param[in,out] r   The relation to oppose a row.
 * \param[in]     row The row number to oppose.
 */
void clan_relation_oppose_row(osl_relation_p r, int row) {
    int i;

    if (r == NULL)
        return;

    if ((row < 0) || (row >= r->nb_rows))
        CLAN_error("bad row number");

    for (i = 1; i < r->nb_columns; i++)
        osl_int_oppose(r->precision, &r->m[row][i], r->m[row][i]);
}


/**
 * clan_relation_extract_bounding function:
 * this function separates the constraints of a given relation part
 * (not an union) into two constraints sets: one which contains the
 * constraints contributing to one bound of the loop iterator at a given
 * depth and another one which contains all the remaining constraints.
 * Equalities contributing to the bound are separated into two
 * inequalities.
 * \param[in]  r        The constraint set to separate.
 * \param[out] bound    The constraints contributing to the bound (output).
 * \param[out] notbound The constraints not contributing to the bound (output).
 * \param[in]  depth    The loop depth of the bound.
 * \param[in]  lower    1 for the lower bound, 0 for the upper bound.
 */
static
void clan_relation_extract_bounding(osl_relation_p r,
                                    osl_relation_p* bound,
                                    osl_relation_p* notbound,
                                    int depth, int lower) {
    int i, precision;
    osl_relation_p constraint;

    if (r == NULL)
        return;

    if ((depth < 1) || (depth > CLAN_MAX_DEPTH))
        CLAN_error("bad depth");

    if ((lower < 0) || (lower > 1))
        CLAN_error("lower parameter must be 0 or 1");

    // Create two empty sets bound and notbound.
    precision = r->precision;
    *bound = osl_relation_pmalloc(precision, 0, r->nb_columns);
    osl_relation_set_attributes(*bound,
                                r->nb_output_dims,
                                r->nb_input_dims,
                                r->nb_local_dims,
                                r->nb_parameters);
    *notbound = osl_relation_pmalloc(precision, 0, r->nb_columns);
    osl_relation_set_attributes(*notbound,
                                r->nb_output_dims,
                                r->nb_input_dims,
                                r->nb_local_dims,
                                r->nb_parameters);

    // For each constraint in r...
    for (i = 0; i < r->nb_rows; i++) {
        constraint = clan_relation_extract_constraint(r, i);

        if (osl_int_zero(precision, constraint->m[0][depth])) {
            // If it does not involve the loop iterator => notbound set.
            osl_relation_insert_constraints(*notbound, constraint, -1);
        } else if (osl_int_zero(precision, constraint->m[0][0])) {
            // If this is an equality, separate it into two inequalities, then
            // put one in bound and the other one in notbound conveniently.
            osl_int_set_si(precision, &constraint->m[0][0], 1);
            osl_relation_insert_constraints(*bound, constraint, -1);
            osl_relation_insert_constraints(*notbound, constraint, -1);
            if ((lower && osl_int_pos(precision, constraint->m[0][depth])) ||
                    (!lower && osl_int_neg(precision, constraint->m[0][depth]))) {
                clan_relation_oppose_row(*notbound, (*notbound)->nb_rows - 1);
            } else {
                clan_relation_oppose_row(*bound, (*bound)->nb_rows - 1);
            }
        } else {
            // If it is an inequality, drive it to the right set.
            if ((lower && osl_int_pos(precision, constraint->m[0][depth])) ||
                    (!lower && osl_int_neg(precision, constraint->m[0][depth]))) {
                osl_relation_insert_constraints(*bound, constraint, -1);
            } else {
                osl_relation_insert_constraints(*notbound, constraint, -1);
            }
        }
        osl_relation_free(constraint);
    }
}


/**
 * clan_relation_to_expressions function:
 * this function translates a set of inequalities involving the
 * coefficient of the loop iterator at depth "depth" to a set of
 * expressions which would compare to the iterator alone, hence
 * not involving the loop iterator anymore. E.g., an inequality
 * "j - i + 3 >= 0" for iterator "j" will be converted to "i - 3"
 * (j >= i - 3) and "-j - i + 3 >= 0" will be converted to "-i + 3"
 * (j <= -i + 3). If the coefficient of the iterator is not +/-1,
 * it is stored in the equality/inequality marker.
 * \param[in] r     The inequality set to convert (not an union).
 * \param[in] depth Loop depth of the iterator to remove.
 */
static
void clan_relation_to_expressions(osl_relation_p r, int depth) {
    int i, coef, mark;

    for (i = 0; i < r->nb_rows; i++) {
        mark = osl_int_get_si(r->precision, r->m[i][0]);
        coef = osl_int_get_si(r->precision, r->m[i][depth]);
        if ((mark != 1) || (coef == 0))
            CLAN_error("you found a bug");

        if (coef > 0)
            clan_relation_oppose_row(r, i);

        coef = (coef > 0) ? coef : -coef;
        if (coef > 1)
            osl_int_set_si(r->precision, &r->m[i][0], coef);
        else
            osl_int_set_si(r->precision, &r->m[i][0], 0);
        osl_int_set_si(r->precision, &r->m[i][depth], 0);
    }
}


/**
 * clan_relation_stride function:
 * this function computes and returns a relation built from an input
 * relation modified by the contribution of a loop stride at a given
 * depth. Basically, the input relation corresponds to an iteration
 * domain with a loop stride of 1 for the input depth. It returns the
 * new iteration domain when we take into account a non-unit stride at
 * this depth.
 * \param[in] r      The relation without the stride.
 * \param[in] depth  The depth of the strided loop to take into account.
 * \param[in] stride The loop stride value.
 */
osl_relation_p clan_relation_stride(osl_relation_p r, int depth, int stride) {
    int i, lower, precision;
    osl_relation_p contribution;
    osl_relation_p constraint;
    osl_relation_p bound, notbound;
    osl_relation_p part;
    osl_relation_p full = NULL;

    if (depth < 1)
        CLAN_error("invalid loop depth");
    else if (stride == 0)
        CLAN_error("unsupported zero stride");

    precision = r->precision;
    lower = (stride > 0) ? 1 : 0;
    stride = (stride > 0) ? stride : -stride;

    // Each part of the relation union will provide independent contribution.
    while (r != NULL) {
        part = NULL;

        // Separate the bounding constraints (bound) which are impacted by the
        // stride from others (notbound) which will be reinjected later.
        clan_relation_extract_bounding(r, &bound, &notbound, depth, lower);

        // Change the bounding constraints to a set of linear expressions
        // to make it easy to manipulate them through existing functions.
        clan_relation_to_expressions(bound, depth);

        // Each bound constraint contributes along with the stride.
        for (i = 0; i < bound->nb_rows; i++) {
            // -1. Extract the contributing constraint c.
            constraint = clan_relation_extract_constraint(bound, i);

            // -2. For every constaint before c, ensure the comparison at step 3
            //     will be strictly greater, by adding 1: since the different
            //     sets must be disjoint, we don't want a >= b then b >= a but
            //     a >= b then b > a to avoid a == b to be in both sets.
            //     (Resp. adding -1 for the upper case.)
            if (i > 0) {
                if (lower) {
                    osl_int_add_si(precision,
                                   &bound->m[i - 1][bound->nb_columns - 1],
                                   bound->m[i - 1][bound->nb_columns - 1], 1);
                } else {
                    osl_int_add_si(precision,
                                   &bound->m[i - 1][bound->nb_columns - 1],
                                   bound->m[i - 1][bound->nb_columns - 1], -1);
                }
            }

            // -3. Compute c > a && c > b && c >= c && c >= d ...
            //     We remove the c >= c row which corresponds to a trivial 0 >= 0.
            //     (Resp. c < a && c <b && c <= c && c <=d ... for the upper case.)
            if (lower)
                contribution = clan_relation_greater(constraint, bound, 0);
            else
                contribution = clan_relation_greater(bound, constraint, 0);
            osl_relation_remove_row(contribution, i);

            // -4. The iterator i of the current depth is i >= c (i.e., i - c >= 0).
            //     (Resp. i <= c, i.e., -i + c >= 0, for the upper case.)
            //     * 4.1 Put c at the end of the constraint set.
            osl_relation_insert_constraints(contribution, constraint, -1);
            //     * 4.2 Oppose so we have -c.
            //           (Resp. do nothing so we have c for the upper case.)
            if (lower) {
                clan_relation_oppose_row(contribution, contribution->nb_rows - 1);
            }
            //     * 4.3 Put the loop iterator so we have i - c.
            //           (Resp. -i + c for the upper case.)
            if (lower) {
                osl_int_set_si(precision,
                               &contribution->m[contribution->nb_rows - 1][depth], 1);
            } else {
                osl_int_set_si(precision,
                               &contribution->m[contribution->nb_rows - 1][depth], -1);
            }
            //     * 4.4 Set the inequality marker so we have i - c >= 0.
            //           (Resp. -i + c >= 0 for the upper case.)
            osl_int_set_si(precision,
                           &contribution->m[contribution->nb_rows - 1][0], 1);

            // -5. Add the contribution of the stride (same for lower and upper).
            //     * 5.1 Put c at the end of the constraint set.
            osl_relation_insert_constraints(contribution, constraint, -1);
            //     * 5.2 Put the opposed loop iterator so we have -i + c.
            osl_int_set_si(precision,
                           &contribution->m[contribution->nb_rows - 1][depth], -1);
            //     * 5.3 Put stride * local dimension so we have -i + c + stride*ld.
            //           The equality marker is set so we have i == c + stride*ld.
            osl_int_set_si(precision,
                           &contribution->m[contribution->nb_rows - 1]
                           [CLAN_MAX_DEPTH + 1 + clan_parser_nb_ld()], stride);

            osl_relation_free(constraint);
            osl_relation_add(&part, contribution);
        }

        // Re-inject notbound constraints
        clan_relation_and(notbound, part);
        osl_relation_free(bound);
        osl_relation_free(part);
        osl_relation_add(&full, notbound);
        r = r->next;
    }
    clan_parser_add_ld();

    return full;
}


/**
 * clan_relation_gaussian_elimination function:
 * this function eliminates the coefficients of a given column (pivot_column)
 * of a relation matrix using a given pivot (the pivot itself is not
 * eliminated). This function updates the relation directly. The correct
 * elimination of columns elements (except the pivot) is guaranteed only if
 * the pivot row corresponds to an equality. Otherwise, this fuction will do
 * its best to eliminate the other columns elements, but the resulting
 * relation will not be equivalent to the input.
 * \param[in,out] relation     Relation where to eliminate a column (modified).
 * \param[in]     pivot_row    Row coordinate of the pivot.
 * \param[in]     pivot_column Column coordinate of the pivot.
 */
static
void clan_relation_gaussian_elimination(osl_relation_p relation,
                                        int pivot_row, int pivot_column) {
    int i, j, same_sign, precision, identical;
    osl_int_p temp, pivot_coef, current_coef;

    if (relation == NULL)
        return;

    precision = relation->precision;

    if (relation->next != NULL)
        OSL_debug("gaussian elimination works only on the first part of unions");

    if ((pivot_row    >= relation->nb_rows)    || (pivot_row    < 0) ||
            (pivot_column >= relation->nb_columns) || (pivot_column < 0))
        OSL_error("bad pivot position");

    if (osl_int_zero(precision, relation->m[pivot_row][pivot_column]))
        OSL_error("pivot value is 0");

    if (!osl_int_zero(precision, relation->m[pivot_row][0]))
        OSL_warning("pivot not in an equality: non equivalent simplified relation");

    // Achieve the gaussian elimination.
    // TODO: (ndCedric) investigate the impact of converting i > 0 to i - 1 >= 0.
    //       When we multiply with some coefficients, like here, we may run into
    //       trouble. For instance, let us suppose we want to simplify N > i
    //       knowing that 2i = N. If we keep the >, we end up with N > 0. If we
    //       translate to >=, we end up with N >= 2 which is not quite the same.
    temp = osl_int_malloc(precision);
    pivot_coef = osl_int_malloc(precision);
    current_coef = osl_int_malloc(precision);
    for (i = 0; i < relation->nb_rows; i++) {
        // Do not eliminate if:
        // - The current element to eliminate is the pivot,
        // - The current element to eliminate is already zero,
        // - The pivot lies in an inequality and the element in an equality,
        // - The pivot and the current element are described with inequalities and
        //   their coefficients have the same sign (impossible to eliminate).
        same_sign = (osl_int_neg(precision, relation->m[pivot_row][pivot_column])&&
                     osl_int_neg(precision, relation->m[i][pivot_column])) ||
                    (osl_int_pos(precision, relation->m[pivot_row][pivot_column])&&
                     osl_int_pos(precision, relation->m[i][pivot_column]));
        if ((i != pivot_row) &&
                (!osl_int_zero(precision, relation->m[i][pivot_column])) &&
                (osl_int_zero(precision, relation->m[pivot_row][0]) ||
                 !osl_int_zero(precision, relation->m[i][0]))) {
            if (osl_int_zero(precision, relation->m[pivot_row][0]) ||
                    osl_int_zero(precision, relation->m[i][0]) || !same_sign) {
                // Set the values of coefficients for the pivot and the current rows:
                // - if the pivot and the current element do not have the same sign,
                //   ensure that only an equality can be multiplied by a negative coef,
                // - if the signs are different, use positive coefficients.
                osl_int_assign(precision,
                               pivot_coef, relation->m[pivot_row][pivot_column]);
                osl_int_assign(precision,
                               current_coef, relation->m[i][pivot_column]);
                if (same_sign) {
                    if (osl_int_zero(precision, relation->m[pivot_row][0])) {
                        osl_int_oppose(precision, current_coef, *current_coef);
                    } else {
                        osl_int_oppose(precision, pivot_coef, *pivot_coef);
                    }
                } else {
                    osl_int_abs(precision, pivot_coef, *pivot_coef);
                    osl_int_abs(precision, current_coef, *current_coef);
                }

                // element = pivot_coef * element + current_coef * pivot_row_element
                for (j = 1; j < relation->nb_columns; j++) {
                    osl_int_mul(precision,
                                temp, *current_coef, relation->m[pivot_row][j]);
                    osl_int_mul(precision,
                                &relation->m[i][j], *pivot_coef, relation->m[i][j]);
                    osl_int_add(precision,
                                &relation->m[i][j], relation->m[i][j], *temp);
                }
            } else {
                // In the case of two inequalities of the same sign, check whether they
                // are identical and if yes, zero the current row.
                identical = 1;
                for (j = 1; j < relation->nb_columns; j++) {
                    if (osl_int_ne(precision,
                                   relation->m[i][j], relation->m[pivot_row][j])) {
                        identical = 0;
                        break;
                    }
                }
                if (identical) {
                    for (j = 1; j < relation->nb_columns; j++)
                        osl_int_sub(precision, &relation->m[i][j],
                                    relation->m[i][j], relation->m[i][j]);
                }
            }
        }
    }
    osl_int_free(precision, temp);
    osl_int_free(precision, pivot_coef);
    osl_int_free(precision, current_coef);
}


/**
 * clan_relation_simplify_parts function:
 * this function removes some duplicated union parts in a lazy way: there
 * is no guarantee that it will remove duplicated constraints, it will
 * just try and remove trivial duplicates.
 * \param[in,out] relation The relation to simplify (modified).
 */
static
void clan_relation_simplify_parts(osl_relation_p relation) {
    osl_relation_p test, temp;

    test = relation->next;
    while (relation != NULL) {
        while (test != NULL) {
            if (osl_relation_part_equal(relation, test)) {
                temp = test;
                test = test->next;
                if (relation->next == temp)
                    relation->next = test;
                temp->next = NULL;
                osl_relation_free(temp);
            } else {
                test = test->next;
            }
        }
        relation = relation->next;
    }
}


/**
 * clan_relation_simplify function:
 * this function removes some duplicated constraints in a lazy way: there
 * is no guarantee that it will remove duplicated constraints, it will
 * just try and remove trivial duplicates. Hey, no polyhedral library
 * there, so this is just trivial stuff.
 * \param[in,out] relation The relation to simplify (modified).
 */
void clan_relation_simplify(osl_relation_p relation) {
    int i, j, k, to_eliminate, offset;
    osl_relation_p gauss, reference_gauss, reference = relation;

    gauss = osl_relation_clone(relation);
    reference_gauss = gauss;
    while (relation != NULL) {
        // First, try to eliminate columns elements by pivoting.
        for (j = 1; j < gauss->nb_columns; j++) {
            // Try to find a pivot, hence such that:
            // - the pivot is not 0,
            // - the constraint including the pivot is an equality,
            // - there is no non-zero element in the row before the pivot.

            //printf("j = %d\n", j);
            //osl_relation_dump(stdout, gauss);
            for (i = 0; i < gauss->nb_rows; i++) {
                if (!osl_int_zero(gauss->precision, gauss->m[i][j]) &&
                        osl_int_zero(gauss->precision, gauss->m[i][0])) {
                    to_eliminate = 1;
                    for (k = 1; k < j; k++)
                        if (!osl_int_zero(gauss->precision, gauss->m[i][k]))
                            to_eliminate = 0;
                    if (to_eliminate)
                        clan_relation_gaussian_elimination(gauss, i, j);
                }
            }
            //osl_relation_dump(stdout, gauss);
        }

        // Second, remove trivially duplicated rows.
        for (i = 0; i < gauss->nb_rows; i++) {
            for (k = i + 1; k < gauss->nb_rows; k++) {
                to_eliminate = 1;
                for (j = 1; j < gauss->nb_columns; j++) {
                    if (osl_int_ne(gauss->precision, gauss->m[i][j], gauss->m[k][j])) {
                        to_eliminate = 0;
                        break;
                    }
                }
                if (to_eliminate) {
                    for (j = 1; j < gauss->nb_columns; j++)
                        osl_int_sub(gauss->precision, &gauss->m[k][j],
                                    gauss->m[k][j], gauss->m[k][j]);
                }
            }
        }

        // Third, remove positive constant >= 0 constraints (e.g., 42 >= 0)
        for (i = 0; i < gauss->nb_rows; i++) {
            if (osl_int_pos(gauss->precision, gauss->m[i][gauss->nb_columns - 1]) &&
                    !osl_int_zero(gauss->precision, gauss->m[i][0])) {
                to_eliminate = 1;
                for (j = 1; j < gauss->nb_columns - 1; j++) {
                    if (!osl_int_zero(gauss->precision, gauss->m[i][j])) {
                        to_eliminate = 0;
                        break;
                    }
                }
                if (to_eliminate)
                    osl_int_sub(gauss->precision,
                                &gauss->m[i][gauss->nb_columns - 1],
                                gauss->m[i][gauss->nb_columns - 1],
                                gauss->m[i][gauss->nb_columns - 1]);
            }
        }

        // Remove the rows in the original relation which correspond to
        // zero-rows in the gauss relation since they are redundant.
        offset = 0;
        for (i = 0; i < gauss->nb_rows; i++) {
            to_eliminate = 1;
            for (j = 1; j < gauss->nb_columns; j++) {
                if (!osl_int_zero(gauss->precision, gauss->m[i][j])) {
                    to_eliminate = 0;
                    break;
                }
            }
            if (to_eliminate) {
                osl_relation_remove_row(relation, i - offset);
                offset++;
            }
        }

        gauss    = gauss->next;
        relation = relation->next;
    }
    osl_relation_free(reference_gauss);
    clan_relation_simplify_parts(reference);
}


/**
 * clan_relation_loop_context function:
 * this function adds the constraints to ensure that the loop iterator
 * initial value respects the loop condition. This set of constraints is
 * called the "loop context". Without such a context, a loop like the
 * following: for (i = 0; i > 2; i++) would just translate to the
 * constraints i >= 0 && i > 2 which is obviously wrong. Hence this
 * function computes the set of constraints of the loop condition with
 * respect to the initial value (in our example this would be 0 > 2)
 * and adds it to the loop condition constraints. This function modifies
 * the loop condition provided as input.
 * \param[in,out] condition      The set of constraints corresponding to the
 *                               loop condition (updated).
 * \param[in]     initialization Lower bound constraints.
 * \param[in]     depth          Current loop depth.
 */
void clan_relation_loop_context(osl_relation_p condition,
                                osl_relation_p initialization,
                                int depth) {
    int i, j;
    osl_relation_p contextual = NULL, temp, first_condition, new_condition;

    if ((condition == NULL) || (initialization == NULL))
        return;

    if (initialization->next != NULL)
        OSL_error("cannot compute the loop context for an initialization union");

    if (initialization->nb_columns != condition->nb_columns)
        OSL_error("imcompatible number of columns");

    for (i = 0; i < initialization->nb_rows; i++)
        if (osl_int_zero(initialization->precision, initialization->m[i][0]))
            OSL_error("no equality is allowed in the initialization relation");

    first_condition = condition;
    // For each possible initial value (e.g., in the case of a max):
    for (i = 0; i < initialization->nb_rows; i++) {
        condition = first_condition;
        // For each union part of the condition
        while (condition != NULL) {
            // Build the loop context (i.e. the loop condition where the
            // iterator is replaced by its initial value).
            temp = osl_relation_nclone(condition, 1);
            osl_relation_insert_blank_row(temp, 0);
            for (j = 0; j < temp->nb_columns; j++)
                osl_int_assign(temp->precision,
                               &temp->m[0][j], initialization->m[i][j]);
            clan_relation_tag_equality(temp, 0);
            clan_relation_gaussian_elimination(temp, 0, depth);
            osl_relation_remove_row(temp, 0);

            // Intersect the union part of the condition with its loop context.
            new_condition = osl_relation_nclone(condition, 1);
            osl_relation_insert_constraints(new_condition, temp, -1);

            osl_relation_free(temp);
            osl_relation_add(&contextual, new_condition);
            condition = condition->next;
        }
    }

    condition = first_condition;
    osl_relation_free_inside(condition);
    osl_relation_free(condition->next);

    // Replace the inside of condition.
    condition->nb_rows = contextual->nb_rows;
    condition->m = contextual->m;
    condition->next = contextual->next;

    // Free the contextual "shell".
    free(contextual);
}
