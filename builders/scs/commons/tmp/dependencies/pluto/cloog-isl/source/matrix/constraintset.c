
/**-------------------------------------------------------------------**
 **                               CLooG                               **
 **-------------------------------------------------------------------**
 **                          constraintset.c                          **
 **-------------------------------------------------------------------**
 **                    First version: april 17th 2005                 **
 **-------------------------------------------------------------------**/


/******************************************************************************
 *               CLooG : the Chunky Loop Generator (experimental)             *
 ******************************************************************************
 *                                                                            *
 * Copyright (C) 2005 Cedric Bastoul                                          *
 *                                                                            *
 * This library is free software; you can redistribute it and/or              *
 * modify it under the terms of the GNU Lesser General Public                 *
 * License as published by the Free Software Foundation; either               *
 * version 2.1 of the License, or (at your option) any later version.         *
 *                                                                            *
 * This library is distributed in the hope that it will be useful,            *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of             *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU          *
 * Lesser General Public License for more details.                            *
 *                                                                            *
 * You should have received a copy of the GNU Lesser General Public           *
 * License along with this library; if not, write to the Free Software        *
 * Foundation, Inc., 51 Franklin Street, Fifth Floor,                         *
 * Boston, MA  02110-1301  USA                                                *
 *                                                                            *
 * CLooG, the Chunky Loop Generator                                           *
 * Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                         *
 *                                                                            *
 ******************************************************************************/
/* CAUTION: the english used for comments is probably the worst you ever read,
 *          please feel free to correct and improve it !
 */


# include <stdlib.h>
# include <stdio.h>
# include <ctype.h>
#include <cloog/cloog.h>
#include <cloog/matrix/constraintset.h>


#define ALLOC(type) (type*)malloc(sizeof(type))
#define ALLOCN(type,n) (type*)malloc((n)*sizeof(type))


CloogConstraint *cloog_constraint_first(CloogConstraintSet *constraints);
CloogConstraint *cloog_constraint_next(CloogConstraint *constraint);


CloogConstraintSet *cloog_constraint_set_from_cloog_matrix(CloogMatrix *M) {
    return (CloogConstraintSet *)M;
}


void cloog_constraint_set_free(CloogConstraintSet *constraints) {
    cloog_matrix_free(&constraints->M);
}

int cloog_constraint_set_contains_level(CloogConstraintSet *constraints,
                                        int level, int nb_parameters) {
    return constraints->M.NbColumns - 2 - nb_parameters >= level;
}

/* Check if the variable at position level is defined by an
 * equality.  If so, return the row number.  Otherwise, return -1.
 *
 * If there is an equality, we can print it directly -no ambiguity-.
 * PolyLib can give more than one equality, we use just the first one
 * (this is a PolyLib problem, but all equalities are equivalent).
 */
CloogConstraint *cloog_constraint_set_defining_equality(CloogConstraintSet *constraints, int level) {
    CloogConstraint *constraint = ALLOC(CloogConstraint);
    int i;

    constraint->set = constraints;
    for (i = 0; i < constraints->M.NbRows; i++)
        if (cloog_int_is_zero(constraints->M.p[i][0]) &&
                !cloog_int_is_zero(constraints->M.p[i][level])) {
            constraint->line = &constraints->M.p[i];
            return constraint;
        }
    free(constraint);
    return cloog_constraint_invalid();
}

/* Check if the variable (e) at position level is defined by a
 * pair of inequalities
 *		 <a, i> + -m e +  <b, p> + k1 >= 0
 *		<-a, i> +  m e + <-b, p> + k2 >= 0
 * with 0 <= k1 + k2 < m
 * If so return the row number of the upper bound and set *lower
 * to the row number of the lower bound.  If not, return -1.
 *
 * If the variable at position level occurs in any other constraint,
 * then we currently return -1.  The modulo guard that we would generate
 * would still be correct, but we would also need to generate
 * guards corresponding to the other constraints, and this has not
 * been implemented yet.
 */
CloogConstraint *cloog_constraint_set_defining_inequalities(CloogConstraintSet *constraints,
        int level, CloogConstraint **lower, int nb_par) {
    int i, j, k;
    cloog_int_t m;
    CloogMatrix *matrix = &constraints->M;
    unsigned len = matrix->NbColumns - 2;
    unsigned nb_iter = len - nb_par;
    CloogConstraint *constraint;

    for (i = 0; i < matrix->NbRows; i++) {
        if (cloog_int_is_zero(matrix->p[i][level]))
            continue;
        if (cloog_int_is_zero(matrix->p[i][0]))
            return cloog_constraint_invalid();
        if (cloog_int_is_one(matrix->p[i][level]))
            return cloog_constraint_invalid();
        if (cloog_int_is_neg_one(matrix->p[i][level]))
            return cloog_constraint_invalid();
        if (cloog_seq_first_non_zero(matrix->p[i]+level+1,
                                     (1+nb_iter)-(level+1)) != -1)
            return cloog_constraint_invalid();
        for (j = i+1; j < matrix->NbRows; ++j) {
            if (cloog_int_is_zero(matrix->p[j][level]))
                continue;
            if (cloog_int_is_zero(matrix->p[j][0]))
                return cloog_constraint_invalid();
            if (cloog_int_is_one(matrix->p[j][level]))
                return cloog_constraint_invalid();
            if (cloog_int_is_neg_one(matrix->p[j][level]))
                return cloog_constraint_invalid();
            if (cloog_seq_first_non_zero(matrix->p[j]+level+1,
                                         (1+nb_iter)-(level+1)) != -1)
                return cloog_constraint_invalid();

            cloog_int_init(m);
            cloog_int_add(m, matrix->p[i][1+len], matrix->p[j][1+len]);
            if (cloog_int_is_neg(m) ||
                    cloog_int_abs_ge(m, matrix->p[i][level])) {
                cloog_int_clear(m);
                return cloog_constraint_invalid();
            }
            cloog_int_clear(m);

            if (!cloog_seq_is_neg(matrix->p[i]+1, matrix->p[j]+1,
                                  len))
                return cloog_constraint_invalid();
            for (k = j+1; k < matrix->NbRows; ++k)
                if (!cloog_int_is_zero(matrix->p[k][level]))
                    return cloog_constraint_invalid();
            *lower = ALLOC(CloogConstraint);
            constraint = ALLOC(CloogConstraint);
            (*lower)->set = constraints;
            constraint->set = constraints;
            if (cloog_int_is_pos(matrix->p[i][level])) {
                (*lower)->line = &matrix->p[i];
                constraint->line = &matrix->p[j];
            } else {
                (*lower)->line = &matrix->p[j];
                constraint->line = &matrix->p[i];
            }
            return constraint;
        }
    }
    return cloog_constraint_invalid();
}

int cloog_constraint_set_total_dimension(CloogConstraintSet *constraints) {
    return constraints->M.NbColumns - 2;
}

int cloog_constraint_set_n_iterators(CloogConstraintSet *constraint, int nb_par) {
    return cloog_constraint_set_total_dimension(constraint) - nb_par;
}

int cloog_equal_total_dimension(CloogEqualities *equal) {
    return cloog_constraint_set_total_dimension(equal->constraints);
}

int cloog_constraint_total_dimension(CloogConstraint *constraint) {
    return cloog_constraint_set_total_dimension(constraint->set);
}



/******************************************************************************
 *                        Equalities spreading functions                      *
 ******************************************************************************/


/* Equalities are stored inside a CloogMatrix data structure called "equal".
 * This matrix has (nb_scattering + nb_iterators + 1) rows (i.e. total
 * dimensions + 1, the "+ 1" is because a statement can be included inside an
 * external loop without iteration domain), and (nb_scattering + nb_iterators +
 * nb_parameters + 2) columns (all unknowns plus the scalar plus the equality
 * type). The ith row corresponds to the equality "= 0" for the ith dimension
 * iterator. The first column gives the equality type (0: no equality, then
 * EQTYPE_* -see pprint.h-). At each recursion of pprint, if an equality for
 * the current level is found, the corresponding row is updated. Then the
 * equality if it exists is used to simplify expressions (e.g. if we have
 * "i+1" while we know that "i=2", we simplify it in "3"). At the end of
 * the pprint call, the corresponding row is reset to zero.
 */

CloogEqualities *cloog_equal_alloc(int n, int nb_levels,
                                   int nb_parameters) {
    int i;
    CloogEqualities *equal = ALLOC(CloogEqualities);

    equal->constraints = cloog_constraint_set_from_cloog_matrix(
                             cloog_matrix_alloc(n, nb_levels + nb_parameters + 1));
    equal->types = ALLOCN(int, n);
    for (i = 0; i < n; ++i)
        equal->types[i] = EQTYPE_NONE;
    return equal;
}

void cloog_equal_free(CloogEqualities *equal) {
    cloog_matrix_free(&equal->constraints->M);
    free(equal->types);
    free(equal);
}

int cloog_equal_count(CloogEqualities *equal) {
    return equal->constraints->M.NbRows;
}

CloogConstraintSet *cloog_equal_constraints(CloogEqualities *equal) {
    return equal->constraints;
}


/**
 * cloog_constraint_equal_type function :
 * This function returns the type of the equality in the constraint (line) of
 * (constraints) for the element (level). An equality is 'constant' iff all
 * other factors are null except the constant one. It is a 'pure item' iff
 * it is equal or opposite to a single variable or parameter.
 * Otherwise it is an 'affine expression'.
 * For instance:
 *   i = -13 is constant, i = j, j = -M are pure items,
 *   j = 2*M, i = j+1, 2*j = M are affine expressions.
 *
 * - constraints is the matrix of constraints,
 * - level is the column number in equal of the element which is 'equal to',
 **
 * - July     3rd 2002: first version, called pprint_equal_isconstant.
 * - July     6th 2002: adaptation for the 3 types.
 * - June    15th 2005: (debug) expr = domain->Constraint[line] was evaluated
 *                      before checking if line != ONE_TIME_LOOP. Since
 *                      ONE_TIME_LOOP is -1, an invalid read was possible.
 * - October 19th 2005: Removal of the once-time-loop specific processing.
 */
static int cloog_constraint_equal_type(CloogConstraint *constraint, int level) {
    int i, one=0 ;
    cloog_int_t *expr;

    expr = *constraint->line;

    if (!cloog_int_is_one(expr[level]) && !cloog_int_is_neg_one(expr[level]))
        return EQTYPE_EXAFFINE;

    /* There is only one non null factor, and it must be +1 or -1 for
     * iterators or parameters.
     */
    for (i = 1; i <= constraint->set->M.NbColumns-2; i++)
        if (!cloog_int_is_zero(expr[i]) && (i != level)) {
            if ((!cloog_int_is_one(expr[i]) && !cloog_int_is_neg_one(expr[i])) || (one != 0))
                return EQTYPE_EXAFFINE ;
            else
                one = 1 ;
        }
    /* if the constant factor is non null, it must be alone. */
    if (one != 0) {
        if (!cloog_int_is_zero(expr[constraint->set->M.NbColumns-1]))
            return EQTYPE_EXAFFINE ;
    } else
        return EQTYPE_CONSTANT ;

    return EQTYPE_PUREITEM ;
}


int cloog_equal_type(CloogEqualities *equal, int level) {
    return equal->types[level-1];
}


/**
 * cloog_equal_update function:
 * this function updates a matrix of equalities where each row corresponds to
 * the equality "=0" of an affine expression such that the entry at column
 * "row" (="level") is not zero. This matrix is upper-triangular, except the
 * row number "level-1" which has to be updated for the matrix to be triangular.
 * This function achieves the processing.
 * - equal is the matrix to be updated,
 * - level gives the row that has to be updated (it is actually row "level-1"),
 * - nb_par is the number of parameters of the program.
 **
 * - September 20th 2005: first version.
 */
static void cloog_equal_update(CloogEqualities *equal, int level, int nb_par) {
    int i, j ;
    cloog_int_t gcd, factor_level, factor_outer, temp_level, temp_outer;

    cloog_int_init(gcd);
    cloog_int_init(temp_level);
    cloog_int_init(temp_outer);
    cloog_int_init(factor_level);
    cloog_int_init(factor_outer);

    /* For each previous level, */
    for (i=level-2; i>=0; i--) {
        /* if the corresponding iterator is inside the current equality and is equal
         * to something,
         */
        if (!cloog_int_is_zero(equal->constraints->M.p[level-1][i+1]) && equal->types[i]) {
            /* Compute the Greatest Common Divisor. */
            cloog_int_gcd(gcd, equal->constraints->M.p[level-1][i+1],
                          equal->constraints->M.p[i][i+1]);

            /* Compute the factors to apply to each row vector element. */
            cloog_int_divexact(factor_level, equal->constraints->M.p[i][i+1], gcd);
            cloog_int_divexact(factor_outer, equal->constraints->M.p[level-1][i+1], gcd);

            /* Now update the row 'level'. */
            /* - the iterators, up to level, */
            for (j = 1; j <= level; j++) {
                cloog_int_mul(temp_level, factor_level,
                              equal->constraints->M.p[level-1][j]);
                cloog_int_mul(temp_outer, factor_outer, equal->constraints->M.p[i][j]);
                cloog_int_sub(equal->constraints->M.p[level-1][j], temp_level, temp_outer);
            }
            /* - between last useful iterator (level) and the first parameter, the
             *   matrix is sparse (full of zeroes), we just do nothing there.
             * - the parameters and the scalar.
             */
            for (j = 0; j < nb_par + 1; j++) {
                cloog_int_mul(temp_level,factor_level,
                              equal->constraints->M.p[level-1]
                              [equal->constraints->M.NbColumns-j-1]);
                cloog_int_mul(temp_outer,factor_outer,
                              equal->constraints->M.p[i][equal->constraints->M.NbColumns-j-1]);
                cloog_int_sub(equal->constraints->M.p[level-1]
                              [equal->constraints->M.NbColumns-j-1],
                              temp_level,temp_outer) ;
            }
        }
    }

    /* Normalize (divide by GCD of all elements) the updated equality. */
    cloog_seq_normalize(&(equal->constraints->M.p[level-1][1]),
                        equal->constraints->M.NbColumns-1);

    cloog_int_clear(gcd);
    cloog_int_clear(temp_level);
    cloog_int_clear(temp_outer);
    cloog_int_clear(factor_level);
    cloog_int_clear(factor_outer);
}


/**
 * cloog_equal_add function:
 * This function updates the row (level-1) of the equality matrix (equal) with
 * the row that corresponds to the row (line) of the matrix (matrix).
 * - equal is the matrix of equalities,
 * - matrix is the matrix of constraints,
 * - level is the column number in matrix of the element which is 'equal to',
 * - line is the line number in matrix of the constraint we want to study,
 * - the infos structure gives the user all options on code printing and more.
 **
 * - July     2nd 2002: first version.
 * - October 19th 2005: Addition of the once-time-loop specific processing.
 */
void cloog_equal_add(CloogEqualities *equal, CloogConstraintSet *constraints,
                     int level, CloogConstraint *line, int nb_par) {
    int j;
    CloogConstraint *i = cloog_constraint_invalid();
    CloogMatrix *matrix = &constraints->M;

    /* If we are in the case of a loop running once, this means that the equality
     * comes from an inequality. Here we find this inequality.
     */
    if (!cloog_constraint_is_valid(line)) {
        for (i = cloog_constraint_first(constraints);
                cloog_constraint_is_valid(i); i = cloog_constraint_next(i))
            if ((!cloog_int_is_zero(i->line[0][0]))&& (!cloog_int_is_zero(i->line[0][level]))) {
                line = i ;

                /* Since in once-time-loops, equalities derive from inequalities, we
                 * may have to offset the values. For instance if we have 2i>=3, the
                 * equality is in fact i=2. This may happen when the level coefficient is
                 * not 1 or -1 and the scalar value is not zero. In any other case (e.g.,
                 * if the inequality is an expression including outer loop counters or
                 * parameters) the once time loop would not have been detected
                 * because of floord and ceild functions.
                 */
                if (cloog_int_ne_si(i->line[0][level],1) &&
                        cloog_int_ne_si(i->line[0][level],-1) &&
                        !cloog_int_is_zero(i->line[0][matrix->NbColumns-1])) {
                    cloog_int_t denominator;

                    cloog_int_init(denominator);
                    cloog_int_abs(denominator, i->line[0][level]);
                    cloog_int_fdiv_q(i->line[0][matrix->NbColumns-1],
                                     i->line[0][matrix->NbColumns-1], denominator);
                    cloog_int_set_si(i->line[0][level], cloog_int_sgn(i->line[0][level]));
                    cloog_int_clear(denominator);
                }

                break ;
            }
    }
    assert(cloog_constraint_is_valid(line));

    /* We update the line of equal corresponding to level:
     * - the first element gives the equality type,
     */
    equal->types[level-1] = cloog_constraint_equal_type(line, level);
    /* - the other elements corresponding to the equality itself
     *   (the iterators up to level, then the parameters and the scalar).
     */
    for (j=1; j<=level; j++)
        cloog_int_set(equal->constraints->M.p[level-1][j], line->line[0][j]);
    for (j = 0; j < nb_par + 1; j++)
        cloog_int_set(equal->constraints->M.p[level-1][equal->constraints->M.NbColumns-j-1],
                      line->line[0][line->set->M.NbColumns-j-1]);

    if (cloog_constraint_is_valid(i))
        cloog_constraint_release(line);
    cloog_equal_update(equal, level, nb_par);
}


/**
 * cloog_equal_del function :
 * This function reset the equality corresponding to the iterator (level)
 * in the equality matrix (equal).
 * - July 2nd 2002: first version.
 */
void cloog_equal_del(CloogEqualities *equal, int level) {
    equal->types[level-1] = EQTYPE_NONE;
}



/******************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/

/**
 * Function cloog_constraint_set_normalize:
 * This function will modify the constraint system in such a way that when
 * there is an equality depending on the element at level 'level', there are
 * no more (in)equalities depending on this element. For instance, try
 * test/valilache.cloog with options -f 8 -l 9, with and without the call
 * to this function. At a given moment, for the level L we will have
 * 32*P=L && L>=1 (P is a lower level), this constraint system cannot be
 * translated directly into a source code. Thus, we normalize the domain to
 * remove L from the inequalities. In our example, this leads to
 * 32*P=L && 32*P>=1, that can be transated to the code
 * if (P>=1) { L=32*P ; ... }. This function solves the DaeGon Kim bug.
 * WARNING: Remember that if there is another call to Polylib after a call to
 * this function, we have to recall this function.
 *  -June    16th 2005: first version (adaptation from URGent June-7th-2005 by
 *                      N. Vasilache).
 * - June    21rd 2005: Adaptation for GMP.
 * - November 4th 2005: Complete rewriting, simpler and faster. It is no more an
 *                      adaptation from URGent.
 */
void cloog_constraint_set_normalize(CloogConstraintSet *constraints, int level) {
    int ref, i, j ;
    cloog_int_t factor_i, factor_ref, temp_i, temp_ref, gcd;
    CloogMatrix *matrix = &constraints->M;

    if (matrix == NULL)
        return ;

    /* Don't "normalize" the constant term. */
    if (level == matrix->NbColumns-1)
        return;

    /* Let us find an equality for the current level that can be propagated. */
    for (ref=0; ref<matrix->NbRows; ref++)
        if (cloog_int_is_zero(matrix->p[ref][0]) && !cloog_int_is_zero(matrix->p[ref][level])) {
            cloog_int_init(gcd);
            cloog_int_init(temp_i);
            cloog_int_init(temp_ref);
            cloog_int_init(factor_i);
            cloog_int_init(factor_ref);

            /* Row "ref" is the reference equality, now let us find a row to simplify.*/
            for (i=ref+1; i<matrix->NbRows; i++)
                if (!cloog_int_is_zero(matrix->p[i][level])) {
                    /* Now let us set to 0 the "level" coefficient of row "j" using "ref".
                     * First we compute the factors to apply to each row vector element.
                     */
                    cloog_int_gcd(gcd, matrix->p[ref][level], matrix->p[i][level]);
                    cloog_int_divexact(factor_i, matrix->p[ref][level], gcd);
                    cloog_int_divexact(factor_ref, matrix->p[i][level], gcd);

                    /* Maybe we are simplifying an inequality: factor_i must not be <0. */
                    if (cloog_int_is_neg(factor_i)) {
                        cloog_int_abs(factor_i, factor_i);
                        cloog_int_neg(factor_ref, factor_ref);
                    }

                    /* Now update the vector. */
                    for (j=1; j<matrix->NbColumns; j++) {
                        cloog_int_mul(temp_i, factor_i, matrix->p[i][j]);
                        cloog_int_mul(temp_ref, factor_ref, matrix->p[ref][j]);
                        cloog_int_sub(matrix->p[i][j], temp_i, temp_ref);
                    }

                    /* Normalize (divide by GCD of all elements) the updated vector. */
                    cloog_seq_normalize(&(matrix->p[i][1]), matrix->NbColumns-1);
                }

            cloog_int_clear(gcd);
            cloog_int_clear(temp_i);
            cloog_int_clear(temp_ref);
            cloog_int_clear(factor_i);
            cloog_int_clear(factor_ref);
            break ;
        }
}



/**
 * cloog_constraint_set_copy function:
 * this functions builds and returns a "hard copy" (not a pointer copy) of a
 * CloogMatrix data structure.
 * - October 26th 2005: first version.
 */
CloogConstraintSet *cloog_constraint_set_copy(CloogConstraintSet *constraints) {
    int i, j ;
    CloogMatrix *copy;
    CloogMatrix *matrix = &constraints->M;

    copy = cloog_matrix_alloc(matrix->NbRows, matrix->NbColumns);

    for (i=0; i<matrix->NbRows; i++)
        for (j=0; j<matrix->NbColumns; j++)
            cloog_int_set(copy->p[i][j], matrix->p[i][j]);

    return cloog_constraint_set_from_cloog_matrix(copy);
}


/**
 * cloog_equal_vector_simplify function:
 * this function simplify an affine expression with its coefficients in
 * "vector" of length "length" thanks to an equality matrix "equal" that gives
 * for some elements of the affine expression an equality with other elements,
 * preferably constants. For instance, if the vector contains i+j+3 and the
 * equality matrix gives i=n and j=2, the vector is simplified to n+3 and is
 * returned in a new vector.
 * - vector is the array of affine expression coefficients
 * - equal is the matrix of equalities,
 * - length is the vector length,
 * - level is a level we don't want to simplify (-1 if none),
 * - nb_par is the number of parameters of the program.
 **
 * - September 20th 2005: first version.
 * - November   2nd 2005: (debug) we are simplifying inequalities, thus we are
 *                        not allowed to multiply the vector by a negative
 *                        constant.Problem found after a report of Michael
 *                        Classen.
 */
struct cloog_vec *cloog_equal_vector_simplify(CloogEqualities *equal, cloog_int_t *vector,
        int length, int level, int nb_par) {
    int i, j ;
    cloog_int_t gcd, factor_vector, factor_equal, temp_vector, temp_equal;
    struct cloog_vec *simplified;

    simplified = cloog_vec_alloc(length);
    cloog_seq_cpy(simplified->p, vector, length);

    cloog_int_init(gcd);
    cloog_int_init(temp_vector);
    cloog_int_init(temp_equal);
    cloog_int_init(factor_vector);
    cloog_int_init(factor_equal);

    /* For each non-null coefficient in the vector, */
    for (i=length-nb_par-2; i>0; i--)
        if (i != level) {
            /* if the coefficient in not null, and there exists a useful equality */
            if ((!cloog_int_is_zero(simplified->p[i])) && equal->types[i-1]) {
                /* Compute the Greatest Common Divisor. */
                cloog_int_gcd(gcd, simplified->p[i], equal->constraints->M.p[i-1][i]);

                /* Compute the factors to apply to each row vector element. */
                cloog_int_divexact(factor_vector, equal->constraints->M.p[i-1][i], gcd);
                cloog_int_divexact(factor_equal, simplified->p[i], gcd);

                /* We are simplifying an inequality: factor_vector must not be <0. */
                if (cloog_int_is_neg(factor_vector)) {
                    cloog_int_abs(factor_vector, factor_vector);
                    cloog_int_neg(factor_equal, factor_equal);
                }

                /* Now update the vector. */
                /* - the iterators, up to the current level, */
                for (j=1; j<=length-nb_par-2; j++) {
                    cloog_int_mul(temp_vector, factor_vector, simplified->p[j]);
                    cloog_int_mul(temp_equal, factor_equal, equal->constraints->M.p[i-1][j]);
                    cloog_int_sub(simplified->p[j], temp_vector, temp_equal);
                }
                /* - between last useful iterator (i) and the first parameter, the equal
                 *   matrix is sparse (full of zeroes), we just do nothing there.
                 * - the parameters and the scalar.
                 */
                for (j = 0; j < nb_par + 1; j++) {
                    cloog_int_mul(temp_vector, factor_vector, simplified->p[length-1-j]);
                    cloog_int_mul(temp_equal,factor_equal,
                                  equal->constraints->M.p[i-1][equal->constraints->M.NbColumns-j-1]);
                    cloog_int_sub(simplified->p[length-1-j],temp_vector,temp_equal) ;
                }
            }
        }

    /* Normalize (divide by GCD of all elements) the updated vector. */
    cloog_seq_normalize(&simplified->p[1], length - 1);

    cloog_int_clear(gcd);
    cloog_int_clear(temp_vector);
    cloog_int_clear(temp_equal);
    cloog_int_clear(factor_vector);
    cloog_int_clear(factor_equal);

    return simplified ;
}


/**
 * cloog_constraint_set_simplify function:
 * this function simplify all constraints inside the matrix "matrix" thanks to
 * an equality matrix "equal" that gives for some elements of the affine
 * constraint an equality with other elements, preferably constants.
 * For instance, if a row of the matrix contains i+j+3>=0 and the equality
 * matrix gives i=n and j=2, the constraint is simplified to n+3>=0. The
 * simplified constraints are returned back inside a new simplified matrix.
 * - matrix is the set of constraints to simplify,
 * - equal is the matrix of equalities,
 * - level is a level we don't want to simplify (-1 if none),
 * - nb_par is the number of parameters of the program.
 **
 * - November 4th 2005: first version.
 */
CloogConstraintSet *cloog_constraint_set_simplify(CloogConstraintSet *constraints,
        CloogEqualities *equal, int level, int nb_par) {
    int i, j, k ;
    struct cloog_vec *vector;
    CloogMatrix *simplified;
    CloogMatrix *matrix = &constraints->M;

    if (matrix == NULL)
        return NULL ;

    /* The simplified matrix is such that each row has been simplified thanks
     * tho the "equal" matrix. We allocate the memory for the simplified matrix,
     * then for each row of the original matrix, we compute the simplified
     * vector and we copy its content into the according simplified row.
     */
    simplified = cloog_matrix_alloc(matrix->NbRows, matrix->NbColumns);
    for (i=0; i<matrix->NbRows; i++) {
        vector = cloog_equal_vector_simplify(equal, matrix->p[i],
                                             matrix->NbColumns, level, nb_par);
        for (j=0; j<matrix->NbColumns; j++)
            cloog_int_set(simplified->p[i][j], vector->p[j]);

        cloog_vec_free(vector);
    }

    /* After simplification, it may happen that few constraints are the same,
     * we remove them here by replacing them with 0=0 constraints.
     */
    for (i=0; i<simplified->NbRows; i++)
        for (j=i+1; j<simplified->NbRows; j++) {
            for (k=0; k<simplified->NbColumns; k++)
                if (cloog_int_ne(simplified->p[i][k],simplified->p[j][k]))
                    break ;

            if (k == matrix->NbColumns) {
                for (k=0; k<matrix->NbColumns; k++)
                    cloog_int_set_si(simplified->p[j][k],0);
            }
        }

    return cloog_constraint_set_from_cloog_matrix(simplified);
}


/**
 * Return clast_expr corresponding to the variable "level" (1 based) in
 * the given constraint.
 */
struct clast_expr *cloog_constraint_variable_expr(CloogConstraint *constraint,
        int level, CloogNames *names) {
    int total_dim, nb_iter;
    const char *name;

    total_dim = cloog_constraint_total_dimension(constraint);
    nb_iter = total_dim - names->nb_parameters;

    if (level <= nb_iter)
        name = cloog_names_name_at_level(names, level);
    else
        name = names->parameters[level - (nb_iter+1)] ;

    return &new_clast_name(name)->expr;
}


/**
 * Return true if constraint c involves variable v (zero-based).
 */
int cloog_constraint_involves(CloogConstraint *constraint, int v) {
    return !cloog_int_is_zero(constraint->line[0][1+v]);
}

int cloog_constraint_is_lower_bound(CloogConstraint *constraint, int v) {
    return cloog_int_is_pos(constraint->line[0][1+v]);
}

int cloog_constraint_is_upper_bound(CloogConstraint *constraint, int v) {
    return cloog_int_is_neg(constraint->line[0][1+v]);
}

int cloog_constraint_is_equality(CloogConstraint *constraint) {
    return cloog_int_is_zero(constraint->line[0][0]);
}

void cloog_constraint_clear(CloogConstraint *constraint) {
    int k;

    for (k = 1; k <= constraint->set->M.NbColumns - 2; k++)
        cloog_int_set_si(constraint->line[0][k], 0);
}

CloogConstraintSet *cloog_constraint_set_drop_constraint(
    CloogConstraintSet *constraints, CloogConstraint *constraint) {
    cloog_constraint_clear(constraint);
    return constraints;
}

void cloog_constraint_coefficient_get(CloogConstraint *constraint,
                                      int var, cloog_int_t *val) {
    cloog_int_set(*val, constraint->line[0][1+var]);
}

void cloog_constraint_coefficient_set(CloogConstraint *constraint,
                                      int var, cloog_int_t val) {
    cloog_int_set(constraint->line[0][1+var], val);
}

void cloog_constraint_constant_get(CloogConstraint *constraint, cloog_int_t *val) {
    cloog_int_set(*val, constraint->line[0][constraint->set->M.NbColumns-1]);
}

/**
 * Copy the coefficient of constraint c into dst in PolyLib order,
 * i.e., first the coefficients of the variables, then the coefficients
 * of the parameters and finally the constant.
 */
void cloog_constraint_copy_coefficients(CloogConstraint *constraint,
                                        cloog_int_t *dst) {
    cloog_seq_cpy(dst, constraint->line[0]+1, constraint->set->M.NbColumns-1);
}

CloogConstraint *cloog_constraint_invalid(void) {
    return NULL;
}

int cloog_constraint_is_valid(CloogConstraint *constraint) {
    return constraint != NULL;
}


/**
 * Check whether there is any need for the constraint "upper" on
 * "level" to get reduced.
 * Yes.
 */
int cloog_constraint_needs_reduction(CloogConstraint *upper, int level) {
    return 1;
}


/**
 * Create a CloogConstraintSet containing enough information to perform
 * a reduction on the upper equality (in this case lower is an invalid
 * CloogConstraint) or the pair of inequalities upper and lower
 * from within insert_modulo_guard.
 * In the PolyLib backend, we return a CloogConstraintSet containting only
 * the upper bound.  The reduction will not change the stride so there
 * will be no need to recompute the bound on the modulo expression.
 */
CloogConstraintSet *cloog_constraint_set_for_reduction(CloogConstraint *upper,
        CloogConstraint *lower) {
    CloogConstraintSet *set;

    set = cloog_constraint_set_from_cloog_matrix(
              cloog_matrix_alloc(1, upper->set->M.NbColumns));
    cloog_seq_cpy(set->M.p[0], upper->line[0], set->M.NbColumns);
    return set;
}


/* Computes x, y and g such that g = gcd(a,b) and a*x+b*y = g */
static void Euclid(cloog_int_t a, cloog_int_t b,
                   cloog_int_t *x, cloog_int_t *y, cloog_int_t *g) {
    cloog_int_t c, d, e, f, tmp;

    cloog_int_init(c);
    cloog_int_init(d);
    cloog_int_init(e);
    cloog_int_init(f);
    cloog_int_init(tmp);
    cloog_int_abs(c, a);
    cloog_int_abs(d, b);
    cloog_int_set_si(e, 1);
    cloog_int_set_si(f, 0);
    while (cloog_int_is_pos(d)) {
        cloog_int_tdiv_q(tmp, c, d);
        cloog_int_mul(tmp, tmp, f);
        cloog_int_sub(e, e, tmp);
        cloog_int_tdiv_q(tmp, c, d);
        cloog_int_mul(tmp, tmp, d);
        cloog_int_sub(c, c, tmp);
        cloog_int_swap(c, d);
        cloog_int_swap(e, f);
    }
    cloog_int_set(*g, c);
    if (cloog_int_is_zero(a))
        cloog_int_set_si(*x, 0);
    else if (cloog_int_is_pos(a))
        cloog_int_set(*x, e);
    else cloog_int_neg(*x, e);
    if (cloog_int_is_zero(b))
        cloog_int_set_si(*y, 0);
    else {
        cloog_int_mul(tmp, a, *x);
        cloog_int_sub(tmp, c, tmp);
        cloog_int_divexact(*y, tmp, b);
    }
    cloog_int_clear(c);
    cloog_int_clear(d);
    cloog_int_clear(e);
    cloog_int_clear(f);
    cloog_int_clear(tmp);
}

/**
 * Reduce the modulo guard expressed by "contraints" using equalities
 * found in outer nesting levels (stored in "equal").
 * The modulo guard may be an equality or a pair of inequalities.
 * In case of a pair of inequalities, "constraints" only contains the
 * upper bound and *bound contains the bound on the
 * corresponding modulo expression.  The bound is left untouched by
 * this function.
 */
CloogConstraintSet *cloog_constraint_set_reduce(CloogConstraintSet *constraints,
        int level, CloogEqualities *equal, int nb_par, cloog_int_t *bound) {
    int i, j, k, len, len2, nb_iter;
    struct cloog_vec *line_vector2;
    cloog_int_t *line, *line2, val, x, y, g;

    len = constraints->M.NbColumns;
    len2 = cloog_equal_total_dimension(equal) + 2;
    nb_iter = len - 2 - nb_par;

    cloog_int_init(val);
    cloog_int_init(x);
    cloog_int_init(y);
    cloog_int_init(g);

    line_vector2 = cloog_vec_alloc(len2);
    line2 = line_vector2->p;

    line = constraints->M.p[0];
    if (cloog_int_is_pos(line[level]))
        cloog_seq_neg(line+1, line+1, len-1);
    cloog_int_neg(line[level], line[level]);
    assert(cloog_int_is_pos(line[level]));

    for (i = nb_iter; i >= 1; --i) {
        if (i == level)
            continue;
        cloog_int_fdiv_r(line[i], line[i], line[level]);
        if (cloog_int_is_zero(line[i]))
            continue;

        /* Look for an earlier variable that is also a multiple of line[level]
         * and check whether we can use the corresponding affine expression
         * to "reduce" the modulo guard, where reduction means that we eliminate
         * a variable, possibly at the expense of introducing other variables
         * with smaller index.
         */
        for (j = level-1; j >= 0; --j) {
            CloogConstraint *equal_constraint;
            if (cloog_equal_type(equal, j+1) != EQTYPE_EXAFFINE)
                continue;
            equal_constraint = cloog_equal_constraint(equal, j);
            cloog_constraint_coefficient_get(equal_constraint, j, &val);
            if (!cloog_int_is_divisible_by(val, line[level])) {
                cloog_constraint_release(equal_constraint);
                continue;
            }
            cloog_constraint_coefficient_get(equal_constraint, i-1, &val);
            if (cloog_int_is_divisible_by(val, line[level])) {
                cloog_constraint_release(equal_constraint);
                continue;
            }
            for (k = j; k > i; --k) {
                cloog_constraint_coefficient_get(equal_constraint, k-1, &val);
                if (cloog_int_is_zero(val))
                    continue;
                if (!cloog_int_is_divisible_by(val, line[level]))
                    break;
            }
            if (k > i) {
                cloog_constraint_release(equal_constraint);
                continue;
            }
            cloog_constraint_coefficient_get(equal_constraint, i-1, &val);
            Euclid(val, line[level], &x, &y, &g);
            if (!cloog_int_is_divisible_by(val, line[i])) {
                cloog_constraint_release(equal_constraint);
                continue;
            }
            cloog_int_divexact(val, line[i], g);
            cloog_int_neg(val, val);
            cloog_int_mul(val, val, x);
            cloog_int_set_si(y, 1);
            /* Add (equal->p[j][i])^{-1} * line[i] times the equality */
            cloog_constraint_copy_coefficients(equal_constraint, line2+1);
            cloog_seq_combine(line+1, y, line+1, val, line2+1, i);
            cloog_seq_combine(line+len-nb_par-1, y, line+len-nb_par-1,
                              val, line2+len2-nb_par-1, nb_par+1);
            cloog_constraint_release(equal_constraint);
            break;
        }
    }

    cloog_vec_free(line_vector2);

    cloog_int_clear(val);
    cloog_int_clear(x);
    cloog_int_clear(y);
    cloog_int_clear(g);

    /* Make sure the line is not inverted again in the calling function. */
    cloog_int_neg(line[level], line[level]);

    return constraints;
}

CloogConstraint *cloog_constraint_first(CloogConstraintSet *constraints) {
    CloogConstraint *c;
    if (constraints->M.NbRows == 0)
        return cloog_constraint_invalid();
    c = ALLOC(CloogConstraint);
    c->set = constraints;
    c->line = &constraints->M.p[0];
    return c;
}

CloogConstraint *cloog_constraint_next(CloogConstraint *constraint) {
    constraint->line++;
    if (constraint->line == constraint->set->M.p + constraint->set->M.NbRows) {
        cloog_constraint_release(constraint);
        return NULL;
    }
    return constraint;
}

CloogConstraint *cloog_constraint_copy(CloogConstraint *constraint) {
    CloogConstraint *c = ALLOC(CloogConstraint);
    c->set = constraint->set;
    c->line = constraint->line;
    return c;
}

void cloog_constraint_release(CloogConstraint *constraint) {
    free(constraint);
}

int cloog_constraint_set_foreach_constraint(CloogConstraintSet *constraints,
        int (*fn)(CloogConstraint *constraint, void *user), void *user) {
    CloogConstraint *c;

    for (c = cloog_constraint_first(constraints);
            cloog_constraint_is_valid(c); c = cloog_constraint_next(c))
        if (fn(c, user) < 0) {
            cloog_constraint_release(c);
            return -1;
        }

    return 0;
}

CloogConstraint *cloog_equal_constraint(CloogEqualities *equal, int j) {
    CloogConstraint *c = ALLOC(CloogConstraint);
    c->set = equal->constraints;
    c->line = &equal->constraints->M.p[j];
    return c;
}
