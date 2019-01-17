#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <cloog/isl/cloog.h>
#include <cloog/isl/backend.h>
#include <isl/aff.h>
#include <isl/set.h>
#include <isl/val.h>

#if ISL_USING_GMP
#include <isl/val_gmp.h>
#endif

#define ALLOC(type) (type*)malloc(sizeof(type))
#define ALLOCN(type,n) (type*)malloc((n)*sizeof(type))

__isl_give isl_val *cloog_int_to_isl_val(isl_ctx* ctx, cloog_int_t c) {
    isl_val *v;
#if defined(CLOOG_INT_INT)
    v = isl_val_int_from_si(ctx, c);
#elif defined(CLOOG_INT_LONG)
    v = isl_val_int_from_si(ctx, c);
#elif defined(CLOOG_INT_LONG_LONG)
    v = isl_val_int_from_si(ctx, c);
#elif defined(CLOOG_INT_GMP)
#if ISL_USING_GMP // ISL using GMP
    v = isl_val_int_from_gmp(ctx, c);
#else // ISL using iMath or iMath-32
    // The best way to ensure full precision is to go through strings!!!
    char* str = NULL;
    str = mpz_get_str(str, 10, c);
    v = isl_val_read_from_str(ctx, str);
    free(str);
#endif
#else
#error "No integer type defined"
#endif
    return v;
}

/*
 * CLooG'll be dealing in integers so we expect numerator/1 form
 * from isl_val. Thus get numerator to assign to cloog_int
 */
void isl_val_to_cloog_int(__isl_keep isl_val *val, cloog_int_t *cint) {
    assert(isl_val_is_int(val));
#if defined(CLOOG_INT_INT)
    *cint = isl_val_get_num_si(val);
#elif defined(CLOOG_INT_LONG)
    *cint = isl_val_get_num_si(val);
#elif defined(CLOOG_INT_LONG_LONG)
    *cint = isl_val_get_num_si(val);
#elif defined(CLOOG_INT_GMP)
#if ISL_USING_GMP
    isl_val_get_num_gmp(val, *cint);
#else
    isl_printer *string_printer = isl_printer_to_str(isl_val_get_ctx(val));
    isl_printer_print_val(string_printer, val);
    char *str = isl_printer_get_str(string_printer);
    mpz_set_str(*cint, str, 10);
    isl_printer_free(string_printer);
    free(str);
#endif
#else
#error "No integer type defined"
#endif
}


CloogConstraintSet *cloog_constraint_set_from_isl_basic_set(struct isl_basic_set *bset) {
    return (CloogConstraintSet *)bset;
}

CloogConstraint *cloog_constraint_from_isl_constraint(struct isl_constraint *constraint) {
    return (CloogConstraint *)constraint;
}

isl_constraint *cloog_constraint_to_isl(CloogConstraint *constraint) {
    return (isl_constraint *)constraint;
}

isl_basic_set *cloog_constraints_set_to_isl(CloogConstraintSet *constraints) {
    return (isl_basic_set *)constraints;
}


/******************************************************************************
 *                             Memory leaks hunting                           *
 ******************************************************************************/



void cloog_constraint_set_free(CloogConstraintSet *constraints) {
    isl_basic_set_free(cloog_constraints_set_to_isl(constraints));
}


int cloog_constraint_set_contains_level(CloogConstraintSet *constraints,
                                        int level, int nb_parameters) {
    isl_basic_set *bset;
    bset = cloog_constraints_set_to_isl(constraints);
    return isl_basic_set_dim(bset, isl_dim_set) >= level;
}

struct cloog_isl_dim {
    enum isl_dim_type type;
    int		  pos;
};

static struct cloog_isl_dim basic_set_cloog_dim_to_isl_dim(
    __isl_keep isl_basic_set *bset, int pos) {
    enum isl_dim_type types[] = { isl_dim_set, isl_dim_div, isl_dim_param };
    int i;
    struct cloog_isl_dim ci_dim;

    for (i = 0; i < 3; ++i) {
        unsigned dim = isl_basic_set_dim(bset, types[i]);
        if (pos < dim) {
            ci_dim.type = types[i];
            ci_dim.pos = pos;
            return ci_dim;
        }
        pos -= dim;
    }
    assert(0);
}

static struct cloog_isl_dim set_cloog_dim_to_isl_dim(
    CloogConstraintSet *constraints, int pos) {
    isl_basic_set *bset;

    bset = cloog_constraints_set_to_isl(constraints);
    return basic_set_cloog_dim_to_isl_dim(bset, pos);
}

/* Check if the variable at position level is defined by an
 * equality.  If so, return the row number.  Otherwise, return -1.
 */
CloogConstraint *cloog_constraint_set_defining_equality(
    CloogConstraintSet *constraints, int level) {
    struct isl_constraint *c;
    struct cloog_isl_dim dim;
    isl_basic_set *bset;

    bset = cloog_constraints_set_to_isl(constraints);
    dim = set_cloog_dim_to_isl_dim(constraints, level - 1);
    if (isl_basic_set_has_defining_equality(bset, dim.type, dim.pos, &c))
        return cloog_constraint_from_isl_constraint(c);
    else
        return NULL;
}


struct cloog_isl_other {
    int level;
    int found;
    isl_constraint *u;
    isl_constraint *l;
};


/* Set other->found to 1 if the given constraint involves other->level
 * and is different from other->u and other->l.
 */
static int check_other_constraint(__isl_take isl_constraint *c, void *user) {
    struct cloog_isl_other *other = user;
    CloogConstraint *cc;

    if (!isl_constraint_is_equal(c, other->l) &&
            !isl_constraint_is_equal(c, other->u)) {
        cc = cloog_constraint_from_isl_constraint(c);
        if (cloog_constraint_involves(cc, other->level - 1))
            other->found = 1;
    }

    isl_constraint_free(c);

    return other->found ? -1 : 0;
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
CloogConstraint *cloog_constraint_set_defining_inequalities(
    CloogConstraintSet *constraints,
    int level, CloogConstraint **lower, int nb_par) {
    struct isl_constraint *u;
    struct isl_constraint *l;
    struct cloog_isl_dim dim;
    struct isl_basic_set *bset;
    struct cloog_isl_other other;

    bset = cloog_constraints_set_to_isl(constraints);
    dim = set_cloog_dim_to_isl_dim(constraints, level - 1);
    if (!isl_basic_set_has_defining_inequalities(bset, dim.type, dim.pos,
            &l, &u))
        return cloog_constraint_invalid();

    other.l = l;
    other.u = u;
    other.found = 0;
    other.level = level;
    isl_basic_set_foreach_constraint(bset, &check_other_constraint, &other);
    if (other.found) {
        isl_constraint_free(l);
        isl_constraint_free(u);
        *lower = NULL;
        return NULL;
    }
    *lower = cloog_constraint_from_isl_constraint(l);
    return cloog_constraint_from_isl_constraint(u);
}

int cloog_constraint_set_total_dimension(CloogConstraintSet *constraints) {
    isl_basic_set *bset;
    bset = cloog_constraints_set_to_isl(constraints);
    return isl_basic_set_total_dim(bset);
}

int cloog_constraint_set_n_iterators(CloogConstraintSet *constraints, int n_par) {
    isl_basic_set *bset;
    bset = cloog_constraints_set_to_isl(constraints);
    return isl_basic_set_dim(bset, isl_dim_set);
}


/******************************************************************************
 *                        Equalities spreading functions                      *
 ******************************************************************************/


/* Equalities are stored inside a Matrix data structure called "equal".
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

CloogEqualities *cloog_equal_alloc(int n, int nb_levels, int nb_parameters) {
    int i;
    CloogEqualities *equal = ALLOC(CloogEqualities);

    equal->total_dim = nb_levels - 1 + nb_parameters;
    equal->n = n;
    equal->constraints = ALLOCN(isl_constraint *, n);
    equal->types = ALLOCN(int, n);
    for (i = 0; i < n; ++i) {
        equal->constraints[i] = NULL;
        equal->types[i] = EQTYPE_NONE;
    }
    return equal;
}

int cloog_equal_total_dimension(CloogEqualities *equal) {
    return equal->total_dim;
}

void cloog_equal_free(CloogEqualities *equal) {
    int i;

    for (i = 0; i < equal->n; ++i)
        isl_constraint_free(equal->constraints[i]);
    free(equal->constraints);
    free(equal->types);
    free(equal);
}

int cloog_equal_count(CloogEqualities *equal) {
    return equal->n;
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
 */
static int cloog_constraint_equal_type(CloogConstraint *cc, int level) {
    int i;
    isl_val *c;
    int type = EQTYPE_NONE;
    struct isl_constraint *constraint = cloog_constraint_to_isl(cc);

    c = isl_constraint_get_constant_val(constraint);
    if (!isl_val_is_zero(c))
        type = EQTYPE_CONSTANT;
    isl_val_free(c);
    c = isl_constraint_get_coefficient_val(constraint, isl_dim_set, level - 1);
    if (!isl_val_is_one(c) && !isl_val_is_negone(c))
        type = EQTYPE_EXAFFINE;
    isl_val_free(c);
    for (i = 0; i < isl_constraint_dim(constraint, isl_dim_param); ++i) {
        c = isl_constraint_get_coefficient_val(constraint, isl_dim_param, i);
        if (isl_val_is_zero(c)) {
            isl_val_free(c);
            continue;
        }
        if ((!isl_val_is_one(c) && !isl_val_is_negone(c)) ||
                type != EQTYPE_NONE) {
            type = EQTYPE_EXAFFINE;
            isl_val_free(c);
            break;
        }
        type = EQTYPE_PUREITEM;
        isl_val_free(c);
    }
    for (i = 0; i < isl_constraint_dim(constraint, isl_dim_set); ++i) {
        if (i == level - 1)
            continue;
        c = isl_constraint_get_coefficient_val(constraint, isl_dim_set, i);
        if (isl_val_is_zero(c)) {
            isl_val_free(c);
            continue;
        }
        if ((!isl_val_is_one(c) && !isl_val_is_negone(c)) ||
                type != EQTYPE_NONE) {
            type = EQTYPE_EXAFFINE;
            isl_val_free(c);
            break;
        }
        type = EQTYPE_PUREITEM;
        isl_val_free(c);
    }
    for (i = 0; i < isl_constraint_dim(constraint, isl_dim_div); ++i) {
        c = isl_constraint_get_coefficient_val(constraint, isl_dim_div, i);
        if (isl_val_is_zero(c)) {
            isl_val_free(c);
            continue;
        }
        if ((!isl_val_is_one(c) && !isl_val_is_negone(c)) ||
                type != EQTYPE_NONE) {
            type = EQTYPE_EXAFFINE;
            isl_val_free(c);
            break;
        }
        type = EQTYPE_PUREITEM;
        isl_val_free(c);
    }

    if (type == EQTYPE_NONE)
        type = EQTYPE_CONSTANT;

    return type;
}


int cloog_equal_type(CloogEqualities *equal, int level) {
    return equal->types[level-1];
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
 * line is set to an invalid constraint for equalities that CLooG itself has
 * discovered because the lower and upper bound of a loop happened to be equal.
 * This situation shouldn't happen in the isl port since isl should
 * have found the equality itself.
 */
void cloog_equal_add(CloogEqualities *equal, CloogConstraintSet *matrix,
                     int level, CloogConstraint *line, int nb_par) {
    isl_constraint *c;
    assert(cloog_constraint_is_valid(line));

    equal->types[level-1] = cloog_constraint_equal_type(line, level);
    c = cloog_constraint_to_isl(line);
    equal->constraints[level - 1] = isl_constraint_copy(c);
}


/**
 * cloog_equal_del function :
 * This function reset the equality corresponding to the iterator (level)
 * in the equality matrix (equal).
 * - July 2nd 2002: first version.
 */
void cloog_equal_del(CloogEqualities *equal, int level) {
    equal->types[level-1] = EQTYPE_NONE;
    isl_constraint_free(equal->constraints[level - 1]);
    equal->constraints[level-1] = NULL;
}



/******************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/

/**
 * Function cloog_constraint_set_normalize:
 * This function will modify the constraint system in such a way that when
 * there is an equality depending on the element at level 'level', there are
 * no more (in)equalities depending on this element.
 *
 * The simplified form of isl automatically satisfies this condition.
 */
void cloog_constraint_set_normalize(CloogConstraintSet *matrix, int level) {
}



/**
 * cloog_constraint_set_copy function:
 * this functions builds and returns a "soft copy" of a CloogConstraintSet data
 * structure.
 *
 * NOTE: this function used to return a "hard copy" (not a pointer copy) but isl
 * doesn't provide isl_basic_set_dup() anymore and a soft copy works as well.
 */
CloogConstraintSet *cloog_constraint_set_copy(CloogConstraintSet *constraints) {
    isl_basic_set *bset;
    bset = cloog_constraints_set_to_isl(constraints);
    return cloog_constraint_set_from_isl_basic_set(isl_basic_set_copy(bset));
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
 * isl should have performed these simplifications already in isl_set_gist.
 */
CloogConstraintSet *cloog_constraint_set_simplify(CloogConstraintSet *matrix,
        CloogEqualities *equal, int level, int nb_par) {
    return cloog_constraint_set_copy(matrix);
}


static struct cloog_isl_dim constraint_cloog_dim_to_isl_dim(
    CloogConstraint *constraint, int pos) {
    enum isl_dim_type types[] = { isl_dim_set, isl_dim_div, isl_dim_param };
    int i;
    struct cloog_isl_dim ci_dim;

    for (i = 0; i < 3; ++i) {
        isl_constraint *c = cloog_constraint_to_isl(constraint);
        unsigned dim = isl_constraint_dim(c, types[i]);
        if (pos < dim) {
            ci_dim.type = types[i];
            ci_dim.pos = pos;
            return ci_dim;
        }
        pos -= dim;
    }
    assert(0);
}

static struct clast_expr *div_expr(CloogConstraint *constraint, int pos,
                                   CloogNames *names) {
    int i, nb_elts;
    unsigned dim = cloog_constraint_total_dimension(constraint);
    isl_val *c;
    struct clast_reduction *r;
    struct clast_expr *e = NULL;
    isl_aff *div;
    cloog_int_t cint;

    cloog_int_init(cint);
    div = isl_constraint_get_div(cloog_constraint_to_isl(constraint), pos);

    for (i = 0, nb_elts = 0; i < dim; ++i) {
        struct cloog_isl_dim dim;

        dim = constraint_cloog_dim_to_isl_dim(constraint, i);
        if (dim.type == isl_dim_set)
            dim.type = isl_dim_in;
        c = isl_aff_get_coefficient_val(div, dim.type, dim.pos);
        if (!isl_val_is_zero(c))
            ++nb_elts;

        isl_val_free(c);
    }
    c = isl_aff_get_constant_val(div);
    if (!isl_val_is_zero(c))
        ++nb_elts;
    isl_val_free(c);

    r = new_clast_reduction(clast_red_sum, nb_elts);
    for (i = 0, nb_elts = 0; i < dim; ++i) {
        struct clast_expr *v;
        struct cloog_isl_dim dim;

        dim = constraint_cloog_dim_to_isl_dim(constraint, i);
        if (dim.type == isl_dim_set)
            dim.type = isl_dim_in;
        c = isl_aff_get_coefficient_val(div, dim.type, dim.pos);
        if (isl_val_is_zero(c)) {
            isl_val_free(c);
            continue;
        }

        v = cloog_constraint_variable_expr(constraint, 1 + i, names);

        /* We are interested only in the numerator */
        cloog_int_set_si(cint, isl_val_get_num_si(c));
        r->elts[nb_elts++] = &new_clast_term(cint, v)->expr;

        isl_val_free(c);
    }

    c = isl_aff_get_constant_val(div);
    if (!isl_val_is_zero(c)) {
        /* We are interested only in the numerator */
        cloog_int_set_si(cint, isl_val_get_num_si(c));
        r->elts[nb_elts++] = &new_clast_term(cint, NULL)->expr;
    }
    isl_val_free(c);

    c = isl_aff_get_denominator_val(div);
    isl_val_to_cloog_int(c, &cint);
    isl_val_free(c);
    e = &new_clast_binary(clast_bin_fdiv, &r->expr, cint)->expr;

    cloog_int_clear(cint);

    isl_aff_free(div);

    return e;
}

/**
 * Return clast_expr corresponding to the variable "level" (1 based) in
 * the given constraint.
 */
struct clast_expr *cloog_constraint_variable_expr(CloogConstraint *constraint,
        int level, CloogNames *names) {
    struct cloog_isl_dim dim;
    const char *name;

    assert(constraint);

    dim = constraint_cloog_dim_to_isl_dim(constraint, level - 1);
    if (dim.type == isl_dim_div)
        return div_expr(constraint, dim.pos, names);

    if (dim.type == isl_dim_set)
        name = cloog_names_name_at_level(names, level);
    else
        name = names->parameters[dim.pos];

    return &new_clast_name(name)->expr;
}


/**
 * Return true if constraint c involves variable v (zero-based).
 */
int cloog_constraint_involves(CloogConstraint *constraint, int v) {
    isl_val *c;
    int res;

    c = cloog_constraint_coefficient_get_val(constraint, v);
    res = !isl_val_is_zero(c);
    isl_val_free(c);
    return res;
}

int cloog_constraint_is_lower_bound(CloogConstraint *constraint, int v) {
    isl_val *c;
    int res;

    c = cloog_constraint_coefficient_get_val(constraint, v);
    res = isl_val_is_pos(c);
    isl_val_free(c);
    return res;
}

int cloog_constraint_is_upper_bound(CloogConstraint *constraint, int v) {
    isl_val *c;
    int res;

    c = cloog_constraint_coefficient_get_val(constraint, v);
    res = isl_val_is_neg(c);
    isl_val_free(c);
    return res;
}

int cloog_constraint_is_equality(CloogConstraint *constraint) {
    return isl_constraint_is_equality(cloog_constraint_to_isl(constraint));
}

CloogConstraintSet *cloog_constraint_set_drop_constraint(
    CloogConstraintSet *constraints, CloogConstraint *constraint) {
    isl_basic_set *bset;
    isl_constraint *c;

    bset = cloog_constraints_set_to_isl(constraints);
    c = cloog_constraint_to_isl(cloog_constraint_copy(constraint));
    bset = isl_basic_set_drop_constraint(bset, c);
    return cloog_constraint_set_from_isl_basic_set(bset);
}

void cloog_constraint_coefficient_get(CloogConstraint *constraint,
                                      int var, cloog_int_t *val) {
    struct cloog_isl_dim dim;
    isl_constraint *c;
    isl_val *ival;

    if (!constraint)
        val = NULL;

    dim = constraint_cloog_dim_to_isl_dim(constraint, var);
    c = cloog_constraint_to_isl(constraint);
    ival = isl_constraint_get_coefficient_val(c, dim.type, dim.pos);

    isl_val_to_cloog_int(ival, val);
    isl_val_free(ival);
}

isl_val *cloog_constraint_coefficient_get_val(CloogConstraint *constraint,
        int var) {
    struct cloog_isl_dim dim;
    isl_constraint *c;
    isl_val *val;

    if (!constraint)
        return NULL;

    dim = constraint_cloog_dim_to_isl_dim(constraint, var);
    c = cloog_constraint_to_isl(constraint);
    val = isl_constraint_get_coefficient_val(c, dim.type, dim.pos);
    return val;
}



void cloog_constraint_coefficient_set(CloogConstraint *constraint,
                                      int var, cloog_int_t val) {
    struct cloog_isl_dim dim;
    isl_constraint *c;

    assert(constraint);

    dim = constraint_cloog_dim_to_isl_dim(constraint, var);
    c = cloog_constraint_to_isl(constraint);
    isl_constraint_set_coefficient_val(c, dim.type, dim.pos,
                                       cloog_int_to_isl_val(isl_constraint_get_ctx(c), val));
}

void cloog_constraint_constant_get(CloogConstraint *constraint, cloog_int_t *val) {
    isl_val *ival;
    ival = isl_constraint_get_constant_val(cloog_constraint_to_isl(constraint));
    isl_val_to_cloog_int(ival, val);
    isl_val_free(ival);
}


__isl_give isl_val *cloog_constraint_constant_get_val(CloogConstraint *constraint) {
    return isl_constraint_get_constant_val(cloog_constraint_to_isl(constraint));
}



/**
 * Copy the coefficient of constraint c into dst in PolyLib order,
 * i.e., first the coefficients of the variables, then the coefficients
 * of the parameters and finally the constant.
 */
void cloog_constraint_copy_coefficients(CloogConstraint *constraint,
                                        cloog_int_t *dst) {
    int i;
    unsigned dim;

    dim = cloog_constraint_total_dimension(constraint);

    for (i = 0; i < dim; ++i)
        cloog_constraint_coefficient_get(constraint, i, dst+i);
    cloog_constraint_constant_get(constraint, dst+dim);
}

CloogConstraint *cloog_constraint_invalid(void) {
    return NULL;
}

int cloog_constraint_is_valid(CloogConstraint *constraint) {
    return constraint != NULL;
}

int cloog_constraint_total_dimension(CloogConstraint *constraint) {
    isl_constraint *c;
    c = cloog_constraint_to_isl(constraint);
    return isl_constraint_dim(c, isl_dim_all);
}


/**
 * Check whether there is any need for the constraint "upper" on
 * "level" to get reduced.
 * In case of the isl backend, there should be no need to do so
 * if the level corresponds to an existentially quantified variable.
 * Moreover, the way reduction is performed does not work for such
 * variables since its position might chance during the construction
 * of a set for reduction.
 */
int cloog_constraint_needs_reduction(CloogConstraint *upper, int level) {
    isl_basic_set *bset;
    isl_constraint *c;
    struct cloog_isl_dim dim;

    c = cloog_constraint_to_isl(upper);
    bset = isl_basic_set_from_constraint(isl_constraint_copy(c));
    dim = basic_set_cloog_dim_to_isl_dim(bset, level - 1);
    isl_basic_set_free(bset);

    return dim.type == isl_dim_set;
}


/**
 * Create a CloogConstraintSet containing enough information to perform
 * a reduction on the upper equality (in this case lower is an invalid
 * CloogConstraint) or the pair of inequalities upper and lower
 * from within insert_modulo_guard.
 * In the isl backend, we return a CloogConstraintSet containing both
 * bounds, as the stride may change during the reduction and we may
 * need to recompute the bound on the modulo expression.
 */
CloogConstraintSet *cloog_constraint_set_for_reduction(CloogConstraint *upper,
        CloogConstraint *lower) {
    struct isl_basic_set *bset;
    isl_constraint *c;

    c = cloog_constraint_to_isl(upper);
    bset = isl_basic_set_from_constraint(isl_constraint_copy(c));
    if (cloog_constraint_is_valid(lower)) {
        c = cloog_constraint_to_isl(lower);
        bset = isl_basic_set_add_constraint(bset,
                                            isl_constraint_copy(c));
    }
    return cloog_constraint_set_from_isl_basic_set(bset);
}


static int add_constant_term(CloogConstraint *c, void *user) {
    isl_val **bound = (isl_val **)user;
    isl_val *v;

    v = cloog_constraint_constant_get_val(c);
    *bound = isl_val_add(*bound, v);

    return 0;
}


/* Return an isl_basic_set representation of the equality stored
 * at position i in the given CloogEqualities.
 */
static __isl_give isl_basic_set *equality_to_basic_set(CloogEqualities *equal,
        int i) {
    isl_constraint *c;
    isl_basic_set *bset;
    unsigned nparam;
    unsigned nvar;

    c = isl_constraint_copy(equal->constraints[i]);
    bset = isl_basic_set_from_constraint(c);
    nparam = isl_basic_set_dim(bset, isl_dim_param);
    nvar = isl_basic_set_dim(bset, isl_dim_set);
    bset = isl_basic_set_add_dims(bset, isl_dim_set,
                                  equal->total_dim - (nparam + nvar));
    return bset;
}

/**
 * Reduce the modulo guard expressed by "constraints" using equalities
 * found in outer nesting levels (stored in "equal").
 * The modulo guard may be an equality or a pair of inequalities.
 * In case of a pair of inequalities, *bound contains the bound on the
 * corresponding modulo expression.  If any reduction is performed
 * then this bound is recomputed.
 *
 * "level" may not correspond to an existentially quantified variable.
 *
 * We first check if there are any equalities we can use.  If not,
 * there is again nothing to reduce.
 * For the actual reduction, we use isl_basic_set_gist, but this
 * function will only perform the reduction we want here if the
 * the variable that imposes the modulo constraint has been projected
 * out (i.e., turned into an existentially quantified variable).
 * After the call to isl_basic_set_gist, we need to move the
 * existential variable back into the position where the calling
 * function expects it (assuming there are any constraints left).
 * We do this by adding an equality between the given dimension and
 * the existentially quantified variable.
 *
 * If there are no existentially quantified variables left, then
 * we don't need to add this equality.
 * If, on the other hand, the resulting basic set involves more
 * than one existentially quantified variable, then the caller
 * will not be able to handle the result, so we just return the
 * original input instead.
 */
CloogConstraintSet *cloog_constraint_set_reduce(CloogConstraintSet *constraints,
        int level, CloogEqualities *equal, int nb_par, cloog_int_t *bound) {
    int j;
    isl_space *idim;
    struct isl_basic_set *eq;
    struct isl_basic_map *id;
    struct cloog_isl_dim dim;
    struct isl_constraint *c;
    unsigned constraints_dim;
    unsigned n_div;
    isl_basic_set *bset, *orig;

    bset = cloog_constraints_set_to_isl(constraints);
    orig = isl_basic_set_copy(bset);
    dim = set_cloog_dim_to_isl_dim(constraints, level - 1);
    assert(dim.type == isl_dim_set);

    eq = NULL;
    for (j = 0; j < level - 1; ++j) {
        isl_basic_set *bset_j;
        if (equal->types[j] != EQTYPE_EXAFFINE)
            continue;
        bset_j = equality_to_basic_set(equal, j);
        if (!eq)
            eq = bset_j;
        else
            eq = isl_basic_set_intersect(eq, bset_j);
    }
    if (!eq) {
        isl_basic_set_free(orig);
        return cloog_constraint_set_from_isl_basic_set(bset);
    }

    idim = isl_space_map_from_set(isl_basic_set_get_space(bset));
    id = isl_basic_map_identity(idim);
    id = isl_basic_map_remove_dims(id, isl_dim_out, dim.pos, 1);
    bset = isl_basic_set_apply(bset, isl_basic_map_copy(id));
    bset = isl_basic_set_apply(bset, isl_basic_map_reverse(id));

    constraints_dim = isl_basic_set_dim(bset, isl_dim_set);
    eq = isl_basic_set_remove_dims(eq, isl_dim_set, constraints_dim,
                                   isl_basic_set_dim(eq, isl_dim_set) - constraints_dim);
    bset = isl_basic_set_gist(bset, eq);
    n_div = isl_basic_set_dim(bset, isl_dim_div);
    if (n_div > 1) {
        isl_basic_set_free(bset);
        return cloog_constraint_set_from_isl_basic_set(orig);
    }
    if (n_div < 1) {
        isl_basic_set_free(orig);
        return cloog_constraint_set_from_isl_basic_set(bset);
    }

    c = isl_equality_alloc(isl_basic_set_get_local_space(bset));
    c = isl_constraint_set_coefficient_si(c, isl_dim_div, 0, 1);
    c = isl_constraint_set_coefficient_si(c, isl_dim_set, dim.pos, -1);
    bset = isl_basic_set_add_constraint(bset, c);

    cloog_int_set_si(*bound, 0);
    isl_val *v = cloog_int_to_isl_val(isl_basic_set_get_ctx(bset), *bound);
    constraints = cloog_constraint_set_from_isl_basic_set(bset);
    cloog_constraint_set_foreach_constraint(constraints,
                                            add_constant_term, &v);
    isl_val_to_cloog_int(v, bound); //return the value to bound

    isl_val_free(v);
    isl_basic_set_free(orig);
    return cloog_constraint_set_from_isl_basic_set(bset);
}

CloogConstraint *cloog_constraint_copy(CloogConstraint *constraint) {
    return cloog_constraint_from_isl_constraint(
               isl_constraint_copy(cloog_constraint_to_isl(constraint)));
}

void cloog_constraint_release(CloogConstraint *constraint) {
    isl_constraint_free(cloog_constraint_to_isl(constraint));
}

struct cloog_isl_foreach {
    int (*fn)(CloogConstraint *constraint, void *user);
    void *user;
};

static int cloog_isl_foreach_cb(__isl_take isl_constraint *c, void *user) {
    struct cloog_isl_foreach *data = (struct cloog_isl_foreach *)user;
    int ret;

    if (isl_constraint_is_div_constraint(c)) {
        isl_constraint_free(c);
        return 0;
    }

    ret = data->fn(cloog_constraint_from_isl_constraint(c), data->user);

    isl_constraint_free(c);

    return ret;
}

int cloog_constraint_set_foreach_constraint(CloogConstraintSet *constraints,
        int (*fn)(CloogConstraint *constraint, void *user), void *user) {
    struct cloog_isl_foreach data = { fn, user };
    isl_basic_set *bset;

    bset = cloog_constraints_set_to_isl(constraints);
    return isl_basic_set_foreach_constraint(bset,
                                            cloog_isl_foreach_cb, &data);
}

CloogConstraint *cloog_equal_constraint(CloogEqualities *equal, int j) {
    isl_constraint *c;

    c = isl_constraint_copy(equal->constraints[j]);
    return cloog_constraint_from_isl_constraint(c);
}

/* Given a stride constraint on iterator i (specified by level) of the form
 *
 *	i = f(outer iterators) + stride * f(existentials)
 *
 * extract f as an isl_aff.
 */
static isl_aff *extract_stride_offset(__isl_keep isl_constraint *c,
                                      int level, CloogStride *stride) {
    int i;
    isl_space *dim = isl_constraint_get_space(c);
    isl_local_space *ls = isl_local_space_from_space(dim);
    isl_aff *offset = isl_aff_zero_on_domain(ls);
    isl_val *u;
    unsigned nparam, nvar;

    nparam = isl_constraint_dim(c, isl_dim_param);
    nvar = isl_constraint_dim(c, isl_dim_set);

    for (i = 0; i < nparam; ++i) {
        u = isl_constraint_get_coefficient_val(c, isl_dim_param, i);
        u = isl_val_mul(u, cloog_int_to_isl_val(isl_constraint_get_ctx(c), stride->factor));
        offset = isl_aff_set_coefficient_val(offset, isl_dim_param, i, u);
    }
    for (i = 0; i < nvar; ++i) {
        if (i == level - 1)
            continue;
        u = isl_constraint_get_coefficient_val(c, isl_dim_set, i);
        u = isl_val_mul(u, cloog_int_to_isl_val(isl_constraint_get_ctx(c), stride->factor));
        offset = isl_aff_set_coefficient_val(offset, isl_dim_in, i, u);
    }
    u = isl_constraint_get_constant_val(c);
    u = isl_val_mul(u, cloog_int_to_isl_val(isl_constraint_get_ctx(c), stride->factor));
    offset = isl_aff_set_constant_val(offset, u);

    return offset;
}

/* Update the given lower bound on level such that it satisfies the stride
 * constraint.  The computation performed here is essentially the same
 * as that performed in constraint_stride_lower_c.
 *
 * We update the constraint
 *
 *	a i + f >= 0
 *
 * to
 *
 *	i >= s * ceil((-f/a - d)/s) + d
 *
 * with s the stride and d the offset encoded in the stride constraint.
 */
CloogConstraint *cloog_constraint_stride_lower_bound(CloogConstraint *c,
        int level, CloogStride *stride) {
    isl_constraint *stride_c = cloog_constraint_to_isl(stride->constraint);
    isl_constraint *bound = cloog_constraint_to_isl(c);
    isl_aff *offset;
    isl_aff *lower;

    lower = isl_constraint_get_bound(bound, isl_dim_set, level - 1);
    isl_constraint_free(bound);

    offset = extract_stride_offset(stride_c, level, stride);

    lower = isl_aff_sub(lower, isl_aff_copy(offset));
    lower = isl_aff_scale_down_val(lower, cloog_int_to_isl_val(isl_constraint_get_ctx(stride_c), stride->stride));
    lower = isl_aff_ceil(lower);
    lower = isl_aff_scale_val(lower, cloog_int_to_isl_val(isl_constraint_get_ctx(stride_c), stride->stride));
    lower = isl_aff_add(lower, offset);
    lower = isl_aff_neg(lower);
    lower = isl_aff_add_coefficient_si(lower, isl_dim_in, level - 1, 1);

    bound = isl_inequality_from_aff(lower);

    return cloog_constraint_from_isl_constraint(bound);
}
