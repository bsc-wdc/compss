#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "../include/cloog/cloog.h"

#define ALLOC(type) (type*)malloc(sizeof(type))
#define ALLOCN(type,n) (type*)malloc((n)*sizeof(type))

/**
 * CloogInfos structure:
 * this structure contains all the informations necessary for pretty printing,
 * they come from the original CloogProgram structure (language, names), from
 * genereral options (options) or are built only for pretty printing (stride).
 * This structure is mainly there to reduce the number of function parameters,
 * since most pprint.c functions need most of its field.
 */
struct clooginfos {
    CloogState *state;         /**< State. */
    CloogStride **stride;
    int stride_level;          /**< Number of valid entries in stride array. */
    int  nb_scattdims ;        /**< Scattering dimension number. */
    int * scaldims ;           /**< Boolean array saying whether a given
                              *   scattering dimension is scalar or not.
                              */
    CloogNames * names ;       /**< Names of iterators and parameters. */
    CloogOptions * options ;   /**< Options on CLooG's behaviour. */
    CloogEqualities *equal;    /**< Matrix of equalities. */
} ;

typedef struct clooginfos CloogInfos ;

static int clast_expr_cmp(struct clast_expr *e1, struct clast_expr *e2);
static int clast_term_cmp(struct clast_term *t1, struct clast_term *t2);
static int clast_binary_cmp(struct clast_binary *b1, struct clast_binary *b2);
static int clast_reduction_cmp(struct clast_reduction *r1,
                               struct clast_reduction *r2);

static struct clast_expr *clast_expr_copy(struct clast_expr *e);

static int clast_equal_add(CloogEqualities *equal,
                           CloogConstraintSet *constraints,
                           int level, CloogConstraint *constraint,
                           CloogInfos *infos);

static struct clast_stmt *clast_equal(int level, CloogInfos *infos);
static struct clast_expr *clast_minmax(CloogConstraintSet *constraints,
                                       int level, int max, int guard,
                                       int lower_bound, int no_earlier,
                                       CloogInfos *infos);
static void insert_guard(CloogConstraintSet *constraints, int level,
                         struct clast_stmt ***next, CloogInfos *infos);
static int insert_modulo_guard(CloogConstraint *upper,
                               CloogConstraint *lower, int level,
                               struct clast_stmt ***next, CloogInfos *infos);
static int insert_equation(CloogDomain *domain, CloogConstraint *upper,
                           CloogConstraint *lower, int level,
                           struct clast_stmt ***next, CloogInfos *infos);
static int insert_for(CloogDomain *domain, CloogConstraintSet *constraints,
                      int level, int otl, struct clast_stmt ***next,
                      CloogInfos *infos);
static void insert_block(CloogDomain *domain, CloogBlock *block, int level,
                         struct clast_stmt ***next, CloogInfos *infos);
static void insert_loop(CloogLoop * loop, int level,
                        struct clast_stmt ***next, CloogInfos *infos);


struct clast_name *new_clast_name(const char *name) {
    struct clast_name *n = malloc(sizeof(struct clast_name));
    n->expr.type = clast_expr_name;
    n->name = name;
    return n;
}

struct clast_term *new_clast_term(cloog_int_t c, struct clast_expr *v) {
    struct clast_term *t = malloc(sizeof(struct clast_term));
    t->expr.type = clast_expr_term;
    cloog_int_init(t->val);
    cloog_int_set(t->val, c);
    t->var = v;
    return t;
}

struct clast_binary *new_clast_binary(enum clast_bin_type t,
                                      struct clast_expr *lhs, cloog_int_t rhs) {
    struct clast_binary *b = malloc(sizeof(struct clast_binary));
    b->expr.type = clast_expr_bin;
    b->type = t;
    b->LHS = lhs;
    cloog_int_init(b->RHS);
    cloog_int_set(b->RHS, rhs);
    return b;
}

struct clast_reduction *new_clast_reduction(enum clast_red_type t, int n) {
    int i;
    struct clast_reduction *r;
    r = malloc(sizeof(struct clast_reduction)+(n-1)*sizeof(struct clast_expr *));
    r->expr.type = clast_expr_red;
    r->type = t;
    r->n = n;
    for (i = 0; i < n; ++i) {
        r->elts[i] = NULL;
    }
    return r;
}

static void free_clast_root(struct clast_stmt *s);

const struct clast_stmt_op stmt_root = { free_clast_root };

static void free_clast_root(struct clast_stmt *s) {
    struct clast_root *r = (struct clast_root *)s;
    assert(CLAST_STMT_IS_A(s, stmt_root));
    cloog_names_free(r->names);
    free(r);
}

struct clast_root *new_clast_root(CloogNames *names) {
    struct clast_root *r = malloc(sizeof(struct clast_root));
    r->stmt.op = &stmt_root;
    r->stmt.next = NULL;
    r->names = cloog_names_copy(names);
    return r;
}

static void free_clast_assignment(struct clast_stmt *s);

const struct clast_stmt_op stmt_ass = { free_clast_assignment };

static void free_clast_assignment(struct clast_stmt *s) {
    struct clast_assignment *a = (struct clast_assignment *)s;
    assert(CLAST_STMT_IS_A(s, stmt_ass));
    free_clast_expr(a->RHS);
    free(a);
}

struct clast_assignment *new_clast_assignment(const char *lhs,
        struct clast_expr *rhs) {
    struct clast_assignment *a = malloc(sizeof(struct clast_assignment));
    a->stmt.op = &stmt_ass;
    a->stmt.next = NULL;
    a->LHS = lhs;
    a->RHS = rhs;
    return a;
}

static void free_clast_user_stmt(struct clast_stmt *s);

const struct clast_stmt_op stmt_user = { free_clast_user_stmt };

static void free_clast_user_stmt(struct clast_stmt *s) {
    struct clast_user_stmt *u = (struct clast_user_stmt *)s;
    assert(CLAST_STMT_IS_A(s, stmt_user));
    cloog_domain_free(u->domain);
    cloog_statement_free(u->statement);
    cloog_clast_free(u->substitutions);
    free(u);
}

struct clast_user_stmt *new_clast_user_stmt(CloogDomain *domain,
        CloogStatement *stmt, struct clast_stmt *subs) {
    struct clast_user_stmt *u = malloc(sizeof(struct clast_user_stmt));
    u->stmt.op = &stmt_user;
    u->stmt.next = NULL;
    u->domain = cloog_domain_copy(domain);
    u->statement = cloog_statement_copy(stmt);
    u->substitutions = subs;
    return u;
}

static void free_clast_block(struct clast_stmt *b);

const struct clast_stmt_op stmt_block = { free_clast_block };

static void free_clast_block(struct clast_stmt *s) {
    struct clast_block *b = (struct clast_block *)s;
    assert(CLAST_STMT_IS_A(s, stmt_block));
    cloog_clast_free(b->body);
    free(b);
}

struct clast_block *new_clast_block() {
    struct clast_block *b = malloc(sizeof(struct clast_block));
    b->stmt.op = &stmt_block;
    b->stmt.next = NULL;
    b->body = NULL;
    return b;
}

static void free_clast_for(struct clast_stmt *s);

const struct clast_stmt_op stmt_for = { free_clast_for };

static void free_clast_for(struct clast_stmt *s) {
    struct clast_for *f = (struct clast_for *)s;
    assert(CLAST_STMT_IS_A(s, stmt_for));
    cloog_domain_free(f->domain);
    free_clast_expr(f->LB);
    free_clast_expr(f->UB);
    cloog_int_clear(f->stride);
    cloog_clast_free(f->body);
    if (f->private_vars) free(f->private_vars);
    if (f->reduction_vars) free(f->reduction_vars);
    if (f->time_var_name) free(f->time_var_name);
    free(f);
}

struct clast_for *new_clast_for(CloogDomain *domain, const char *it,
                                struct clast_expr *LB, struct clast_expr *UB,
                                CloogStride *stride) {
    struct clast_for *f = malloc(sizeof(struct clast_for));
    f->stmt.op = &stmt_for;
    f->stmt.next = NULL;
    f->domain = cloog_domain_copy(domain);
    f->iterator = it;
    f->LB = LB;
    f->UB = UB;
    f->body = NULL;
    f->parallel = CLAST_PARALLEL_NOT;
    f->private_vars = NULL;
    f->reduction_vars = NULL;
    f->time_var_name = NULL;
    cloog_int_init(f->stride);
    if (stride) {
        cloog_int_set(f->stride, stride->stride);
    } else {
        cloog_int_set_si(f->stride, 1);
    }
    return f;
}

static void free_clast_guard(struct clast_stmt *s);

const struct clast_stmt_op stmt_guard = { free_clast_guard };

static void free_clast_guard(struct clast_stmt *s) {
    int i;
    struct clast_guard *g = (struct clast_guard *)s;
    assert(CLAST_STMT_IS_A(s, stmt_guard));
    cloog_clast_free(g->then);
    for (i = 0; i < g->n; ++i) {
        free_clast_expr(g->eq[i].LHS);
        free_clast_expr(g->eq[i].RHS);
    }
    free(g);
}

struct clast_guard *new_clast_guard(int n) {
    int i;
    struct clast_guard *g = malloc(sizeof(struct clast_guard) +
                                   (n-1) * sizeof(struct clast_equation));
    g->stmt.op = &stmt_guard;
    g->stmt.next = NULL;
    g->then = NULL;
    g->n = n;
    for (i = 0; i < n; ++i) {
        g->eq[i].LHS = NULL;
        g->eq[i].RHS = NULL;
    }
    return g;
}

void free_clast_name(struct clast_name *n) {
    free(n);
}

void free_clast_term(struct clast_term *t) {
    cloog_int_clear(t->val);
    free_clast_expr(t->var);
    free(t);
}

void free_clast_binary(struct clast_binary *b) {
    cloog_int_clear(b->RHS);
    free_clast_expr(b->LHS);
    free(b);
}

void free_clast_reduction(struct clast_reduction *r) {
    int i;
    for (i = 0; i < r->n; ++i) {
        free_clast_expr(r->elts[i]);
    }
    free(r);
}

void free_clast_expr(struct clast_expr *e) {
    if (!e) {
        return;
    }

    switch (e->type) {
    case clast_expr_name:
        free_clast_name((struct clast_name*) e);
        break;
    case clast_expr_term:
        free_clast_term((struct clast_term*) e);
        break;
    case clast_expr_red:
        free_clast_reduction((struct clast_reduction*) e);
        break;
    case clast_expr_bin:
        free_clast_binary((struct clast_binary*) e);
        break;
    default:
        assert(0);
    }
}

void free_clast_stmt(struct clast_stmt *s) {
    assert(s->op);
    assert(s->op->free);
    s->op->free(s);
}

void cloog_clast_free(struct clast_stmt *s) {
    struct clast_stmt *next;
    while (s) {
        next = s->next;
        free_clast_stmt(s);
        s = next;
    }
}

static int clast_name_cmp(struct clast_name *n1, struct clast_name *n2) {
    return n1->name == n2->name ? 0 : strcmp(n1->name, n2->name);
}

static int clast_term_cmp(struct clast_term *t1, struct clast_term *t2) {
    int c;
    if (!t1->var && t2->var) {
        return -1;
    }
    if (t1->var && !t2->var) {
        return 1;
    }
    c = clast_expr_cmp(t1->var, t2->var);
    if (c) {
        return c;
    }
    return cloog_int_cmp(t1->val, t2->val);
}

static int clast_binary_cmp(struct clast_binary *b1, struct clast_binary *b2) {
    int c;

    if (b1->type != b2->type) {
        return b1->type - b2->type;
    }
    if ((c = cloog_int_cmp(b1->RHS, b2->RHS))) {
        return c;
    }
    return clast_expr_cmp(b1->LHS, b2->LHS);
}

static int clast_reduction_cmp(struct clast_reduction *r1, struct clast_reduction *r2) {
    int i;
    int c;

    if (r1->n == 1 && r2->n == 1) {
        return clast_expr_cmp(r1->elts[0], r2->elts[0]);
    }
    if (r1->type != r2->type) {
        return r1->type - r2->type;
    }
    if (r1->n != r2->n) {
        return r1->n - r2->n;
    }
    for (i = 0; i < r1->n; ++i) {
        if ((c = clast_expr_cmp(r1->elts[i], r2->elts[i]))) {
            return c;
        }
    }
    return 0;
}

static int clast_expr_cmp(struct clast_expr *e1, struct clast_expr *e2) {
    if (!e1 && !e2) {
        return 0;
    }
    if (!e1) {
        return -1;
    }
    if (!e2) {
        return 1;
    }
    if (e1->type != e2->type) {
        return e1->type - e2->type;
    }

    switch (e1->type) {
    case clast_expr_name:
        return clast_name_cmp((struct clast_name*) e1,
                              (struct clast_name*) e2);
    case clast_expr_term:
        return clast_term_cmp((struct clast_term*) e1,
                              (struct clast_term*) e2);
    case clast_expr_bin:
        return clast_binary_cmp((struct clast_binary*) e1,
                                (struct clast_binary*) e2);
    case clast_expr_red:
        return clast_reduction_cmp((struct clast_reduction*) e1,
                                   (struct clast_reduction*) e2);
    default:
        assert(0);
    }
}

int clast_expr_equal(struct clast_expr *e1, struct clast_expr *e2) {
    return clast_expr_cmp(e1, e2) == 0;
}

/**
 * Return 1 is both expressions are constant terms and e1 is bigger than e2.
 */
int clast_expr_is_bigger_constant(struct clast_expr *e1, struct clast_expr *e2) {
    struct clast_term *t1, *t2;
    struct clast_reduction *r;

    if (!e1 || !e2) {
        return 0;
    }
    if (e1->type == clast_expr_red) {
        r = (struct clast_reduction *)e1;
        return r->n == 1 && clast_expr_is_bigger_constant(r->elts[0], e2);
    }
    if (e2->type == clast_expr_red) {
        r = (struct clast_reduction *)e2;
        return r->n == 1 && clast_expr_is_bigger_constant(e1, r->elts[0]);
    }
    if (e1->type != clast_expr_term || e2->type != clast_expr_term) {
        return 0;
    }
    t1 = (struct clast_term *)e1;
    t2 = (struct clast_term *)e2;
    if (t1->var || t2->var) {
        return 0;
    }
    return cloog_int_gt(t1->val, t2->val);
}

static int qsort_expr_cmp(const void *p1, const void *p2) {
    return clast_expr_cmp(*(struct clast_expr **)p1, *(struct clast_expr **)p2);
}

static void clast_reduction_sort(struct clast_reduction *r) {
    qsort(&r->elts[0], r->n, sizeof(struct clast_expr *), qsort_expr_cmp);
}

static int qsort_eq_cmp(const void *p1, const void *p2) {
    struct clast_equation *eq1 = (struct clast_equation *)p1;
    struct clast_equation *eq2 = (struct clast_equation *)p2;
    int cmp;

    cmp = clast_expr_cmp(eq1->LHS, eq2->LHS);
    if (cmp) {
        return cmp;
    }

    cmp = clast_expr_cmp(eq1->RHS, eq2->RHS);
    if (cmp) {
        return cmp;
    }

    return eq1->sign - eq2->sign;
}

/**
 * Sort equations in a clast_guard.
 */
static void clast_guard_sort(struct clast_guard *g) {
    qsort(&g->eq[0], g->n, sizeof(struct clast_equation), qsort_eq_cmp);
}


/**
 * Construct a (deep) copy of an expression clast.
 */
static struct clast_expr *clast_expr_copy(struct clast_expr *e) {
    if (!e) {
        return NULL;
    }

    switch (e->type) {
    case clast_expr_name: {
        struct clast_name* n = (struct clast_name*) e;
        return &new_clast_name(n->name)->expr;
    }
    case clast_expr_term: {
        struct clast_term* t = (struct clast_term*) e;
        return &new_clast_term(t->val, clast_expr_copy(t->var))->expr;
    }
    case clast_expr_red: {
        int i;
        struct clast_reduction *r = (struct clast_reduction*) e;
        struct clast_reduction *r2 = new_clast_reduction(r->type, r->n);
        for (i = 0; i < r->n; ++i) {
            r2->elts[i] = clast_expr_copy(r->elts[i]);
        }
        return &r2->expr;
    }
    case clast_expr_bin: {
        struct clast_binary *b = (struct clast_binary*) e;
        return &new_clast_binary(b->type, clast_expr_copy(b->LHS), b->RHS)->expr;
    }
    default:
        assert(0);
    }
}


/******************************************************************************
 *                        Equalities spreading functions                      *
 ******************************************************************************/


/**
 * clast_equal_allow function:
 * This function checks whether the options allow us to spread the equality or
 * not. It returns 1 if so, 0 otherwise.
 * - equal is the matrix of equalities,
 * - level is the column number in equal of the element which is 'equal to',
 * - line is the line number in equal of the constraint we want to study,
 * - the infos structure gives the user all options on code printing and more.
 **
 * - October 27th 2005: first version (extracted from old pprint_equal_add).
 */
static int clast_equal_allow(CloogEqualities *equal, int level, int line,
                             CloogInfos *infos) {
    if (level < infos->options->fsp) {
        return 0;
    }

    if ((cloog_equal_type(equal, level) == EQTYPE_EXAFFINE) &&
            !infos->options->esp) {
        return 0 ;
    }

    return 1 ;
}


/**
 * clast_equal_add function:
 * This function updates the row (level-1) of the equality matrix (equal) with
 * the row that corresponds to the row (line) of the matrix (matrix). It returns
 * 1 if the row can be updated, 0 otherwise.
 * - equal is the matrix of equalities,
 * - matrix is the matrix of constraints,
 * - level is the column number in matrix of the element which is 'equal to',
 * - line is the line number in matrix of the constraint we want to study,
 * - the infos structure gives the user all options on code printing and more.
 */
static int clast_equal_add(CloogEqualities *equal,
                           CloogConstraintSet *constraints,
                           int level, CloogConstraint *constraint,
                           CloogInfos *infos) {
    cloog_equal_add(equal, constraints, level, constraint,
                    infos->names->nb_parameters);

    return clast_equal_allow(equal, level, level-1, infos);
}



/**
 * clast_equal function:
 * This function prints the substitution data of a statement into a clast_stmt.
 * Using this function instead of pprint_equal is useful for generating
 * a compilable pseudo-code by using preprocessor macro for each statement.
 * By opposition to pprint_equal, the result is less human-readable. For
 * instance this function will print (i,i+3,k,3) where pprint_equal would
 * return (j=i+3,l=3).
 * - level is the number of loops enclosing the statement,
 * - the infos structure gives the user all options on code printing and more.
 **
 * - March    12th 2004: first version.
 * - November 21th 2005: (debug) now works well with GMP version.
 */
static struct clast_stmt *clast_equal(int level, CloogInfos *infos) {
    int i ;
    struct clast_expr *e;
    struct clast_stmt *a = NULL;
    struct clast_stmt **next = &a;
    CloogEqualities *equal = infos->equal;
    CloogConstraint *equal_constraint;

    for (i=infos->names->nb_scattering; i<level-1; i++) {
        if (cloog_equal_type(equal, i+1)) {
            equal_constraint = cloog_equal_constraint(equal, i);
            e = clast_bound_from_constraint(equal_constraint, i+1, infos->names);
            cloog_constraint_release(equal_constraint);
        } else {
            e = &new_clast_term(infos->state->one, &new_clast_name(
                                    cloog_names_name_at_level(infos->names, i+1))->expr)->expr;
        }
        *next = &new_clast_assignment(NULL, e)->stmt;
        next = &(*next)->next;
    }

    return a;
}


/**
 * clast_bound_from_constraint function:
 * This function returns a clast_expr containing the printing of the
 * 'right part' of a constraint according to an element.
 * For instance, for the constraint -3*i + 2*j - M >=0 and the element j,
 * we have j >= (3*i + M)/2. As we are looking for integral solutions, this
 * function should return 'ceild(3*i+M,2)'.
 * - matrix is the polyhedron containing all the constraints,
 * - line_num is the line number in domain of the constraint we want to print,
 * - level is the column number in domain of the element we want to use,
 * - names structure gives the user some options about code printing,
 *   the number of parameters in domain (nb_par), and the arrays of iterator
 *   names and parameters (iters and params).
 **
 * - November 2nd 2001: first version.
 * - June    27th 2003: 64 bits version ready.
 */
struct clast_expr *clast_bound_from_constraint(CloogConstraint *constraint,
        int level, CloogNames *names) {
    int i, sign, nb_elts=0, len;
    cloog_int_t *line, numerator, denominator, temp, division;
    struct clast_expr *e = NULL;
    struct cloog_vec *line_vector;

    len = cloog_constraint_total_dimension(constraint) + 2;
    line_vector = cloog_vec_alloc(len);
    line = line_vector->p;
    cloog_constraint_copy_coefficients(constraint, line+1);
    cloog_int_init(temp);
    cloog_int_init(numerator);
    cloog_int_init(denominator);

    if (!cloog_int_is_zero(line[level])) {
        struct clast_reduction *r;
        /* Maybe we need to invert signs in such a way that the element sign is>0.*/
        sign = -cloog_int_sgn(line[level]);

        for (i = 1, nb_elts = 0; i <= len - 1; ++i) {
            if (i != level && !cloog_int_is_zero(line[i])) {
                nb_elts++;
            }
        }
        r = new_clast_reduction(clast_red_sum, nb_elts);
        nb_elts = 0;

        /* First, we have to print the iterators and the parameters. */
        for (i = 1; i <= len - 2; i++) {
            struct clast_expr *v;

            if (i == level || cloog_int_is_zero(line[i])) {
                continue;
            }

            v = cloog_constraint_variable_expr(constraint, i, names);

            if (sign == -1) {
                cloog_int_neg(temp,line[i]);
            } else {
                cloog_int_set(temp,line[i]);
            }

            r->elts[nb_elts++] = &new_clast_term(temp, v)->expr;
        }

        if (sign == -1) {
            cloog_int_neg(numerator, line[len - 1]);
            cloog_int_set(denominator, line[level]);
        } else {
            cloog_int_set(numerator, line[len - 1]);
            cloog_int_neg(denominator, line[level]);
        }

        /* Finally, the constant, and the final printing. */
        if (nb_elts) {
            if (!cloog_int_is_zero(numerator)) {
                r->elts[nb_elts++] = &new_clast_term(numerator, NULL)->expr;
            }

            if (!cloog_int_is_one(line[level]) && !cloog_int_is_neg_one(line[level])) {
                if (!cloog_constraint_is_equality(constraint)) {
                    if (cloog_int_is_pos(line[level])) {
                        e = &new_clast_binary(clast_bin_cdiv, &r->expr, denominator)->expr;
                    } else {
                        e = &new_clast_binary(clast_bin_fdiv, &r->expr, denominator)->expr;
                    }
                } else {
                    e = &new_clast_binary(clast_bin_div, &r->expr, denominator)->expr;
                }
            } else {
                e = &r->expr;
            }
        } else {
            free_clast_reduction(r);
            if (cloog_int_is_zero(numerator)) {
                e = &new_clast_term(numerator, NULL)->expr;
            } else {
                if (!cloog_int_is_one(denominator)) {
                    if (!cloog_constraint_is_equality(constraint)) { /* useful? */
                        if (cloog_int_is_divisible_by(numerator, denominator)) {
                            cloog_int_divexact(temp, numerator, denominator);
                            e = &new_clast_term(temp, NULL)->expr;
                        } else {
                            cloog_int_init(division);
                            cloog_int_tdiv_q(division, numerator, denominator);
                            if (cloog_int_is_neg(numerator)) {
                                if (cloog_int_is_pos(line[level])) {
                                    /* nb<0 need max */
                                    e = &new_clast_term(division, NULL)->expr;
                                } else {
                                    /* nb<0 need min */
                                    cloog_int_sub_ui(temp, division, 1);
                                    e = &new_clast_term(temp, NULL)->expr;
                                }
                            } else {
                                if (cloog_int_is_pos(line[level])) {
                                    /* nb>0 need max */
                                    cloog_int_add_ui(temp, division, 1);
                                    e = &new_clast_term(temp, NULL)->expr;
                                } else {
                                    /* nb>0 need min */
                                    e = &new_clast_term(division, NULL)->expr;
                                }
                            }
                            cloog_int_clear(division);
                        }
                    } else {
                        e = &new_clast_binary(clast_bin_div,
                                              &new_clast_term(numerator, NULL)->expr,
                                              denominator)->expr;
                    }
                } else {
                    e = &new_clast_term(numerator, NULL)->expr;
                }
            }
        }
    }

    cloog_vec_free(line_vector);

    cloog_int_clear(temp);
    cloog_int_clear(numerator);
    cloog_int_clear(denominator);

    return e;
}


/* Temporary structure for communication between clast_minmax and
 * its cloog_constraint_set_foreach_constraint callback functions.
 */
struct clast_minmax_data {
    int level;
    int max;
    int guard;
    int lower_bound;
    int no_earlier;
    CloogInfos *infos;
    int n;
    struct clast_reduction *r;
};


/* Should constraint "c" be considered by clast_minmax?
 *
 * If d->no_earlier is set, then the constraint may not involve
 * any earlier variables.
 */
static int valid_bound(CloogConstraint *c, struct clast_minmax_data *d) {
    int i;

    if (d->max && !cloog_constraint_is_lower_bound(c, d->level - 1)) {
        return 0;
    }
    if (!d->max && !cloog_constraint_is_upper_bound(c, d->level - 1)) {
        return 0;
    }
    if (cloog_constraint_is_equality(c)) {
        return 0;
    }
    if (d->guard && cloog_constraint_involves(c, d->guard - 1)) {
        return 0;
    }

    if (d->no_earlier) {
        for (i = 0; i < d->level - 1; ++i) {
            if (cloog_constraint_involves(c, i)) {
                return 0;
            }
        }
    }

    return 1;
}


/* Increment n for each bound that should be considered by clast_minmax.
 */
static int count_bounds(CloogConstraint *c, void *user) {
    struct clast_minmax_data *d = (struct clast_minmax_data *) user;

    if (!valid_bound(c, d)) {
        return 0;
    }

    d->n++;

    return 0;
}


/* Update the given lower bound based on stride information,
 * for those cases where the stride offset is represented by
 * a constraint.
 * Note that cloog_loop_stride may have already performed a
 * similar update of the lower bounds, but the updated lower
 * bounds may have been eliminated because they are redundant
 * by definition.  On the other hand, performing the update
 * on an already updated constraint is an identity operation
 * and is therefore harmless.
 */
static CloogConstraint *update_lower_bound_c(CloogConstraint *c, int level,
        CloogStride *stride) {
    if (!stride->constraint) {
        return c;
    }

    return cloog_constraint_stride_lower_bound(c, level, stride);
}


/* Update the given lower bound based on stride information.
 * If the stride offset is represented by a constraint,
 * then we have already performed the update in update_lower_bound_c.
 * Otherwise, the original lower bound is known to be a constant.
 * If the bound has already been updated and it just happens
 * to be a constant, then this function performs an identity
 * operation on the constant.
 */
static void update_lower_bound(struct clast_expr *expr, int level,
                               CloogStride *stride) {
    struct clast_term *t;
    if (stride->constraint) {
        return;
    }
    if (expr->type != clast_expr_term) {
        return;
    }
    t = (struct clast_term *)expr;
    if (t->var) {
        return;
    }

    cloog_int_sub(t->val, t->val, stride->offset);
    cloog_int_cdiv_q(t->val, t->val, stride->stride);
    cloog_int_mul(t->val, t->val, stride->stride);
    cloog_int_add(t->val, t->val, stride->offset);
}


/* Add all relevant bounds to r->elts and update lower bounds
 * based on stride information.
 */
static int collect_bounds(CloogConstraint *c, void *user) {
    struct clast_minmax_data *d = (struct clast_minmax_data *) user;

    if (!valid_bound(c, d)) {
        return 0;
    }

    c = cloog_constraint_copy(c);

    if (d->lower_bound && d->infos->stride[d->level - 1]) {
        c = update_lower_bound_c(c, d->level, d->infos->stride[d->level - 1]);
    }

    d->r->elts[d->n] = clast_bound_from_constraint(c, d->level,
                       d->infos->names);
    if (d->lower_bound && d->infos->stride[d->level - 1]) {
        update_lower_bound(d->r->elts[d->n], d->level,
                           d->infos->stride[d->level - 1]);
    }

    cloog_constraint_release(c);

    d->n++;

    return 0;
}


/**
 * clast_minmax function:
 * This function returns a clast_expr containing the printing of a minimum or a
 * maximum of the 'right parts' of all constraints according to an element.
 * For instance consider the constraints:
 * -3*i  +2*j   -M >= 0
 *  2*i    +j      >= 0
 *   -i    -j +2*M >= 0
 * if we are looking for the minimum for the element j, the function should
 * return 'max(ceild(3*i+M,2),-2*i)'.
 * - constraints is the constraints,
 * - level is the column number in domain of the element we want to use,
 * - max is a boolean set to 1 if we are looking for a maximum, 0 for a minimum,
 * - guard is set to 0 if there is no guard, and set to the level of the element
 *   with a guard otherwise (then the function gives the max or the min only
 *   for the constraint where the guarded coefficient is 0),
 * - lower is set to 1 if the maximum is to be used a lower bound on a loop
 * - no_earlier is set if no constraints should be used that involve
 *   earlier dimensions,
 * - the infos structure gives the user some options about code printing,
 *   the number of parameters in domain (nb_par), and the arrays of iterator
 *   names and parameters (iters and params).
 **
 * - November 2nd 2001: first version.
 */
static struct clast_expr *clast_minmax(CloogConstraintSet *constraints,
                                       int level, int max, int guard,
                                       int lower_bound, int no_earlier,
                                       CloogInfos *infos) {
    struct clast_minmax_data data = { level, max, guard, lower_bound,
               no_earlier, infos
    };

    data.n = 0;

    cloog_constraint_set_foreach_constraint(constraints, count_bounds, &data);

    if (!data.n) {
        return NULL;
    }
    data.r = new_clast_reduction(max ? clast_red_max : clast_red_min, data.n);

    data.n = 0;
    cloog_constraint_set_foreach_constraint(constraints, collect_bounds, &data);

    clast_reduction_sort(data.r);
    return &data.r->expr;
}


/**
 * Insert modulo guards defined by existentially quantified dimensions,
 * not involving the given level.
 *
 * This function is called from within insert_guard.
 * Any constraint used in constructing a modulo guard is removed
 * from the constraint set to avoid insert_guard
 * adding a duplicate (pair of) constraint(s).
 *
 * Return the updated CloogConstraintSet.
 */
static CloogConstraintSet *insert_extra_modulo_guards(
    CloogConstraintSet *constraints, int level,
    struct clast_stmt ***next, CloogInfos *infos) {
    int i;
    int nb_iter;
    int total_dim;
    CloogConstraint *upper, *lower;

    total_dim = cloog_constraint_set_total_dimension(constraints);
    nb_iter = cloog_constraint_set_n_iterators(constraints,
              infos->names->nb_parameters);

    for (i = total_dim - infos->names->nb_parameters; i >= nb_iter + 1; i--) {
        if (cloog_constraint_is_valid(upper =
                                          cloog_constraint_set_defining_equality(constraints, i))) {
            if (!level || (nb_iter < level) ||
                    !cloog_constraint_involves(upper, level-1)) {
                insert_modulo_guard(upper,
                                    cloog_constraint_invalid(), i, next, infos);
                constraints = cloog_constraint_set_drop_constraint(constraints,
                              upper);
            }
            cloog_constraint_release(upper);
        } else if (cloog_constraint_is_valid(upper =
                cloog_constraint_set_defining_inequalities(constraints,
                        i, &lower, infos->names->nb_parameters))) {
            if (!level || (nb_iter < level) ||
                    !cloog_constraint_involves(upper, level-1)) {
                insert_modulo_guard(upper, lower, i, next, infos);
                constraints = cloog_constraint_set_drop_constraint(constraints,
                              upper);
                constraints = cloog_constraint_set_drop_constraint(constraints,
                              lower);
            }
            cloog_constraint_release(upper);
            cloog_constraint_release(lower);
        }
    }

    return constraints;
}


/* Temporary structure for communication between insert_guard and
 * its cloog_constraint_set_foreach_constraint callback function.
 */
struct clast_guard_data {
    int level;
    CloogInfos *infos;
    int n;
    int i;
    int nb_iter;
    CloogConstraintSet *copy;
    struct clast_guard *g;

    int min;
    int max;
};


static int guard_count_bounds(CloogConstraint *c, void *user) {
    struct clast_guard_data *d = (struct clast_guard_data *) user;

    d->n++;

    return 0;
}


/* Insert a guard, if necesessary, for constraint j.
 *
 * If the constraint involves any earlier dimensions, then we have
 * already considered it during a previous iteration over the constraints.
 *
 * If we have already generated a min [max] for the current level d->i
 * and if the current constraint is an upper [lower] bound, then we
 * can skip the constraint as it will already have been used
 * in that previously generated min [max].
 */
static int insert_guard_constraint(CloogConstraint *j, void *user) {
    int i;
    struct clast_guard_data *d = (struct clast_guard_data *) user;
    int minmax = -1;
    int individual_constraint;
    struct clast_expr *v;
    struct clast_term *t;

    if (!cloog_constraint_involves(j, d->i - 1)) {
        return 0;
    }

    for (i = 0; i < d->i - 1; ++i) {
        if (cloog_constraint_involves(j, i)) {
            return 0;
        }
    }

    if (d->level && d->nb_iter >= d->level &&
            cloog_constraint_involves(j, d->level - 1)) {
        return 0;
    }

    individual_constraint = !d->level || cloog_constraint_is_equality(j);
    if (!individual_constraint) {
        if (d->max && cloog_constraint_is_lower_bound(j, d->i - 1)) {
            return 0;
        }
        if (d->min && cloog_constraint_is_upper_bound(j, d->i - 1)) {
            return 0;
        }
    }

    v = cloog_constraint_variable_expr(j, d->i, d->infos->names);
    d->g->eq[d->n].LHS = &(t = new_clast_term(d->infos->state->one, v))->expr;
    if (individual_constraint) {
        /* put the "denominator" in the LHS */
        cloog_constraint_coefficient_get(j, d->i - 1, &t->val);
        cloog_constraint_coefficient_set(j, d->i - 1, d->infos->state->one);
        if (cloog_int_is_neg(t->val)) {
            cloog_int_neg(t->val, t->val);
            cloog_constraint_coefficient_set(j, d->i - 1, d->infos->state->negone);
        }
        if (d->level || cloog_constraint_is_equality(j)) {
            d->g->eq[d->n].sign = 0;
        } else if (cloog_constraint_is_lower_bound(j, d->i - 1)) {
            d->g->eq[d->n].sign = 1;
        } else {
            d->g->eq[d->n].sign = -1;
        }
        d->g->eq[d->n].RHS = clast_bound_from_constraint(j, d->i, d->infos->names);
    } else {
        int guarded;

        if (cloog_constraint_is_lower_bound(j, d->i - 1)) {
            minmax = 1;
            d->max = 1;
            d->g->eq[d->n].sign = 1;
        } else {
            minmax = 0;
            d->min = 1;
            d->g->eq[d->n].sign = -1;
        }

        guarded = (d->nb_iter >= d->level) ? d->level : 0 ;
        d->g->eq[d->n].RHS = clast_minmax(d->copy,  d->i, minmax, guarded, 0, 1,
                                          d->infos);
    }
    d->n++;

    return 0;
}


/**
 * insert_guard function:
 * This function inserts a guard in the clast.
 * A guard on an element (level) is :
 * -> the conjunction of all the existing constraints where the coefficient of
 *    this element is 0 if the element is an iterator,
 * -> the conjunction of all the existing constraints if the element isn't an
 *    iterator.
 * For instance, considering these constraints and the element j:
 * -3*i +2*j -M >= 0
 *  2*i      +M >= 0
 * this function should return 'if (2*i+M>=0) {'.
 * - matrix is the polyhedron containing all the constraints,
 * - level is the column number of the element in matrix we want to use,
 * - the infos structure gives the user some options about code printing,
 *   the number of parameters in matrix (nb_par), and the arrays of iterator
 *   names and parameters (iters and params).
 **
 * - November  3rd 2001: first version.
 * - November 14th 2001: a lot of 'purifications'.
 * - July     31th 2002: (debug) some guard parts are no more redundants.
 * - August   12th 2002: polyhedra union ('or' conditions) are now supported.
 * - October  27th 2005: polyhedra union ('or' conditions) are no more supported
 *                       (the need came from loop_simplify that may result in
 *                       domain unions, now it should be fixed directly in
 *                       cloog_loop_simplify).
 */
static void insert_guard(CloogConstraintSet *constraints, int level,
                         struct clast_stmt ***next, CloogInfos *infos) {
    int total_dim;
    struct clast_guard_data data = { level, infos, 0 };

    if (!constraints) {
        return;
    }

    data.copy = cloog_constraint_set_copy(constraints);

    data.copy = insert_extra_modulo_guards(data.copy, level, next, infos);

    cloog_constraint_set_foreach_constraint(constraints,
                                            guard_count_bounds, &data);

    data.g = new_clast_guard(data.n);
    data.n = 0;

    /* Well, it looks complicated because I wanted to have a particular, more
     * readable, ordering, obviously this function may be far much simpler !
     */
    data.nb_iter = cloog_constraint_set_n_iterators(constraints,
                   infos->names->nb_parameters);

    /* We search for guard parts. */
    total_dim = cloog_constraint_set_total_dimension(constraints);
    for (data.i = 1; data.i <= total_dim; data.i++) {
        data.min = 0;
        data.max = 0;
        cloog_constraint_set_foreach_constraint(data.copy,
                                                insert_guard_constraint, &data);
    }

    cloog_constraint_set_free(data.copy);

    data.g->n = data.n;
    if (data.n) {
        clast_guard_sort(data.g);
        **next = &data.g->stmt;
        *next = &data.g->then;
    } else {
        free_clast_stmt(&data.g->stmt);
    }
}

/**
 * Check if the constant "cst" satisfies the modulo guard that
 * would be introduced by insert_computed_modulo_guard.
 * The constant is assumed to have been reduced prior to calling
 * this function.
 */
static int constant_modulo_guard_is_satisfied(CloogConstraint *lower,
        cloog_int_t bound, cloog_int_t cst) {
    if (cloog_constraint_is_valid(lower)) {
        return cloog_int_le(cst, bound);
    } else {
        return cloog_int_is_zero(cst);
    }
}

/**
 * Insert a modulo guard "r % mod == 0" or "r % mod <= bound",
 * depending on whether lower represents a valid constraint.
 */
static void insert_computed_modulo_guard(struct clast_reduction *r,
        CloogConstraint *lower, cloog_int_t mod, cloog_int_t bound,
        struct clast_stmt ***next) {
    struct clast_expr *e;
    struct clast_guard *g;

    e = &new_clast_binary(clast_bin_mod, &r->expr, mod)->expr;
    g = new_clast_guard(1);
    if (!cloog_constraint_is_valid(lower)) {
        g->eq[0].LHS = e;
        cloog_int_set_si(bound, 0);
        g->eq[0].RHS = &new_clast_term(bound, NULL)->expr;
        g->eq[0].sign = 0;
    } else {
        g->eq[0].LHS = e;
        g->eq[0].RHS = &new_clast_term(bound, NULL)->expr;
        g->eq[0].sign = -1;
    }

    **next = &g->stmt;
    *next = &g->then;
}


/* Try and eliminate coefficients from a modulo constraint based on
 * stride information of an earlier level.
 * The modulo of the constraint being constructed is "m".
 * The stride information at level "level" is given by "stride"
 * and indicated that the iterator i at level "level" is equal to
 * some expression modulo stride->stride.
 * If stride->stride is a multiple of "m' then i is also equal to
 * the expression modulo m and so we can eliminate the coefficient of i.
 *
 * If stride->constraint is NULL, then i has a constant value modulo m, stored
 * stride->offset.  We simply multiply this constant with the coefficient
 * of i and add the result to the constant term, reducing it modulo m.
 *
 * If stride->constraint is not NULL, then it is a constraint of the form
 *
 *  e + k i = s a
 *
 * with s equal to stride->stride, e an expression in terms of the
 * parameters and earlier iterators and a some arbitrary expression
 * in terms of existentially quantified variables.
 * stride->factor is a value f such that f * k = -1 mod s.
 * Adding stride->constraint f * c times to the current modulo constraint,
 * with c the coefficient of i eliminates i in favor of parameters and
 * earlier variables.
 */
static void eliminate_using_stride_constraint(cloog_int_t *line, int len,
        int nb_iter, CloogStride *stride, int level, cloog_int_t m) {
    if (!stride) {
        return;
    }
    if (!cloog_int_is_divisible_by(stride->stride, m)) {
        return;
    }

    if (stride->constraint) {
        int i, s_len;
        cloog_int_t t, v;

        cloog_int_init(t);
        cloog_int_init(v);
        cloog_int_mul(t, line[level], stride->factor);
        for (i = 1; i < level; ++i) {
            cloog_constraint_coefficient_get(stride->constraint,
                                             i - 1, &v);
            cloog_int_addmul(line[i], t, v);
            cloog_int_fdiv_r(line[i], line[i], m);
        }
        s_len = cloog_constraint_total_dimension(stride->constraint)+2;
        for (i = nb_iter + 1; i <= len - 2; ++i) {
            cloog_constraint_coefficient_get(stride->constraint,
                                             i - (len - s_len) - 1, &v);
            cloog_int_addmul(line[i], t, v);
            cloog_int_fdiv_r(line[i], line[i], m);
        }
        cloog_constraint_constant_get(stride->constraint, &v);
        cloog_int_addmul(line[len - 1], t, v);
        cloog_int_fdiv_r(line[len - 1], line[len - 1], m);
        cloog_int_clear(v);
        cloog_int_clear(t);
    } else {
        cloog_int_addmul(line[len - 1], line[level], stride->offset);
        cloog_int_fdiv_r(line[len - 1], line[len - 1], m);
    }

    cloog_int_set_si(line[level], 0);
}


/* Temporary structure for communication between insert_modulo_guard and
 * its cloog_constraint_set_foreach_constraint callback function.
 */
struct clast_modulo_guard_data {
    CloogConstraint *lower;
    int level;
    struct clast_stmt ***next;
    CloogInfos *infos;
    int empty;
    cloog_int_t val, bound;
};


/* Insert a modulo guard for constraint c.
 * The constraint may be either an equality or an inequality.
 * Since this function returns -1, it is only called on a single constraint.
 * In case of an inequality, the constraint is usually an upper bound
 * on d->level.  However, if this variable is an existentially
 * quantified variable, the upper bound constraint may get removed
 * as trivially holding and then this function is called with
 * a lower bound instead.  In this case, we need to adjust the constraint
 * based on the sum of the constant terms of the lower and upper bound
 * stored in d->bound.
 */
static int insert_modulo_guard_constraint(CloogConstraint *c, void *user) {
    struct clast_modulo_guard_data *d = (struct clast_modulo_guard_data *) user;
    int level = d->level;
    CloogInfos *infos = d->infos;
    int i, nb_elts = 0, len, nb_iter, nb_par;
    int constant;
    struct cloog_vec *line_vector;
    cloog_int_t *line;

    len = cloog_constraint_total_dimension(c) + 2;
    nb_par = infos->names->nb_parameters;
    nb_iter = len - 2 - nb_par;

    line_vector = cloog_vec_alloc(len);
    line = line_vector->p;
    cloog_constraint_copy_coefficients(c, line + 1);

    if (cloog_int_is_pos(line[level])) {
        cloog_seq_neg(line + 1, line + 1, len - 1);
        if (!cloog_constraint_is_equality(c)) {
            cloog_int_add(line[len - 1], line[len - 1], d->bound);
        }
    }

    cloog_int_neg(line[level], line[level]);
    assert(cloog_int_is_pos(line[level]));

    nb_elts = 0;
    for (i = 1; i <= len-1; ++i) {
        if (i == level) {
            continue;
        }
        cloog_int_fdiv_r(line[i], line[i], line[level]);
        if (cloog_int_is_zero(line[i])) {
            continue;
        }
        if (i == len-1) {
            continue;
        }

        nb_elts++;
    }

    if (nb_elts || !cloog_int_is_zero(line[len-1])) {
        struct clast_reduction *r;
        const char *name;

        r = new_clast_reduction(clast_red_sum, nb_elts + 1);
        nb_elts = 0;

        /* First, the modulo guard : the iterators... */
        i = level - 1;
        if (i > infos->stride_level) {
            i = infos->stride_level;
        }
        for (; i >= 1; --i) {
            eliminate_using_stride_constraint(line, len, nb_iter,
                                              infos->stride[i - 1], i, line[level]);
        }
        for (i=1; i<=nb_iter; i++) {
            if (i == level || cloog_int_is_zero(line[i])) {
                continue;
            }

            name = cloog_names_name_at_level(infos->names, i);

            r->elts[nb_elts++] = &new_clast_term(line[i],
                                                 &new_clast_name(name)->expr)->expr;
        }

        /* ...the parameters... */
        for (i=nb_iter+1; i<=len-2; i++) {
            if (cloog_int_is_zero(line[i])) {
                continue;
            }

            name = infos->names->parameters[i-nb_iter-1] ;
            r->elts[nb_elts++] = &new_clast_term(line[i],
                                                 &new_clast_name(name)->expr)->expr;
        }

        constant = nb_elts == 0;
        /* ...the constant. */
        if (!cloog_int_is_zero(line[len-1])) {
            r->elts[nb_elts++] = &new_clast_term(line[len-1], NULL)->expr;
        }

        /* our initial computation may have been an overestimate */
        r->n = nb_elts;

        if (constant) {
            d->empty = !constant_modulo_guard_is_satisfied(d->lower, d->bound,
                       line[len - 1]);
            free_clast_reduction(r);
        } else {
            insert_computed_modulo_guard(r, d->lower, line[level], d->bound,
                                         d->next);
        }
    }

    cloog_vec_free(line_vector);

    return -1;
}


/**
 * insert_modulo_guard:
 * This function inserts a modulo guard corresponding to an equality
 * or a pair of inequalities.
 * Returns 0 if the modulo guard is discovered to be unsatisfiable.
 *
 * See insert_equation.
 * - matrix is the polyhedron containing all the constraints,
 * - upper and lower are the line numbers of the constraint in matrix
 *   we want to print; in particular, if we want to print an equality,
 *   then lower == -1 and upper is the row of the equality; if we want
 *   to print an inequality, then upper is the row of the upper bound
 *   and lower in the row of the lower bound
 * - level is the column number of the element in matrix we want to use,
 * - the infos structure gives the user some options about code printing,
 *   the number of parameters in matrix (nb_par), and the arrays of iterator
 *   names and parameters (iters and params).
 */
static int insert_modulo_guard(CloogConstraint *upper,
                               CloogConstraint *lower, int level,
                               struct clast_stmt ***next, CloogInfos *infos) {
    int nb_par;
    CloogConstraintSet *set;
    struct clast_modulo_guard_data data = { lower, level, next, infos, 0 };

    cloog_int_init(data.val);
    cloog_constraint_coefficient_get(upper, level-1, &data.val);
    if (cloog_int_is_one(data.val) || cloog_int_is_neg_one(data.val)) {
        cloog_int_clear(data.val);
        return 1;
    }

    nb_par = infos->names->nb_parameters;

    cloog_int_init(data.bound);
    /* Check if would be emitting the redundant constraint mod(e,m) <= m-1 */
    if (cloog_constraint_is_valid(lower)) {
        cloog_constraint_constant_get(upper, &data.val);
        cloog_constraint_constant_get(lower, &data.bound);
        cloog_int_add(data.bound, data.val, data.bound);
        cloog_constraint_coefficient_get(lower, level-1, &data.val);
        cloog_int_sub_ui(data.val, data.val, 1);
        if (cloog_int_eq(data.val, data.bound)) {
            cloog_int_clear(data.val);
            cloog_int_clear(data.bound);
            return 1;
        }
    }

    if (cloog_constraint_needs_reduction(upper, level)) {
        set = cloog_constraint_set_for_reduction(upper, lower);
        set = cloog_constraint_set_reduce(set, level, infos->equal,
                                          nb_par, &data.bound);
        cloog_constraint_set_foreach_constraint(set,
                                                insert_modulo_guard_constraint, &data);
        cloog_constraint_set_free(set);
    } else {
        insert_modulo_guard_constraint(upper, &data);
    }

    cloog_int_clear(data.val);
    cloog_int_clear(data.bound);

    return !data.empty;
}


/**
 * We found an equality or a pair of inequalities identifying
 * a loop with a single iteration, but the user wants us to generate
 * a loop anyway, so we do it here.
 */
static int insert_equation_as_loop(CloogDomain *domain, CloogConstraint *upper,
                                   CloogConstraint *lower, int level, struct clast_stmt ***next,
                                   CloogInfos *infos) {
    const char *iterator = cloog_names_name_at_level(infos->names, level);
    struct clast_expr *e1, *e2;
    struct clast_for *f;

    e2 = clast_bound_from_constraint(upper, level, infos->names);
    if (!cloog_constraint_is_valid(lower)) {
        e1 = clast_expr_copy(e2);
    } else {
        e1 = clast_bound_from_constraint(lower, level, infos->names);
    }

    f = new_clast_for(domain, iterator, e1, e2, infos->stride[level-1]);
    **next = &f->stmt;
    *next = &f->body;

    cloog_constraint_release(lower);
    cloog_constraint_release(upper);
    return 1;
}


/**
 * insert_equation function:
 * This function inserts an equality
 * constraint according to an element in the clast.
 * Returns 1 if the calling function should recurse into inner loops.
 *
 * An equality can be preceded by a 'modulo guard'.
 * For instance, consider the constraint i -2*j = 0 and the
 * element j: pprint_equality should return 'if(i%2==0) { j = i/2 ;'.
 * - matrix is the polyhedron containing all the constraints,
 * - num is the line number of the constraint in matrix we want to print,
 * - level is the column number of the element in matrix we want to use,
 * - the infos structure gives the user some options about code printing,
 *   the number of parameters in matrix (nb_par), and the arrays of iterator
 *   names and parameters (iters and params).
 **
 * - November 13th 2001: first version.
 * - June 26th 2003: simplification of the modulo guards (remove parts such as
 *                   modulo is 0, compare vivien or vivien2 with a previous
 *                   version for an idea).
 * - June 29th 2003: non-unit strides support.
 * - July 14th 2003: (debug) no more print the constant in the modulo guard when
 *                   it was previously included in a stride calculation.
 */
static int insert_equation(CloogDomain *domain, CloogConstraint *upper,
                           CloogConstraint *lower, int level, struct clast_stmt
                           ***next, CloogInfos *infos) {
    struct clast_expr *e;
    struct clast_assignment *ass;

    if (!infos->options->otl) {
        return insert_equation_as_loop(domain, upper, lower, level, next, infos);
    }

    if (!insert_modulo_guard(upper, lower, level, next, infos)) {
        cloog_constraint_release(lower);
        cloog_constraint_release(upper);

        return 0;
    }

    if (cloog_constraint_is_valid(lower) ||
            !clast_equal_add(infos->equal, NULL, level, upper, infos)) {
        /* Finally, the equality. */

        /* If we have to make a block by dimension, we start the block. Function
         * pprint knows if there is an equality, if this is the case, it checks
         * for the same following condition to close the brace.
         */
        if (infos->options->block) {
            struct clast_block *b = new_clast_block();
            **next = &b->stmt;
            *next = &b->body;
        }

        e = clast_bound_from_constraint(upper, level, infos->names);
        ass = new_clast_assignment(cloog_names_name_at_level(infos->names, level), e);

        **next = &ass->stmt;
        *next = &(**next)->next;
    }

    cloog_constraint_release(lower);
    cloog_constraint_release(upper);

    return 1;
}


/**
 * Insert a loop that is executed exactly once as an assignment.
 * In particular, the loop
 *
 *  for (i = e; i <= e; ++i) {
 *      S;
 *  }
 *
 * is generated as
 *
 *  i = e;
 *  S;
 *
 */
static void insert_otl_for(CloogConstraintSet *constraints, int level,
                           struct clast_expr *e, struct clast_stmt ***next, CloogInfos *infos) {
    const char *iterator;

    iterator = cloog_names_name_at_level(infos->names, level);

    if (!clast_equal_add(infos->equal, constraints, level,
                         cloog_constraint_invalid(), infos)) {
        struct clast_assignment *ass;
        if (infos->options->block) {
            struct clast_block *b = new_clast_block();
            **next = &b->stmt;
            *next = &b->body;
        }
        ass = new_clast_assignment(iterator, e);

        **next = &ass->stmt;
        *next = &(**next)->next;
    } else {
        free_clast_expr(e);
    }
}


/**
 * Insert a loop that is executed at most once as an assignment followed
 * by a guard.  In particular, the loop
 *
 *  for (i = e1; i <= e2; ++i) {
 *      S;
 *  }
 *
 * is generated as
 *
 *  i = e1;
 *  if (i <= e2) {
 *      S;
 *  }
 *
 */
static void insert_guarded_otl_for(CloogConstraintSet *constraints, int level,
                                   struct clast_expr *e1, struct clast_expr *e2,
                                   struct clast_stmt ***next, CloogInfos *infos) {
    const char *iterator;
    struct clast_assignment *ass;
    struct clast_guard *guard;

    iterator = cloog_names_name_at_level(infos->names, level);

    if (infos->options->block) {
        struct clast_block *b = new_clast_block();
        **next = &b->stmt;
        *next = &b->body;
    }
    ass = new_clast_assignment(iterator, e1);
    **next = &ass->stmt;
    *next = &(**next)->next;

    guard = new_clast_guard(1);
    guard->eq[0].sign = -1;
    guard->eq[0].LHS = &new_clast_term(infos->state->one,
                                       &new_clast_name(iterator)->expr)->expr;
    guard->eq[0].RHS = e2;

    **next = &guard->stmt;
    *next = &guard->then;
}


/**
 * insert_for function:
 * This function inserts a for loop in the clast.
 * Returns 1 if the calling function should recurse into inner loops.
 *
 * A loop header according to an element is the conjunction of a minimum and a
 * maximum on a given element (they give the loop bounds).
 * For instance, considering these constraints and the element j:
 * i + j -9*M >= 0
 *    -j +5*M >= 0
 *     j -4*M >= 0
 * this function should return 'for (j=max(-i+9*M,4*M),j<=5*M;j++) {'.
 * - constraints contains all constraints,
 * - level is the column number of the element in matrix we want to use,
 * - otl is set if the loop is executed at most once,
 * - the infos structure gives the user some options about code printing,
 *   the number of parameters in matrix (nb_par), and the arrays of iterator
 *   names and parameters (iters and params).
 */
static int insert_for(CloogDomain *domain, CloogConstraintSet *constraints,
                      int level, int otl, struct clast_stmt ***next,
                      CloogInfos *infos) {
    const char *iterator;
    struct clast_expr *e1;
    struct clast_expr *e2;

    e1 = clast_minmax(constraints, level, 1, 0, 1, 0, infos);
    e2 = clast_minmax(constraints, level, 0, 0, 0, 0, infos);

    if (clast_expr_is_bigger_constant(e1, e2)) {
        free_clast_expr(e1);
        free_clast_expr(e2);
        return 0;
    }

    /* If min and max are not equal there is a 'for' else, there is a '='.
     * In the special case e1 = e2 = NULL, this is an infinite loop
     * so this is not a '='.
     */
    if (e1 && e2 && infos->options->otl && clast_expr_equal(e1, e2)) {
        free_clast_expr(e2);
        insert_otl_for(constraints, level, e1, next, infos);
    } else if (otl) {
        insert_guarded_otl_for(constraints, level, e1, e2, next, infos);
    } else {
        struct clast_for *f;
        iterator = cloog_names_name_at_level(infos->names, level);

        f = new_clast_for(domain, iterator, e1, e2, infos->stride[level-1]);
        **next = &f->stmt;
        *next = &f->body;
    }

    return 1;
}


/**
 * insert_block function:
 * This function inserts a statement block.
 * - block is the statement block,
 * - level is the number of loops enclosing the statement,
 * - the infos structure gives the user some options about code printing,
 *   the number of parameters in domain (nb_par), and the arrays of iterator
 *   names and parameters (iters and params).
 **
 * - September 21th 2003: first version (pick from pprint function).
 */
static void insert_block(CloogDomain *domain, CloogBlock *block, int level,
                         struct clast_stmt ***next, CloogInfos *infos) {
    CloogStatement * statement ;
    struct clast_stmt *subs;

    if (!block) {
        return;
    }

    for (statement = block->statement; statement; statement = statement->next) {
        CloogStatement *s_next = statement->next;

        subs = clast_equal(level,infos);

        statement->next = NULL;
        **next = &new_clast_user_stmt(domain, statement, subs)->stmt;
        statement->next = s_next;
        *next = &(**next)->next;
    }
}


/**
 * insert_loop function:
 * This function converts the content of a CloogLoop structure (loop) into a
 * clast_stmt (inserted at **next).
 * The iterator (level) of
 * the current loop is given by 'level': this is the column number of the
 * domain corresponding to the current loop iterator. The data of a loop are
 * written in this order:
 * 1. The guard of the loop, i.e. each constraint in the domain that does not
 *    depend on the iterator (when the entry in the column 'level' is 0).
 * 2. The iteration domain of the iterator, given by the constraints in the
 *    domain depending on the iterator, i.e.:
 *    * an equality if the iterator has only one value (possibly preceded by
 *      a guard verifying if this value is integral), *OR*
 *    * a loop from the minimum possible value of the iterator to the maximum
 *      possible value.
 * 3. The included statement block.
 * 4. The inner loops (recursive call).
 * 5. The following loops (recursive call).
 * - level is the recursion level or the iteration level that we are printing,
 * - the infos structure gives the user some options about code printing,
 *   the number of parameters in domain (nb_par), and the arrays of iterator
 *   names and parameters (iters and params).
 **
 * - November   2nd 2001: first version.
 * - March      6th 2003: infinite domain support.
 * - April     19th 2003: (debug) NULL loop support.
 * - June      29th 2003: non-unit strides support.
 * - April     28th 2005: (debug) level is level+equality when print statement!
 * - June      16th 2005: (debug) the N. Vasilache normalization step has been
 *                        added to avoid iteration duplication (see DaeGon Kim
 *                        bug in cloog_program_generate). Try vasilache.cloog
 *                        with and without the call to cloog_polylib_matrix_normalize,
 *                        using -f 8 -l 9 options for an idea.
 * - September 15th 2005: (debug) don't close equality braces when unnecessary.
 * - October   16th 2005: (debug) scalar value is saved for next loops.
 */
static void insert_loop(CloogLoop * loop, int level,
                        struct clast_stmt ***next, CloogInfos *infos) {
    int equality = 0;
    CloogConstraintSet *constraints, *temp;
    struct clast_stmt **top = *next;
    CloogConstraint *i, *j;
    int empty_loop = 0;

    /* It can happen that loop be NULL when an input polyhedron is empty. */
    if (loop == NULL) {
        return;
    }

    /* The constraints do not always have a shape that allows us to generate code from it,
    * thus we normalize it, we also simplify it with the equalities.
    */
    temp = cloog_domain_constraints(loop->domain);
    cloog_constraint_set_normalize(temp,level);
    constraints = cloog_constraint_set_simplify(temp,infos->equal,level,
                  infos->names->nb_parameters);
    cloog_constraint_set_free(temp);
    if (level) {
        infos->stride[level - 1] = loop->stride;
        infos->stride_level++;
    }

    /* First of all we have to print the guard. */
    insert_guard(constraints,level, next, infos);

    if (level && cloog_constraint_set_contains_level(constraints, level,
            infos->names->nb_parameters)) {
        /* We scan all the constraints to know in which case we are :
         * [[if] equation] or [for].
         */
        if (cloog_constraint_is_valid(i =
                                          cloog_constraint_set_defining_equality(constraints, level))) {
            empty_loop = !insert_equation(loop->unsimplified, i,
                                          cloog_constraint_invalid(), level, next,
                                          infos);
            equality = 1 ;
        } else if (cloog_constraint_is_valid(i =
                cloog_constraint_set_defining_inequalities(constraints,
                        level, &j, infos->names->nb_parameters))) {
            empty_loop = !insert_equation(loop->unsimplified, i, j, level, next,
                                          infos);
        } else {
            empty_loop = !insert_for(loop->unsimplified, constraints, level,
                                     loop->otl, next, infos);
        }
    }

    if (!empty_loop) {
        /* Finally, if there is an included statement block, print it. */
        insert_block(loop->unsimplified, loop->block, level+equality, next, infos);

        /* Go to the next level. */
        if (loop->inner != NULL) {
            insert_loop(loop->inner, level+1, next, infos);
        }
    }

    if (level) {
        cloog_equal_del(infos->equal,level);
        infos->stride_level--;
    }
    cloog_constraint_set_free(constraints);

    /* Go to the next loop on the same level. */
    while (*top) {
        top = &(*top)->next;
    }
    if (loop->next != NULL) {
        insert_loop(loop->next, level, &top,infos);
    }
}


struct clast_stmt *cloog_clast_create(CloogProgram *program,
                                      CloogOptions *options) {
    CloogInfos *infos = ALLOC(CloogInfos);
    int nb_levels;
    struct clast_stmt *root = &new_clast_root(program->names)->stmt;
    struct clast_stmt **next = &root->next;

    infos->state      = options->state;
    infos->names    = program->names;
    infos->options  = options;
    infos->scaldims = program->scaldims;
    infos->nb_scattdims = program->nb_scattdims;

    /* Allocation for the array of strides, there is a +1 since the statement can
    * be included inside an external loop without iteration domain.
    */
    nb_levels = program->names->nb_scattering+program->names->nb_iterators+1;
    infos->stride = ALLOCN(CloogStride *, nb_levels);
    infos->stride_level = 0;

    infos->equal = cloog_equal_alloc(nb_levels,
                                     nb_levels, program->names->nb_parameters);

    insert_loop(program->loop, 0, &next, infos);

    cloog_equal_free(infos->equal);

    free(infos->stride);
    free(infos);

    return root;
}


struct clast_stmt *cloog_clast_create_from_input(CloogInput *input,
        CloogOptions *options) {
    CloogProgram *program;
    struct clast_stmt *root;

    program = cloog_program_alloc(input->context, input->ud, options);
    free(input);

    program = cloog_program_generate(program, options);

    root = cloog_clast_create(program, options);
    cloog_program_free(program);

    return root;
}

/* Adds to the list if not already in it */
static int add_if_new(void **list, int num, void *new, int size) {
    int i;

    for (i=0; i<num; i++) {
        if (!memcmp((*list) + i*size, new, size)) break;
    }

    if (i==num) {
        *list = realloc(*list, (num+1)*size);
        memcpy(*list + num*size, new, size);
        return 1;
    }

    return 0;
}


/* Concatenates all elements of list2 that are not in list1;
 * Returns the new size of the list */
int concat_if_new(void **list1, int num1, void *list2, int num2, int size) {
    int i, ret;

    for (i=0; i<num2; i++) {
        ret = add_if_new(list1, num1, (char *)list2 + i*size, size);
        if (ret) num1++;
    }

    return num1;
}

/* Compares list1 to list2
 * Returns 0 if both have the same elements; returns -1 if all elements of
 * list1 are strictly contained in list2; 1 otherwise
 */
int list_compare(const int *list1, int num1, const int *list2, int num2) {
    int i, j;

    for (i=0; i<num1; i++) {
        for (j=0; j<num2; j++) {
            if (list1[i] == list2[j]) break;
        }
        if (j==num2) break;
    }
    if (i==num1) {
        if (num1 == num2) {
            return 0;
        }
        return -1;
    }

    return 1;
}



/*
 * A multi-purpose function to traverse and get information on Clast
 * loops
 *
 * node: clast node where processing should start
 *
 * Returns:
 * A list of loops under clast_stmt 'node' filtered in two ways: (1) it contains
 * statements appearing in 'stmts_filter', (2) loop iterator's name is 'iter'
 * If iter' is set to NULL, no filtering based on iterator name is done
 *
 * iter: loop iterator name
 * stmts_filter: list of statement numbers for filtering (1-indexed)
 * nstmts_filter: number of statements in stmts_filter
 *
 * FilterType: match exact (i.e., loops containing only and all those statements
 * in stmts_filter) or subset, i.e., loops which have only those statements
 * that appear in stmts_filter
 *
 * To disable all filtering, set 'iter' to NULL, provide all statement
 * numbers in 'stmts_filter' and set FilterType to subset
 *
 * Return fields
 *
 * stmts: an array of statement numbers under node
 * nstmts: number of stmt numbers pointed to by stmts
 * loops: list of clast loops
 * nloops: number of clast loops in loops
 *
 */
void clast_filter(struct clast_stmt *node,
                  ClastFilter filter,
                  struct clast_for ***loops, int *nloops,
                  int **stmts, int *nstmts) {
    int num_next_stmts, num_next_loops, ret, *stmts_next;
    struct clast_for **loops_next;

    *loops = NULL;
    *nloops = 0;
    *nstmts = 0;
    *stmts = NULL;

    if (node == NULL) {
        return;
    }

    ClastFilterType filter_type = filter.filter_type;
    const char *iter = filter.iter;
    int nstmts_filter = filter.nstmts_filter;
    const int *stmts_filter = filter.stmts_filter;

    if (CLAST_STMT_IS_A(node, stmt_root)) {
        // printf("root stmt\n");
        struct clast_root *root = (struct clast_root *) node;
        clast_filter((root->stmt).next, filter, &loops_next,
                     &num_next_loops, &stmts_next, &num_next_stmts);
        *nstmts = concat_if_new((void **)stmts, *nstmts, stmts_next, num_next_stmts, sizeof(int));
        *nloops = concat_if_new((void **)loops, *nloops, loops_next, num_next_loops,
                                sizeof(struct clast_stmt *));
        free(loops_next);
        free(stmts_next);
    }

    if (CLAST_STMT_IS_A(node, stmt_guard)) {
        // printf("guard stmt\n");
        struct clast_guard *guard = (struct clast_guard *) node;
        clast_filter(guard->then, filter, &loops_next,
                     &num_next_loops, &stmts_next, &num_next_stmts);
        *nstmts = concat_if_new((void **)stmts, *nstmts, stmts_next, num_next_stmts, sizeof(int));
        *nloops = concat_if_new((void **)loops, *nloops, loops_next, num_next_loops,
                                sizeof(struct clast_stmt *));
        free(loops_next);
        free(stmts_next);
        clast_filter((guard->stmt).next, filter, &loops_next,
                     &num_next_loops, &stmts_next, &num_next_stmts);
        *nstmts = concat_if_new((void **)stmts, *nstmts, stmts_next, num_next_stmts, sizeof(int));
        *nloops = concat_if_new((void **)loops, *nloops, loops_next, num_next_loops,
                                sizeof(struct clast_stmt *));
        free(loops_next);
        free(stmts_next);
    }

    if (CLAST_STMT_IS_A(node, stmt_user)) {
        struct clast_user_stmt *user_stmt = (struct clast_user_stmt *) node;
        // printf("user stmt: S%d\n", user_stmt->statement->number);
        ret = add_if_new((void **)stmts, *nstmts, &user_stmt->statement->number, sizeof(int));
        if (ret) (*nstmts)++;
        clast_filter((user_stmt->stmt).next, filter, &loops_next,
                     &num_next_loops, &stmts_next, &num_next_stmts);
        *nstmts = concat_if_new((void **)stmts, *nstmts, stmts_next, num_next_stmts, sizeof(int));
        *nloops = concat_if_new((void **)loops, *nloops, loops_next, num_next_loops,
                                sizeof(struct clast_stmt *));
        free(loops_next);
        free(stmts_next);
    }
    if (CLAST_STMT_IS_A(node, stmt_for)) {
        struct clast_for *for_stmt = (struct clast_for *) node;
        clast_filter(for_stmt->body, filter, &loops_next,
                     &num_next_loops, &stmts_next, &num_next_stmts);
        *nstmts = concat_if_new((void **)stmts, *nstmts, stmts_next, num_next_stmts, sizeof(int));
        *nloops = concat_if_new((void **)loops, *nloops, loops_next, num_next_loops,
                                sizeof(struct clast_stmt *));

        if (iter == NULL || !strcmp(for_stmt->iterator, iter)) {
            if (stmts_filter == NULL ||
                    (filter_type == subset && list_compare(stmts_next, num_next_stmts,
                            stmts_filter, nstmts_filter) <= 0)
                    || (filter_type == exact && list_compare(stmts_next, num_next_stmts,
                            stmts_filter, nstmts_filter) == 0 )) {
                ret = add_if_new((void **)loops, *nloops, &for_stmt, sizeof(struct clast_for *));
                if (ret) (*nloops)++;
            }
        }
        free(loops_next);
        free(stmts_next);

        clast_filter((for_stmt->stmt).next, filter, &loops_next,
                     &num_next_loops, &stmts_next, &num_next_stmts);
        *nstmts = concat_if_new((void **)stmts, *nstmts, stmts_next, num_next_stmts, sizeof(int));
        *nloops = concat_if_new((void **)loops, *nloops, loops_next, num_next_loops,
                                sizeof(struct clast_stmt *));
        free(loops_next);
        free(stmts_next);
    }
}
