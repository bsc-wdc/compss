/*
 * Copyright (c) 2015, Maxime SCHMITT
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA  02110-1301  USA
 *
 */

/*
 * This program generates a c language program which once compiled will scan
 * part of a parameter domain for a given cloog problem.
 */

#include <stdlib.h>
#include <stdio.h>
#include <cloog/cloog.h>
#include <stdbool.h>
#if _POSIX_C_SOURCE >=2 || _XOPEN_SOURCE
#define GETOPT_DISPO
#include <getopt.h>
#endif

#define LOWERBOUND 0
#define UPPERBOUND 1

#ifdef DEBUG
CloogOptions *options;
#endif


struct name_list {
    unsigned nb_names;
    char **names;
};

struct bounds {
    struct name_list names;
    struct {
        struct cloog_vec *value;
    } upperbound;
    struct {
        struct cloog_vec *value;
    } lowerbound;
};

static void print_bounds(FILE *out, struct bounds *bounds) {
    unsigned end = bounds->names.nb_names;
    unsigned i;

    for (i = 0; i < end; ++i) {
        fprintf(out, "Upperbound %s : ", bounds->names.names[i]);
        cloog_int_print(out, bounds->upperbound.value->p[i]);
        fprintf(out, "\n");
        fprintf(out, "Lowerbound %s : ", bounds->names.names[i]);
        cloog_int_print(out, bounds->lowerbound.value->p[i]);
        fprintf(out, "\n");
    }
}


#define get_lowerbound(bounds,where) ( (bounds)->lowerbound.value->p[(where)] )
#define get_upperbound(bounds,where) ( (bounds)->upperbound.value->p[(where)] )

static unsigned is_present(const struct name_list nl, const char *name) {
    unsigned i;
    for (i = 0; i < nl.nb_names; ++i) {
        if (nl.names[i] == name)
            break;
    }
    if (i == nl.nb_names)
        return 0;
    return i+1;
}

static inline void set_upperbound(struct bounds *bounds, unsigned where,
                                  cloog_int_t val) {

    cloog_int_set(get_upperbound(bounds, where), val);
}

static inline void set_lowerbound(struct bounds *bounds, unsigned where,
                                  cloog_int_t val) {

    cloog_int_set(get_lowerbound(bounds,where), val);
}

static struct bounds* init_bounds(struct name_list names, cloog_int_t lowerbound,
                                  cloog_int_t upperbound) {

    unsigned i;
    struct bounds *bounds = malloc(sizeof(*bounds));
    if (bounds) {
        bounds->names = names;
        bounds->upperbound.value = cloog_vec_alloc(names.nb_names);
        bounds->lowerbound.value = cloog_vec_alloc(names.nb_names);
        for (i = 0; i < names.nb_names; ++i) {
            set_lowerbound(bounds, i, lowerbound);
            set_upperbound(bounds, i, upperbound);
        }
        return bounds;
    } else
        return NULL;
}


static void free_bounds(struct bounds *bounds) {
    cloog_vec_free(bounds->upperbound.value);
    cloog_vec_free(bounds->lowerbound.value);
    free(bounds);
}

static bool get_expression_bound(struct clast_expr *expr,
                                 struct bounds *param_bounds,
                                 char which_bound,
                                 cloog_int_t *bound);

static bool get_expression_term_bound(struct clast_term *term,
                                      struct bounds *bounds,
                                      char which_bound,
                                      cloog_int_t *bound) {

    if (term->var) {
        if (!get_expression_bound(term->var, bounds, which_bound, bound))
            return false;
        cloog_int_mul(*bound, *bound, term->val);
    } else {
        cloog_int_set(*bound, term->val);
    }
    return true;
}

static bool get_expression_name_bound(struct clast_name *name,
                                      struct bounds *bounds,
                                      char which_bound,
                                      cloog_int_t *bound) {

    unsigned where;
    where = is_present(bounds->names, name->name);
    if(!where)
        return false;
    else
        --where;
    if (which_bound == LOWERBOUND) {
        cloog_int_set(*bound, get_lowerbound(bounds, where));
    } else {
        cloog_int_set(*bound, get_upperbound(bounds, where));
    }
    return true;
}

static bool get_expression_reduction_sum_bound(struct clast_reduction *reduc,
        struct bounds *bounds,
        char which_bound,
        cloog_int_t *bound) {

    cloog_int_t temp, zero;
    cloog_int_init(temp);
    cloog_int_init(zero);
    int i;
    bool valid;
    struct clast_term *t;

    t = (struct clast_term*) reduc->elts[0];
    valid = get_expression_term_bound(t, bounds, which_bound,
                                      bound);

    for (i=1; valid && i < reduc->n; ++i) {
        cloog_int_set(temp, zero);
        t = (struct clast_term*) reduc->elts[i];
        valid = get_expression_term_bound(t, bounds, which_bound,
                                          &temp);
        cloog_int_add(*bound, *bound, temp);
    }

    cloog_int_clear(temp);
    cloog_int_clear(zero);

    return valid;
}

static bool get_expression_reduction_min_max_bound(struct clast_reduction *reduc,
        struct bounds *bounds,
        char which_bound,
        cloog_int_t *bound,
        bool min) {

    cloog_int_t temp, zero;
    cloog_int_init(temp);
    cloog_int_init(zero);
    int i;
    bool valid;
    valid = get_expression_bound(reduc->elts[0], bounds, which_bound,
                                 bound);
    for (i=1; valid && i < reduc->n; ++i) {
        cloog_int_set(temp, zero);
        valid = get_expression_bound(reduc->elts[i], bounds, which_bound,
                                     &temp);
        if (min) {
            if (cloog_int_lt(temp, *bound))
                cloog_int_set(*bound, temp);
        } else { // max
            if (cloog_int_gt(temp, *bound))
                cloog_int_set(*bound, temp);
        }
    }

    cloog_int_clear(temp);
    cloog_int_clear(zero);

    return valid;
}


static bool get_expression_reduction_bound(struct clast_reduction *reduc,
        struct bounds *bounds,
        char which_bound,
        cloog_int_t *bound) {

    bool valid;
    switch (reduc->type) {
    case clast_red_sum:
        valid = get_expression_reduction_sum_bound(reduc, bounds,
                which_bound, bound);
        break;
    case clast_red_max:
        valid = get_expression_reduction_min_max_bound(reduc, bounds,
                which_bound, bound, false);
        break;
    case clast_red_min:
        valid = get_expression_reduction_min_max_bound(reduc, bounds,
                which_bound, bound, true);
        break;
    }
    return valid;
}

static bool get_expression_bin_bound(struct clast_binary *bin,
                                     struct bounds *bounds,
                                     char which_bound,
                                     cloog_int_t *bound) {

    bool valid;

    switch (bin->type) {
    case clast_bin_div:
        valid = get_expression_bound(bin->LHS, bounds, which_bound, bound);
        cloog_int_tdiv_q(*bound, *bound, bin->RHS);
        break;
    case clast_bin_cdiv:
        valid = get_expression_bound(bin->LHS, bounds, which_bound, bound);
        cloog_int_cdiv_q(*bound, *bound, bin->RHS);
        break;
    case clast_bin_fdiv:
        valid = get_expression_bound(bin->LHS, bounds, which_bound, bound);
        cloog_int_fdiv_q(*bound, *bound, bin->RHS);
        break;
    case clast_bin_mod:
        valid = get_expression_bound(bin->LHS, bounds, which_bound, bound);
        cloog_int_fdiv_r(*bound, *bound, bin->RHS);
        break;
    }
    return valid;
}

static bool update_expression_bound(struct clast_expr *expr,
                                    cloog_int_t value, struct bounds *bounds, char which_bound,
                                    cloog_int_t div);

static bool get_expression_bound(struct clast_expr *expr,
                                 struct bounds *param_bounds,
                                 char which_bound,
                                 cloog_int_t *bound) {

    bool valid;
    switch (expr->type) {
    case clast_expr_name :
        valid = get_expression_name_bound((struct clast_name*) expr,
                                          param_bounds, which_bound, bound);
#ifdef DEBUG
        if(!valid) {
            fprintf(stderr,
                    "\x1b[1m\x1b[35mWarning: Cannot compute the bound of a name expression\x1b[0m\n");
            fprintf(stderr, "==== Begin name expression dump ====\n");
            clast_pprint_expr(options, stderr, expr);
            fprintf(stderr, "\n==== End expression dump ====\n");
        }
#endif
        break;
    case clast_expr_bin :
        valid = get_expression_bin_bound((struct clast_binary*) expr,
                                         param_bounds, which_bound, bound);
#ifdef DEBUG
        if(!valid) {
            fprintf(stderr,
                    "\x1b[1m\x1b[35mWarning: Cannot compute the bound of a binary expression\x1b[0m\n");
            fprintf(stderr, "==== Begin binary expression expression dump ====\n");
            clast_pprint_expr(options, stderr, expr);
            fprintf(stderr, "\n==== End expression dump ====\n");
        }
#endif
        break;
    case clast_expr_term :
        valid = get_expression_term_bound((struct clast_term*) expr,
                                          param_bounds, which_bound, bound);
#ifdef DEBUG
        if(!valid) {
            fprintf(stderr,
                    "\x1b[1m\x1b[35mWarning: Cannot compute the bound of a terminal expression\x1b[0m\n");
            fprintf(stderr, "==== Begin terminal expression dump ====\n");
            clast_pprint_expr(options, stderr, expr);
            fprintf(stderr, "\n==== End expression dump ====\n");
        }
#endif
        break;
    case clast_expr_red :
        valid = get_expression_reduction_bound((struct clast_reduction*) expr,
                                               param_bounds, which_bound, bound);
#ifdef DEBUG
        if(!valid) {
            fprintf(stderr,
                    "\x1b[1m\x1b[35mWarning: Cannot compute the bound of a reduction expression\x1b[0m\n");
            fprintf(stderr, "==== Begin reduction expression dump ====\n");
            clast_pprint_expr(options, stderr, expr);
            fprintf(stderr, "\n==== End expression dump ====\n");
        }
#endif
        break;
    }
    return valid;
}

static bool update_expr_name_bound(struct clast_name *name, cloog_int_t value,
                                   struct bounds *bounds, char which_bound, cloog_int_t div) {

    unsigned where;
    where = is_present(bounds->names, name->name);
    if(!where)
        return false;
    else
        --where;

    if (which_bound == LOWERBOUND) {
        if (!cloog_int_is_one(div)) {
            cloog_int_t tempval;
            cloog_int_init(tempval);
            cloog_int_fdiv_q(tempval, value, div);
            set_lowerbound(bounds, where, tempval);
            cloog_int_clear(tempval);
        } else
            set_lowerbound(bounds, where, value);

#ifdef DEBUG
        printf("Lower of %s updated to ", bounds->names.names[where]);
        cloog_int_print(stdout, get_lowerbound(bounds, where));
        printf("\n");
#endif

    } else {
        if (!cloog_int_is_one(div)) {
            cloog_int_t tempval;
            cloog_int_init(tempval);
            cloog_int_cdiv_q(tempval, value, div);
            set_upperbound(bounds, where, tempval);
            cloog_int_clear(tempval);
        } else
            set_upperbound(bounds, where, value);

#ifdef DEBUG
        printf("Upper of %s updated to ", bounds->names.names[where]);
        cloog_int_print(stdout, get_upperbound(bounds, where));
        printf("\n");
#endif

    }
    return true;
}

static bool update_expr_term_bound(struct clast_term *term, cloog_int_t value,
                                   struct bounds *bounds, char which_bound, cloog_int_t div) {

    bool retval;

    if (!term->var) {
#ifdef DEBUG
        fprintf(stderr,
                "\x1b[1m\x1b[35mWarning : Attempt to assign a bound to an integer\x1b[0m\n");
#endif
        return false;
    }
    if (term->var) {
        if (!cloog_int_is_one(term->val)) {
            cloog_int_t tempdiv;
            cloog_int_init(tempdiv);
            cloog_int_mul(tempdiv, div, term->val);
            retval = update_expression_bound(term->var, value, bounds, which_bound,
                                             tempdiv);
            cloog_int_clear(tempdiv);
        } else
            retval = update_expression_bound(term->var, value, bounds, which_bound, div);

        return retval;
    } else {
        return false;
    }
}

static bool update_expr_bin_bound(struct clast_binary *binary,
                                  cloog_int_t value, struct bounds *bounds, char which_bound,
                                  cloog_int_t div) {

    switch (binary->type) {
    case clast_bin_div:
        break;
    case clast_bin_mod:
        return update_expression_bound(binary->LHS, value, bounds, which_bound,
                                       div);
        break;
    case clast_bin_cdiv:
        break;
    case clast_bin_fdiv:
        break;
    }

#ifdef DEBUG
    fprintf(stderr,
            "\x1b[1m\x1b[35mWarning : Trying update binary expression bound\x1b[0m\n");
    fprintf(stderr, "==== Begin binary expression dump ====\n");
    clast_pprint_expr(options, stderr, (struct clast_expr*) binary);
    fprintf(stderr, "\n==== End binary expression dump ====\n");
#endif
    return false;
}

static bool update_expr_red_sum(struct clast_reduction *reduction,
                                cloog_int_t value, struct bounds *bounds, char which_bound,
                                cloog_int_t div) {

    bool changed = false;
    bool valid;
    int i, j;
    cloog_int_t temp_val, expression_bound;

    cloog_int_init(temp_val);
    cloog_int_init(expression_bound);

    for (i = 0; i < reduction->n; ++i) {
        cloog_int_set(temp_val, value);
        cloog_int_set_si(expression_bound, 0);
        struct clast_term *term = (struct clast_term*) reduction->elts[i];
        for (j = 0; j < i; ++j) {
            valid = get_expression_term_bound(
                        (struct clast_term*) reduction->elts[j],
                        bounds,
                        which_bound,
                        &expression_bound);
            if (!valid)
                break;
            cloog_int_sub(temp_val, temp_val, expression_bound);
        }
        if (!valid)
            break;
        for (j = i+1; j < reduction->n; ++j) {
            valid = get_expression_term_bound(
                        (struct clast_term*) reduction->elts[j],
                        bounds,
                        which_bound,
                        &expression_bound);
            if (!valid)
                break;
            cloog_int_sub(temp_val, temp_val, expression_bound);
        }
        if (!valid)
            break;

        cloog_int_set_si(expression_bound, 0);
        valid = get_expression_term_bound(
                    (struct clast_term*) reduction->elts[i],
                    bounds,
                    which_bound,
                    &expression_bound);
        if (!valid)
            break;

        if (which_bound == LOWERBOUND) {
            if (cloog_int_gt(expression_bound, temp_val)) {
                valid = update_expr_term_bound(
                            term,
                            temp_val,
                            bounds,
                            which_bound,
                            div);
                if (valid)
                    changed = true;
            } else {
                valid = true;
            }
        } else {
            if (cloog_int_lt(expression_bound, temp_val)) {
                valid = update_expr_term_bound(
                            term,
                            temp_val,
                            bounds,
                            which_bound,
                            div);
                if (valid)
                    changed = true;
            } else {
                valid = true;
            }
        }
        if (!valid)
            break;
    }

    cloog_int_clear(temp_val);
    cloog_int_clear(expression_bound);

    return changed;
}

static bool update_expr_red_min_max(struct clast_reduction *reduction,
                                    cloog_int_t value, struct bounds *bounds, char which_bound,
                                    cloog_int_t div) {

    bool changed = false;
    bool valid;
    int i;
    cloog_int_t expression_bound;

    cloog_int_init(expression_bound);

    for (i = 0; i < reduction->n; ++i) {
        struct clast_term *term = (struct clast_term*) reduction->elts[i];
        cloog_int_set_si(expression_bound, 0);
        valid = get_expression_term_bound(
                    (struct clast_term*) reduction->elts[i],
                    bounds,
                    which_bound,
                    &expression_bound);
        if (!valid)
            break;
        if (which_bound == LOWERBOUND) {
            if (cloog_int_gt(expression_bound, value)) {
                valid = update_expr_term_bound(
                            term,
                            value,
                            bounds,
                            which_bound,
                            div);
                if (valid)
                    changed = true;
            } else {
                valid = true;
            }
        } else {
            if (cloog_int_lt(expression_bound, value)) {
                valid = update_expr_term_bound(
                            term,
                            value,
                            bounds,
                            which_bound,
                            div);
                if (valid)
                    changed = true;
            } else {
                valid = true;
            }
        }
        if (!valid)
            break;
    }

    cloog_int_clear(expression_bound);

    return changed;
}

static bool update_expr_red_bound(struct clast_reduction *reduction,
                                  cloog_int_t value, struct bounds *bounds, char which_bound,
                                  cloog_int_t div) {

    bool retval;

    switch (reduction->type) {
    case clast_red_sum:
        retval = update_expr_red_sum(reduction, value, bounds, which_bound, div);
        break;
    case clast_red_max:
    case clast_red_min:
        retval = update_expr_red_min_max(reduction, value, bounds, which_bound,
                                         div);
        break;
    }

    return retval;
}

static bool update_expression_bound(struct clast_expr *expr,
                                    cloog_int_t value, struct bounds *bounds, char which_bound,
                                    cloog_int_t div) {

    switch (expr->type) {
    case clast_expr_name :
        return update_expr_name_bound((struct clast_name*) expr, value, bounds,
                                      which_bound, div);
        break;
    case clast_expr_bin :
        return update_expr_bin_bound((struct clast_binary*) expr, value, bounds,
                                     which_bound, div);
        break;
    case clast_expr_term :
        return update_expr_term_bound((struct clast_term*) expr, value, bounds,
                                      which_bound, div);
        break;
    case clast_expr_red :
        return update_expr_red_bound((struct clast_reduction*) expr, value,
                                     bounds, which_bound, div);
        break;
    }
    return false;
}

static bool update_bounds_with_equation(struct bounds *bounds,
                                        struct clast_equation *equation,
                                        cloog_int_t margin) {

    bool updated = false;
    cloog_int_t lowerbound_rhs, upperbound_rhs, lowerbound_lhs, upperbound_lhs,
                extended_lower_rhs, extended_upper_rhs, div;

    cloog_int_init(lowerbound_rhs);
    cloog_int_init(upperbound_rhs);
    cloog_int_init(lowerbound_lhs);
    cloog_int_init(upperbound_lhs);
    cloog_int_init(extended_lower_rhs);
    cloog_int_init(extended_upper_rhs);
    cloog_int_init(div);

    if (!get_expression_bound(equation->RHS, bounds, UPPERBOUND,
                              &upperbound_rhs)) {
        return false;
    }
    if (!get_expression_bound(equation->RHS, bounds, LOWERBOUND,
                              &lowerbound_rhs)) {
        return false;
    }
    if (!get_expression_bound(equation->LHS, bounds, UPPERBOUND,
                              &upperbound_lhs)) {
        return false;
    }
    if (!get_expression_bound(equation->LHS, bounds, LOWERBOUND,
                              &lowerbound_lhs)) {
        return false;
    }

    cloog_int_sub(extended_upper_rhs, upperbound_rhs, margin);
    cloog_int_add(extended_lower_rhs, lowerbound_rhs, margin);

#ifdef DEBUG
    printf("Upper x :");
    cloog_int_print(stdout, upperbound_lhs);
    printf("\n");
    printf("Lower x :");
    cloog_int_print(stdout, lowerbound_lhs);
    printf("\n");
    printf("Lower y + marge :");
    cloog_int_print(stdout, extended_lower_rhs);
    printf("\n");
    printf("Upper y - marge :");
    cloog_int_print(stdout, extended_upper_rhs);
    printf("\n");
#endif

    if (cloog_int_gt(extended_lower_rhs, upperbound_lhs)) {
        cloog_int_read(div, "1");
        updated = update_expression_bound(equation->LHS, extended_lower_rhs, bounds,
                                          UPPERBOUND, div);
    }

    if (cloog_int_lt(extended_upper_rhs, lowerbound_lhs)) {
        cloog_int_read(div, "1");
        updated = updated || update_expression_bound(equation->LHS,
                  extended_upper_rhs, bounds, LOWERBOUND, div);
    }

    if (equation->sign == 0) { // ==
        if (cloog_int_lt(upperbound_lhs, upperbound_rhs)) {
            cloog_int_read(div, "1");
            updated = updated || update_expression_bound(equation->LHS,
                      upperbound_rhs, bounds, UPPERBOUND, div);
        }
        if (cloog_int_gt(lowerbound_lhs, lowerbound_rhs)) {
            cloog_int_read(div, "1");
            updated = updated || update_expression_bound(equation->LHS,
                      lowerbound_rhs, bounds, LOWERBOUND, div);
        }
    } else if (equation->sign > 0) { // >=
        if (cloog_int_lt(upperbound_lhs, upperbound_rhs)) {
            cloog_int_read(div, "1");
            updated = updated || update_expression_bound(equation->LHS,
                      upperbound_rhs, bounds, UPPERBOUND, div);
        }
    } else { // <=
        if (cloog_int_gt(lowerbound_lhs, lowerbound_rhs)) {
            cloog_int_read(div, "1");
            updated = updated || update_expression_bound(equation->LHS,
                      lowerbound_rhs, bounds, LOWERBOUND, div);
        }
    }

    cloog_int_clear(lowerbound_rhs);
    cloog_int_clear(upperbound_rhs);
    cloog_int_clear(lowerbound_lhs);
    cloog_int_clear(upperbound_lhs);
    cloog_int_clear(extended_lower_rhs);
    cloog_int_clear(extended_upper_rhs);
    cloog_int_clear(div);

    return updated;
}

static bool look_for_bounds_in_ast(struct clast_stmt *stmt,
                                   struct bounds *param_bounds,
                                   cloog_int_t margin) {

    bool modified = false;

    for ( ; stmt; stmt = stmt->next) {
        if (CLAST_STMT_IS_A(stmt, stmt_root))
            continue;

        if (CLAST_STMT_IS_A(stmt, stmt_user))
            continue;

        if (CLAST_STMT_IS_A(stmt, stmt_for))
            continue;

        if (CLAST_STMT_IS_A(stmt, stmt_guard)) {
            struct clast_guard *guard_stmt = (struct clast_guard*) stmt;
            int i;
            bool changes = true;
            while (changes) {
                changes = false;
                for (i = 0; i < guard_stmt->n; ++i) {
                    changes = changes || update_bounds_with_equation(param_bounds,
                              &guard_stmt->eq[i], margin);
                }
                modified = modified || changes;
            }
            modified = modified || look_for_bounds_in_ast(guard_stmt->then, param_bounds, margin);
        } else if (CLAST_STMT_IS_A(stmt, stmt_block)) {
            struct clast_block *block_stmt = (struct clast_block*) stmt;
            modified = look_for_bounds_in_ast(block_stmt->body, param_bounds, margin);
        }
    }
    return modified;
}

static void print_statement_macro(FILE *out, struct bounds *bounds) {
    unsigned i;

    fprintf(out, "#define S1(");
    for (i = 0; i < bounds->names.nb_names; ++i) {
        if(i)
            fprintf(out, ", ");
        fprintf(out, "p%u", i);
    }
    fprintf(out, ") do {");
    fprintf(out, "h = h_good;\\\ngood(");
    for (i = 0; i < bounds->names.nb_names; ++i) {
        if(i != 0)
            fprintf(out, ", ");
        fprintf(out, "p%u", i);
    }
    fprintf(out, ");\\\nh_good = h;\\\n");
    fprintf(out, "h = h_test;\\\ntest(");
    for (i = 0; i < bounds->names.nb_names; ++i) {
        if(i != 0)
            fprintf(out, ", ");
        fprintf(out, "p%u", i);
    }
    fprintf(out, ");\\\nh_test = h;\\\n");
    fprintf(out, "if ( h_good != h_test ) {\\\n");
    fprintf(out,
            "fprintf(stderr, \"\\x1b[1m\\x1b[5m\\x1b[31mTest failed\\x1b[0m\\n\");\\\n");
    fprintf(out,
            "fprintf(stderr, \"\\x1b[1m\\x1b[31mThis happened with the following parameters:\\n\");\\\n");
    fprintf(out, "fprintf(stderr, \"(");
    for (i = 0; i < bounds->names.nb_names; ++i) {
        if(i != 0)
            fprintf(out, ", ");
        fprintf(out, "%%d");
    }
    fprintf(out, ")\\n\"");
    for (i = 0; i < bounds->names.nb_names; ++i) {
        fprintf(out, ", ");
        fprintf(out, "p%u", i);
    }
    fprintf(out, ");\\\n");
    fprintf(out, "exit(2);\\\n} } while(0)\n");
}

static void print_good_test_declaration(FILE *out, struct bounds *bounds) {
    unsigned int i;
    fprintf(out, "void test(");
    for (i = 0; i < bounds->names.nb_names; ++i) {
        if(i)
            fprintf(out, ", ");
        fprintf(out, "int p%u", i);
    }
    fprintf(out, ");\n");
    fprintf(out, "void good(");
    for (i = 0; i < bounds->names.nb_names; ++i) {
        if(i)
            fprintf(out, ", ");
        fprintf(out, "int p%u", i);
    }
    fprintf(out, ");\n\n");
}

static void fprint_cloog_program_parameters_decl(FILE *out, CloogProgram *p) {
    int i;
    for (i = 0; i < p->names->nb_iterators; ++i) {
        fprintf(out, "int %s;\n", p->names->iterators[i]);
    }
}

static const char initial_hash_value[] = "2166136261u";

static const char preamble1[] =
    "#include <stdio.h>\n"
    "#include <stdlib.h>\n"
    "\n"
    "static unsigned h;\n"
    "\n"
    "void hash(int v)\n"
    "{\n"
    "  size_t i;\n"
    "  union u {\n"
    "    int v;\n"
    "    unsigned char c[sizeof(int)];\n"
    "  } u;\n"
    "  u.v = v;\n"
    "  for (i = 0; i < sizeof(int); ++i) {\n"
    "    h *= 16777619;\n"
    "    h ^= u.c[i];\n"
    "  }\n"
    "}\n"
    "\n"
    ;

static const char preamble2[] =
    "int main()\n"
    "{\n"
    " unsigned h_good, h_test;\n";

static void print_macros(FILE *file) {
    fprintf(file, "/* Useful macros. */\n") ;
    fprintf(file,
            "#define floord(n,d) (((n)<0) ? -((-(n)+(d)-1)/(d)) : (n)/(d))\n");
    fprintf(file,
            "#define ceild(n,d) (((n)<0) ? -((-(n))/(d)) : ((n)+(d)-1)/(d))\n");
    fprintf(file, "#define max(x,y)    ((x) > (y) ? (x) : (y))\n");
    fprintf(file, "#define min(x,y)    ((x) < (y) ? (x) : (y))\n\n");
}

static const char postamble[] =
    "fprintf(stderr, \"\\x1b[1m\\x1b[35mWarning : it may be possible that the test "
    "did not compute anything\\n\\x1b[0m\");\nreturn 1;"
    "}\nreturn EXIT_SUCCESS;\n}\n";

#if _POSIX_C_SOURCE >=2 || _XOPEN_SOURCE
static const char help[] =
    "Usage: generate_test_advanced [options] input_file output_file\n\n"
    "Options:\n"
    "\t-u upper_bound : Set upper bound to upper_bound\n"
    "\t-l lower_bound : Set lower bound to lower_bound\n"
    "\t-m margin : Set margin to bound\n"
    "\t-o : input file is in OpenScop format\n"
    "\t-h : Print this help\n";
#else
static const char help[] =
    "Usage: generate_test_advanced input_file output_file\n";
#endif

static inline void print_help(FILE *out) {
    fprintf(out, "\x1b[1m%s\x1b[0m", help);
}

static const char getopt_flags[] = "ohl:u:m:";

int main(int argc, char **argv) {

    CloogState *state;
    CloogStatement *statement;
#ifndef DEBUG
    CloogOptions *options;
#endif
    CloogProgram *program, *p;
    CloogDomain *context, *iterators, *temp;
    cloog_int_t lowerbound, upperbound, margin;
    struct clast_stmt *root;
    const char *lowerbound_val = NULL,
                *upperbound_val = NULL,
                 *margin_val = NULL,
                  *input_name,
                  *output_name;

    int set_openscop_option = 0;
#if _POSIX_C_SOURCE >=2 || _XOPEN_SOURCE
    int opt;
    while ((opt = getopt(argc, argv, getopt_flags)) != -1) {
        switch (opt) {
        case 'h':
            print_help(stdout);
            break;
        case 'l':
            lowerbound_val = optarg;
            break;
        case 'u':
            upperbound_val = optarg;
            break;
        case 'm':
            margin_val = optarg;
            break;
        case 'o':
            set_openscop_option = 1;
            break;
        }
    }
    if ((argc - optind) != 2) {
        fprintf(stderr,
                "\x1b[1m\x1b[31mError: Bad arguments\x1b[0m\n\n");
        print_help(stderr);
        return EXIT_FAILURE;
    }
    input_name = argv[optind];
    output_name = argv[optind + 1];

#else
    if (argc != 3) {
        fprintf(stderr,
                "\x1b[1m\x1b[31mError: Bad arguments\x1b[0m\n\n");
        print_help(stderr);
        return EXIT_FAILURE;
    }
    input_name = argv[argc-2];
    output_name = argv[argc-1];
#endif

    FILE *input_file = fopen(input_name, "r");
    FILE *output_file = fopen(output_name, "w");
    if (!input_file) {
        fprintf(stderr,
                "\x1b[1m\x1b[31mError: Unable to open file %s\x1b[0m\n", input_name);
        print_help(stderr);
        return EXIT_FAILURE;
    }
    if (!output_file) {
        fprintf(stderr,
                "\x1b[1m\x1b[31mError: Unable to open file %s\x1b[0m\n", input_name);
        print_help(stderr);
        return EXIT_FAILURE;
    }

    state = cloog_state_malloc();
    options = cloog_options_malloc(state);
    if (set_openscop_option)
        options->openscop = 1;
    program = cloog_program_read(input_file, options);
    context = cloog_domain_copy(program->context);
    context = cloog_domain_from_context(context);
    program = cloog_program_generate(program, options);
    root = cloog_clast_create(program, options);

    struct clast_root *rootclast = (struct clast_root*) root;
    struct name_list parameters =
    { rootclast->names->nb_parameters, rootclast->names->parameters };

    if (parameters.nb_names >= 5) {
        if (!lowerbound_val)
            lowerbound_val = "0";
        if (!upperbound_val)
            upperbound_val = "5";
        if (!margin_val)
            margin_val = "5";
    } else if (parameters.nb_names >= 3) {
        if (!lowerbound_val)
            lowerbound_val = "-30";
        if (!upperbound_val)
            upperbound_val = "30";
        if (!margin_val)
            margin_val = "10";
    } else {
        if (!lowerbound_val)
            lowerbound_val = "-100";
        if (!upperbound_val)
            upperbound_val = "100";
        if (!margin_val)
            margin_val = "15";
    }

    cloog_int_init(lowerbound);
    cloog_int_init(upperbound);
    cloog_int_init(margin);

    cloog_int_read(lowerbound, lowerbound_val);
    cloog_int_read(upperbound, upperbound_val);
    cloog_int_read(margin, margin_val);

    struct bounds* param_bounds = init_bounds(parameters,
                                  lowerbound, upperbound);

    if (parameters.nb_names != 0) {
        bool modified = true;
        while (modified) {
            modified = look_for_bounds_in_ast(root, param_bounds, margin);
        }

#ifdef DEBUG
        print_bounds(stdout, param_bounds);
#endif

    }

    iterators = cloog_domain_from_bounds(state, param_bounds->lowerbound.value,
                                         param_bounds->upperbound.value);

    iterators = cloog_domain_intersection(temp = iterators, context);

#ifdef DEBUG
    //cloog_domain_print_constraints(stdout, iterators, 0);
#endif

    /* Create a new state and new options for the new program. */
    CloogState* new_state = cloog_state_malloc();
    CloogOptions* new_options = cloog_options_malloc(new_state);

    p = cloog_program_malloc();
    assert(p);
    p->names = cloog_names_malloc();
    assert(p->names);
    p->names->nb_iterators = parameters.nb_names;
    p->names->iterators = cloog_names_generate_items(parameters.nb_names, "p", 0);
    p->language = 'c';
    p->context = cloog_domain_universe(new_state, 0);
    statement = cloog_statement_alloc(new_state, 1);
    p->loop = cloog_loop_malloc(new_state);
    p->loop->domain = iterators;
    p->loop->block = cloog_block_alloc(statement, 0, NULL, parameters.nb_names);
    p->blocklist = cloog_block_list_alloc(p->loop->block);
    p = cloog_program_generate(p, new_options);

    fprintf(output_file, "%s", preamble1);
    print_good_test_declaration(output_file, param_bounds);
    fprintf(output_file, "%s", preamble2);
    print_statement_macro(output_file, param_bounds);
    fprintf(output_file, "h_good = %s;\n", initial_hash_value);
    fprintf(output_file, "h_test = %s;\n", initial_hash_value);
    print_macros(output_file);
    fprint_cloog_program_parameters_decl(output_file, p);
    cloog_program_pprint(output_file, p, new_options);
    fprintf(output_file, "if (h_good == %s) {\n", initial_hash_value);
    fprintf(output_file, "%s", postamble);

    fclose(input_file);
    fclose(output_file);

    cloog_int_clear(margin);
    cloog_int_clear(lowerbound);
    cloog_int_clear(upperbound);

    free_bounds(param_bounds);

    cloog_program_free(p);
    cloog_options_free(new_options) ;
    cloog_state_free(new_state);

    cloog_clast_free(root);
    cloog_domain_free(context);
    cloog_domain_free(temp);
    cloog_program_free(program);
    cloog_options_free(options) ;
    cloog_state_free(state);

    return EXIT_SUCCESS;
}
