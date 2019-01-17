#ifndef CLOOG_CLAST_H
#define CLOOG_CLAST_H
#if defined(__cplusplus)
extern "C" 
  {
#endif 

enum clast_expr_type {
    clast_expr_name,
    clast_expr_term,
    clast_expr_bin,
    clast_expr_red
};
struct clast_expr {
    enum clast_expr_type type;
};

struct clast_name {
    struct clast_expr	expr;
    const char *	name;
};

/* Represents the term
 *	val * var	(if var != NULL)
 * or
 *	val		(if var == NULL)
 */
struct clast_term {
    struct clast_expr	expr;
    cloog_int_t		val;
    struct clast_expr  *var;
};

#define CLAST_PARALLEL_NOT 0
#define CLAST_PARALLEL_OMP 1
#define CLAST_PARALLEL_MPI 2
#define CLAST_PARALLEL_VEC 4

enum clast_red_type { clast_red_sum, clast_red_min, clast_red_max };
struct clast_reduction {
    struct clast_expr	expr;
    enum clast_red_type	type;
    int			n;
    struct clast_expr*	elts[1];
};

enum clast_bin_type { clast_bin_fdiv, clast_bin_cdiv, 
		      clast_bin_div, clast_bin_mod };
struct clast_binary {
    struct clast_expr	expr;
    enum clast_bin_type type;
    struct clast_expr*	LHS;
    cloog_int_t		RHS;
};

struct clast_stmt;
struct clast_stmt_op {
    void (*free)(struct clast_stmt *);
};

#define CLAST_STMT_IS_A(stmt, type) ((stmt)->op == &(type))

extern const struct clast_stmt_op stmt_root;
extern const struct clast_stmt_op stmt_ass;
extern const struct clast_stmt_op stmt_user;
extern const struct clast_stmt_op stmt_block;
extern const struct clast_stmt_op stmt_for;
extern const struct clast_stmt_op stmt_guard;

struct clast_stmt {
    const struct clast_stmt_op    *op;
    struct clast_stmt	*next;
};

struct clast_root {
    struct clast_stmt	stmt;
    CloogNames *	names;       /**< Names of iterators and parameters. */
};

struct clast_assignment {
    struct clast_stmt	stmt;
    const char *	LHS;
    struct clast_expr *	RHS;
};

struct clast_block {
    struct clast_stmt	stmt;
    struct clast_stmt *	body;
};

struct clast_user_stmt {
    struct clast_stmt	stmt;
    CloogDomain *	domain;
    CloogStatement *	statement;
    struct clast_stmt *	substitutions;
};

struct clast_for {
    struct clast_stmt	stmt;
    CloogDomain *	domain;
    const char *	iterator;
    struct clast_expr *	LB;
    struct clast_expr *	UB;
    cloog_int_t		stride;
    struct clast_stmt *	body;
    int parallel;
    /* Comma separated list of loop private variables for OpenMP parallelization */
    char *private_vars;
    /* Comma separated list of reduction variable/operators for OpenMP parallelization */
    char *reduction_vars;
    /* Enable execution time reporting for this loop */
    /* Variable name to accumulate execution time this loop */
    char *time_var_name;
};

struct clast_equation {
    struct clast_expr *	LHS;
    struct clast_expr *	RHS;
    int			sign;
};

struct clast_guard {
    struct clast_stmt	stmt;
    struct clast_stmt *	then;
    int			n;
    struct clast_equation	eq[1];
};


struct clast_stmt *cloog_clast_create_from_input(CloogInput *input,
						 CloogOptions *options);
struct clast_stmt *cloog_clast_create(CloogProgram *program,
				      CloogOptions *options);
void cloog_clast_free(struct clast_stmt *s);

struct clast_name *new_clast_name(const char *name);
struct clast_term *new_clast_term(cloog_int_t c, struct clast_expr *v);
struct clast_binary *new_clast_binary(enum clast_bin_type t, 
				      struct clast_expr *lhs, cloog_int_t rhs);
struct clast_reduction *new_clast_reduction(enum clast_red_type t, int n);
struct clast_root *new_clast_root(CloogNames *names);
struct clast_assignment *new_clast_assignment(const char *lhs,
					      struct clast_expr *rhs);
struct clast_user_stmt *new_clast_user_stmt(CloogDomain *domain,
    CloogStatement *stmt, struct clast_stmt *subs);
struct clast_block *new_clast_block(void);
struct clast_for *new_clast_for(CloogDomain *domain, const char *it,
                                struct clast_expr *LB, struct clast_expr *UB,
                                CloogStride *stride);
struct clast_guard *new_clast_guard(int n);

void free_clast_name(struct clast_name *t);
void free_clast_term(struct clast_term *t);
void free_clast_binary(struct clast_binary *b);
void free_clast_reduction(struct clast_reduction *r);
void free_clast_expr(struct clast_expr *e);
void free_clast_stmt(struct clast_stmt *s);

int clast_expr_equal(struct clast_expr *e1, struct clast_expr *e2);

struct clast_expr *clast_bound_from_constraint(CloogConstraint *constraint,
					       int level, CloogNames *names);

typedef enum filterType {exact, subset} ClastFilterType;

typedef struct clastFilter{
    const char *iter;
    const int *stmts_filter;
    int nstmts_filter;
    ClastFilterType filter_type;
} ClastFilter;

void clast_filter(struct clast_stmt *node, ClastFilter filter,
        struct clast_for ***loops, int *nloops, int **stmts, int *nstmts);

#if defined(__cplusplus)
  }
#endif 
#endif /* define _H */
