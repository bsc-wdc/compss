#ifndef CLOOG_ISL_CONSTRAINTSET_H
#define CLOOG_ISL_CONSTRAINTSET_H

#include <cloog/isl/backend.h>

#if defined(__cplusplus)
extern "C" 
  {
#endif 

struct cloogconstraintset {
	int dummy;	/* Solaris cc doesn't like zero-sized structs */
};

struct cloogequalities {
	int			  n;
	unsigned		  total_dim;
	isl_constraint		**constraints;
	int			 *types;
};

struct cloogconstraint {
	int dummy;	/* Solaris cc doesn't like zero-sized structs */
};

CloogConstraintSet *cloog_constraint_set_from_isl_basic_set(struct isl_basic_set *bset);
CloogConstraint *cloog_constraint_from_isl_constraint(struct isl_constraint *constraint);
isl_constraint *cloog_constraint_to_isl(CloogConstraint *constraint);

__isl_give isl_val *cloog_int_to_isl_val(isl_ctx* ctx, cloog_int_t c);
void isl_val_to_cloog_int(__isl_keep isl_val *val, cloog_int_t *cint);

__isl_give isl_val *cloog_constraint_coefficient_get_val(CloogConstraint *constraint,
			int var);

#if defined(__cplusplus)
  }
#endif 
#endif /* define _H */
