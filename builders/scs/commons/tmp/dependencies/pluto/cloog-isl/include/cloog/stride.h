#ifndef CLOOG_STRIDE_H
#define CLOOG_STRIDE_H

#if defined(__cplusplus)
extern "C" {
#endif 

/**
 * Information about strides.
 */
struct cloogstride {
  int references;
  cloog_int_t stride;         /**< The actual stride. */
  cloog_int_t offset;         /**< Offset of strided loop. */
  cloog_int_t factor;
  CloogConstraint *constraint;
};
typedef struct cloogstride CloogStride;

CloogStride *cloog_stride_alloc(cloog_int_t stride, cloog_int_t offset);
CloogStride *cloog_stride_alloc_from_constraint(cloog_int_t stride,
	CloogConstraint *constraint, cloog_int_t factor);
CloogStride *cloog_stride_copy(CloogStride *stride);
void cloog_stride_free(CloogStride *stride);

CloogConstraint *cloog_constraint_stride_lower_bound(CloogConstraint *c,
	int level, CloogStride *stride);

#if defined(__cplusplus)
}
#endif 

#endif
