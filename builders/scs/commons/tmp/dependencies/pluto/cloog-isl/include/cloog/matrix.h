#ifndef CLOOG_MATRIX_H
#define CLOOG_MATRIX_H
#if defined(__cplusplus)
extern "C"
  {
#endif

/* The CloogMatrix structure is equivalent to the PolyLib Matrix data structure
 * (see Wil93). This structure is devoted to represent a set of constraints.
 *
 * The whole matrix is stored in memory row after row at the p_Init address. p
 * is an array of pointers where p[i] points to the first element of the i^{th
 * row. NbRows and NbColumns are respectively the number of rows and columns of
 * the matrix. Each row corresponds to a constraint. The first element of each
 * row is an equality/inequality tag. The constraint is an equality p(x) = 0 if
 * the first element is 0, but it is an inequality p(x) \geq 0 if the first
 * element is 1. The next elements are the unknown coefficients, followed by
 * the parameter coefficients, then the constant term. For instance, the
 * following three constraints:
 *
 *     -i + m  = 0
 *     -j + n >= 0
 *  i + j - k >= 0
 *
 * would be represented by the following rows:
 *
 * # eq/in  i   j   k   m   n   cst
 *     0    0  -1   0   1   0    0
 *     1   -1   0   0   0   1    0
 *     1    1   1  -1   0   0    0
 *
 * To be able to provide different precision version (CLooG supports 32 bits,
 * 64 bits and arbitrary precision through the GMP library), the cloog_int_t
 * type depends on the configuration options (it may be long int for 32 bits
 * version, long long int for 64 bits version, and mpz_t for multiple precision
 * version). */

struct cloogmatrix
{ unsigned NbRows;    /* Number of rows. */
  unsigned NbColumns; /* Number of columns. */
  cloog_int_t ** p;         /* Array of pointers to the matrix rows. */
  cloog_int_t * p_Init;     /* Matrix rows contiguously in memory. */
};

typedef struct cloogmatrix CloogMatrix;

CloogMatrix *cloog_matrix_alloc (unsigned, unsigned);
void cloog_matrix_free (CloogMatrix *);
void cloog_matrix_print_structure(FILE *file, CloogMatrix *M,
		const char *prefix, const char *suffix);
CloogMatrix *cloog_matrix_read(FILE *input);
CloogMatrix *cloog_matrix_read_of_size(FILE *input,
		unsigned n_row, unsigned n_col);
void cloog_matrix_print(FILE*, CloogMatrix*);

#if defined(__cplusplus)
  }
#endif
#endif /* define _H */
