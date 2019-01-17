/******************************************************************************
 *                     PIP : Parametric Integer Programming                   *
 ******************************************************************************
 *                                 piplib.h                                   *
 ******************************************************************************
 *                                                                            *
 * Copyright Paul Feautrier, 1988-2005                                        *
 *                                                                            *
 * This library is free software; you can redistribute it and/or modify it    *
 * under the terms of the GNU Lesser General Public License as published by   *
 * the Free Software Foundation; either version 2.1 of the License, or (at    *
 * your option) any later version.                                            *
 *                                                                            *
 * This software is distributed in the hope that it will be useful, but       *
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY *
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License   *
 * for more details.							      *
 *                                                                            *
 * You should have received a copy of the GNU Lesser General Public License   *
 * along with this library; if not, write to the Free Software Foundation,    *
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA         *
 *                                                                            *
 * Written by Cedric Bastoul                                                  *
 *                                                                            *
 ******************************************************************************/

/* Premiere version du 18 septembre 2002. */


#if \
  !defined(PIPLIB_INT_SP) && \
  !defined(PIPLIB_INT_DP) && \
  !defined(PIPLIB_INT_GMP)
#error "Define of PIPLIB_INT_SP or PIPLIB_INT_DP or PIPLIB_INT_GMP not found"
#endif


#include <stdio.h>
#include <math.h>


#ifdef PIPLIB_INT_GMP

#include <gmp.h>

#define PIPLIB_NAME(name) name##_gmp

#define PIPLIB_ONE_DETERMINANT

#define piplib_int_t_xx PIPLIB_NAME(piplib_int_t)
typedef mpz_t piplib_int_t_xx;
#define piplib_int_format "%d"

#define piplib_int_init(i) (mpz_init(i))
#define piplib_int_init_set(i, v) (mpz_init_set(i, v))
#define piplib_int_init_set_si(i, v) (mpz_init_set_si(i, v))
#define piplib_int_assign(r, i) (mpz_set(r, i))
#define piplib_int_set_si(r, i) (mpz_set_si(r, i))
#define piplib_int_clear(i) (mpz_clear(i))
#define piplib_int_print(file, i) (mpz_out_str(file, 10, i))
#define piplib_int_sscanf(string, i) (gmp_sscanf(string, "%lZd", i))

#define piplib_int_get_si(i) ((int)mpz_get_si(i))
#define piplib_int_get_d(i) (mpz_get_d(i))

#define piplib_int_add(r, a, b) (mpz_add(r, a, b))
#define piplib_int_sub(r, a, b) (mpz_sub(r, a, b))
#define piplib_int_increment(r, i) (mpz_add_ui(r, i, 1))
#define piplib_int_decrement(r, i) (mpz_sub_ui(r, i, 1))
#define piplib_int_mul(r, a, b) (mpz_mul(r, a, b))
#define piplib_int_div_exact(q, a, b) (mpz_divexact(q, a, b))
#define piplib_int_floor_div_q(q, a, b) (mpz_fdiv_q(q, a, b))
#define piplib_int_floor_div_r(r, a, b) (mpz_fdiv_r(r, a, b))
#define piplib_int_floor_div_q_r(q, r, a, b) (mpz_fdiv_qr(q, r, a, b))
#define piplib_int_mod(mod, a, b) (mpz_mod(mod, a, b))
#define piplib_int_gcd(gcd, a, b) (mpz_gcd(gcd, a, b))
#define piplib_int_oppose(r, i) (mpz_neg(r, i))
#define piplib_int_size_in_base_2(i) (mpz_sizeinbase(i, 2))
#define piplib_int_size_in_base_10(i) (mpz_sizeinbase(i, 10))

#define piplib_int_eq(a, b) (mpz_cmp(a, b) == 0)
#define piplib_int_ne(a, b) (mpz_cmp(a, b) != 0)
#define piplib_int_zero(i) (mpz_sgn(i) == 0)
#define piplib_int_one(i) (mpz_cmp_si(i, 1) == 0)
#define piplib_int_pos(i) (mpz_sgn(i) > 0)
#define piplib_int_neg(i) (mpz_sgn(i) < 0)

#endif

#ifdef PIPLIB_INT_SP

#define PIPLIB_NAME(name) name##_sp

#define piplib_int_t_xx PIPLIB_NAME(piplib_int_t)
typedef long int piplib_int_t_xx;
#define piplib_int_format "%ld"

#endif

#ifdef PIPLIB_INT_DP

#define PIPLIB_NAME(name) name##_dp

#define piplib_int_t_xx PIPLIB_NAME(piplib_int_t)
typedef long long int piplib_int_t_xx;
#define piplib_int_format "%lld"

#endif


#if defined(PIPLIB_INT_SP) || defined(PIPLIB_INT_DP)

// Copy from osl_int
#define piplib_llgcdxx PIPLIB_NAME(piplib_llgcd)
#define piplib_llgcd_llabs_xx PIPLIB_NAME(piplib_llgcd_llabs)
#define piplib_lllog2_xx PIPLIB_NAME(piplib_lllog2)
#define piplib_lllog10_xx PIPLIB_NAME(piplib_lllog10)
#define piplib_llmod_xx PIPLIB_NAME(piplib_llmod)
long long int piplib_llgcd_xx(long long int const,
                              long long int const);
long long int piplib_llgcd_llabs_xx(long long int const,
                                    long long int const);
size_t piplib_lllog2_xx(long long int);
size_t piplib_lllog10_xx(long long int);
long long int piplib_llmod_xx(long long int const,
                              long long int const);

#define piplib_int_init(i) (i = 0)
#define piplib_int_init_set(i, v) (i = v)
#define piplib_int_init_set_si(i, v) (i = v)
#define piplib_int_assign(r, i) (r = i)
#define piplib_int_set_si(r, i) (r = (piplib_int_t_xx)i)
#define piplib_int_clear(i) do { } while (0)
#define piplib_int_print(file, i) (fprintf(file, piplib_int_format, i))
#define piplib_int_sscanf(string, i) (sscanf(string, piplib_int_format, &i))

#define piplib_int_get_si(i) ((int)(i))
#define piplib_int_get_d(i) ((double)(i))

#define piplib_int_add(r, a, b) (r = a + b)
#define piplib_int_sub(r, a, b) (r = a - b)
#define piplib_int_increment(r, i) (r = i + 1)
#define piplib_int_decrement(r, i) (r = i - 1)
#define piplib_int_mul(r, a, b) (r = a * b)
#define piplib_int_div_exact(q, a, b) (q = (a) / (b))
//#define piplib_int_floor_div_q(q, a, b) (q = (piplib_int_t_xx)(piplib_ll_floor_div_q(a, b)))
#define piplib_int_floor_div_q(q, a, b) \
    (q = (piplib_int_t_xx)((a - piplib_llmod_xx(a, b)) \
    / (b)))
#define piplib_int_floor_div_r(r, a, b) \
    (r = (piplib_int_t_xx)piplib_llmod_xx(a, b))
//#define piplib_int_floor_div_r(r, a, b) (r = (piplib_int_t_xx)(piplib_ll_floor_div_r(a, b)))
#define piplib_int_floor_div_q_r(q, r, a, b) \
    do { piplib_int_floor_div_q(q, a, b); piplib_int_floor_div_r(r, a, b); } \
    while (0)
#define piplib_int_mod(mod, a, b) \
    (mod = (piplib_int_t_xx)(piplib_llmod_xx(a, b)))
#define piplib_int_gcd(gcd, a, b) \
    (gcd = (piplib_int_t_xx)(piplib_llgcd_llabs_xx(a, b)))
#define piplib_int_oppose(r, i) (r = - (i))
#define piplib_int_size_in_base_2(i) (piplib_lllog2_xx(i))
#define piplib_int_size_in_base_10(i) (piplib_lllog10_xx(i))

#define piplib_int_eq(a, b) (a == b)
#define piplib_int_ne(a, b) (a != b)
#define piplib_int_zero(i) (i == 0)
#define piplib_int_one(i) (i == 1)
#define piplib_int_pos(i) (i > 0)
#define piplib_int_neg(i) (i < 0)

#endif

// Some global variables
#define verbose_xx PIPLIB_NAME(verbose)
#define deepest_cut_xx PIPLIB_NAME(deepest_cut)
#define dump_xx PIPLIB_NAME(dump)


#if defined(__cplusplus)
extern "C"
{
#endif


/**
 * @brief Structure PipMatrix
 *
 * Structure de matrice au format PolyLib. Le premier element d'une ligne
 * indique quand il vaut 1 que la ligne decrit une inequation de la forme
 * p(x)>=0 et quand il vaut 0, que la ligne decrit une egalite de la forme
 * p(x)=0. Le dernier element de chaque ligne correspond au coefficient
 * constant.
 */
struct PIPLIB_NAME(pipmatrix) {
    unsigned int NbRows;    /**< Number of rows */
    unsigned int NbColumns; /**< Number of columns */
    piplib_int_t_xx** p;       /**< Data */
    piplib_int_t_xx* p_Init;   /**< Init */
    int p_Init_size;        /**< Only for PolyLib compatibility under MP version:
                               PolyLib makes sometimes overestimates on the size
                               of the matrices, in order to go faster.
                               Thus NbRows*NbColumns is not the number of
                               allocated elements. With MP version, we have to
                               think to mpz_clear() all the initialized elements
                               before freing, then we need to know the number of
                               allocated elements: p_Init_size. */
};
#define PipMatrix_xx PIPLIB_NAME(PipMatrix)
typedef struct PIPLIB_NAME(pipmatrix) PipMatrix_xx;


/**
 * @brief Structure PipVector
 *
 * Cette structure contient un Vector de 'nb_elements' la ieme composante de
 * ce vecteur vaut the_vector[i]/the_deno[i].
 */
struct PIPLIB_NAME(pipvector) {
    int nb_elements;          /**< Nombre d'elements du vecteur. */
    piplib_int_t_xx* the_vector; /**< Numerateurs du vecteur. */
    piplib_int_t_xx* the_deno;   /**< Denominateurs du vecteur. */
};
#define PipVector_xx PIPLIB_NAME(PipVector)
typedef struct PIPLIB_NAME(pipvector) PipVector_xx;


/**
 * @brief Structure PipNewparm
 *
 * Liste chainee de Newparm, les informations d'un newparm etant son rang, un
 * vecteur de coefficients et un denominateur. Le newparm est egal a la division
 * du vecteur par le denominateur.
 */
struct PIPLIB_NAME(pipnewparm) {
    int rank;                /**< Rang du 'newparm'. */
    PipVector_xx* vector;       /**< Le vector decrivant le newparm. */
    piplib_int_t_xx deno;       /**< Denominateur du 'newparm'. */
    struct PIPLIB_NAME(pipnewparm)* next; /**< Pointeur vers le newparm suivant. */
};
#define PipNewparm_xx PIPLIB_NAME(PipNewparm)
typedef struct PIPLIB_NAME(pipnewparm) PipNewparm_xx;


/**
 * @brief Structure PipList
 *
 * Liste chainee de Vector.
 */
struct PIPLIB_NAME(piplist) {
    PipVector_xx* vector;    /**< Le vector contenant la partie de solution. */
    struct PIPLIB_NAME(piplist)* next; /**< Pointeur vers l'element suivant. */
};
#define PipList_xx PIPLIB_NAME(PipList)
typedef struct PIPLIB_NAME(piplist) PipList_xx;


/**
 * @brief Structure pipquast
 *
 * Arbre binaire. Conformement a la grammaire de sortie (voir mode d'emploi), un
 * noeud de l'arbre des solutions debute par une liste de 'newparm'. Il continue
 * ensuite soit par une 'list' (alors condition vaut null), soit par un 'if'
 * (alors le champ condition contient la condition).
 */
struct PIPLIB_NAME(pipquast) {
    PipNewparm_xx* newparm;        /**< Les 'newparm'. */
    PipList_xx* list;              /**< La 'list' si pas de 'if'. */
    PipVector_xx* condition;       /**< La condition si 'if'. */
    struct PIPLIB_NAME(pipquast)* next_then; /**< Noeud si condition et si verifiee. */
    struct PIPLIB_NAME(pipquast)* next_else; /**< Noeud si condition et si non verifiee. */
    struct PIPLIB_NAME(pipquast)* father;    /**< Pointeur vers le quast pere. */
};
#define PipQuast_xx PIPLIB_NAME(PipQuast)
typedef struct PIPLIB_NAME(pipquast) PipQuast_xx;


/**
 * @brief Structure pipoptions
 *
 * This structure contains each option that can be set to change the PIP
 * behaviour.
 */
struct PIPLIB_NAME(pipoptions) {
    /** 1 if an integer solution is needed, 0 otherwise. */
    int Nq;

    /**
     * @brief Verbose
     *
     * -1 -> absolute silence @n
     *  0 -> relative silence @n
     *  1 -> information on cuts when an integer solution is needed @n
     *  2 -> information sur les pivots et les déterminants @n
     *  3 -> information on arrays
     *
     * Each option include the preceding
     */
    int Verbose;

    /** Set to 1 to eliminate some trivial solutions, 0 otherwise */
    int Simplify;

    /** Set to 1 to include deepest cut algorithm */
    int Deepest_cut;

    /** Set to 1 if maximum is needed */
    int Maximize;

    /**
     * @brief Signs of parameters
     *
     * -1 -> all parameters may be negative @n
     *  0 -> all parameters are non-negative
     */
    int Urs_parms;

    /**
     * @brief Signs of unknowns
     *
     * -1 -> all unknowns may be negative @n
     *  0 -> all unknowns are non-negative
     */
    int Urs_unknowns;

    /** To compute the dual */
    int Compute_dual;
};
#define PipOptions_xx PIPLIB_NAME(PipOptions)
typedef struct PIPLIB_NAME(pipoptions) PipOptions_xx;

#define pip_options_print_xx PIPLIB_NAME(pip_options_print)
void pip_options_print_xx(FILE*, PipOptions_xx*);


/* Fonctions d'affichages des structures de la PipLib. */
#define pip_matrix_print_xx PIPLIB_NAME(pip_matrix_print)
#define pip_vector_print_xx PIPLIB_NAME(pip_vector_print)
#define pip_newparm_print_xx PIPLIB_NAME(pip_newparm_print)
#define pip_list_print_xx PIPLIB_NAME(pip_list_print)
#define pip_quast_print_xx PIPLIB_NAME(pip_quast_print)
void pip_matrix_print_xx(FILE*, PipMatrix_xx*);
void pip_vector_print_xx(FILE*, PipVector_xx*);
void pip_newparm_print_xx(FILE*, PipNewparm_xx*, int);
void pip_list_print_xx(FILE*, PipList_xx*, int);
void pip_quast_print_xx(FILE*, PipQuast_xx*, int);


/* Fonctions de liberation memoire des structures de la PipLib.*/
#define pip_matrix_free_xx PIPLIB_NAME(pip_matrix_free)
#define pip_vector_free_xx PIPLIB_NAME(pip_vector_free)
#define pip_newparm_free_xx PIPLIB_NAME(pip_newparm_free)
#define pip_list_free_xx PIPLIB_NAME(pip_list_free)
#define pip_quast_free_xx PIPLIB_NAME(pip_quast_free)
#define pip_options_free_xx PIPLIB_NAME(pip_options_free)
void pip_matrix_free_xx(PipMatrix_xx*);
void pip_vector_free_xx(PipVector_xx*);
void pip_newparm_free_xx(PipNewparm_xx*);
void pip_list_free_xx(PipList_xx*);
void pip_quast_free_xx(PipQuast_xx*);
void pip_options_free_xx(PipOptions_xx*);


/* Fonctions d'acquisition de matrices de contraintes et options. */
#define pip_matrix_alloc_xx PIPLIB_NAME(pip_matrix_alloc)
#define pip_matrix_read_xx PIPLIB_NAME(pip_matrix_read)
#define pip_options_init_xx PIPLIB_NAME(pip_options_init)
PipMatrix_xx* pip_matrix_alloc_xx(unsigned int, unsigned int);
PipMatrix_xx* pip_matrix_read_xx(FILE*);
PipOptions_xx* pip_options_init_xx(void);


/* initialization of pip library */
#define pip_init_xx PIPLIB_NAME(pip_init)
#define pip_close_xx PIPLIB_NAME(pip_close)
void pip_init_xx();
void pip_close_xx();


#define pip_solve_xx PIPLIB_NAME(pip_solve)
/**
 * @brief Fonction de resolution
 *
 * pip_solve_xx resoud le probleme qu'on lui passe en parametre,
 * suivant les options elles aussi en parametre. Elle renvoie la solution sous
 * forme d'un arbre de PipQuast.
 *
 * @param[in] domain     Inequations definissant le domaine des inconnues
 * @param[in] parameters Inequations satisfaites par les parametres
 * @param[in] bignum     Column rank of the bignum, or negative value if there is no big parameter
 * @param[in] options    PipLib options
 */
PipQuast_xx* pip_solve_xx(
    PipMatrix_xx* domain,
    PipMatrix_xx* parameters,
    int bignum,
    PipOptions_xx* options);


#define sol_quast_edit_xx PIPLIB_NAME(sol_quast_edit)
/** sol_quast_edit */
PipQuast_xx* sol_quast_edit_xx(
    int* i, PipQuast_xx* father, int Bg, int Urs_p, int flags);


#if defined(__cplusplus)
}
#endif



/* Old names (undef) */

#undef piplib_int_t
#undef PipMatrix
#undef PipVector
#undef PipNewparm
#undef PipList
#undef PipQuast
#undef PipOptions
#undef PipMatrix

#undef pip_options_print

#undef pip_matrix_print
#undef pip_vector_print
#undef pip_newparm_print
#undef pip_list_print
#undef pip_quast_print

#undef pip_matrix_free
#undef pip_vector_free
#undef pip_newparm_free
#undef pip_list_free
#undef pip_quast_free
#undef pip_options_free

#undef pip_matrix_alloc
#undef pip_matrix_read
#undef pip_options_init

#undef pip_init
#undef pip_close

#undef pip_solve

#undef sol_quast_edit

#undef Entier

/* Old names (define) */

#ifdef PIPLIB_INT_GMP

#define piplib_int_t piplib_int_t_gmp
#define PipMatrix PipMatrix_gmp
#define PipVector PipVector_gmp
#define PipNewparm PipNewparm_gmp
#define PipList PipList_gmp
#define PipQuast PipQuast_gmp
#define PipOptions PipOptions_gmp
#define PipMatrix PipMatrix_gmp

#define pip_options_print pip_options_print_gmp

#define pip_matrix_print pip_matrix_print_gmp
#define pip_vector_print pip_vector_print_gmp
#define pip_newparm_print pip_newparm_print_gmp
#define pip_list_print pip_list_print_gmp
#define pip_quast_print pip_quast_print_gmp

#define pip_matrix_free pip_matrix_free_gmp
#define pip_vector_free pip_vector_free_gmp
#define pip_newparm_free pip_newparm_free_gmp
#define pip_list_free pip_list_free_gmp
#define pip_quast_free pip_quast_free_gmp
#define pip_options_free pip_options_free_gmp

#define pip_matrix_alloc pip_matrix_alloc_gmp
#define pip_matrix_read pip_matrix_read_gmp
#define pip_options_init pip_options_init_gmp

#define pip_init pip_init_gmp
#define pip_close pip_close_gmp

#define pip_solve pip_solve_gmp

#define sol_quast_edit sol_quast_edit_gmp

#define Entier piplib_int_t_gmp

#endif

#ifdef PIPLIB_INT_SP

#define piplib_int_t piplib_int_t_sp
#define PipMatrix PipMatrix_sp
#define PipVector PipVector_sp
#define PipNewparm PipNewparm_sp
#define PipList PipList_sp
#define PipQuast PipQuast_sp
#define PipOptions PipOptions_sp
#define PipMatrix PipMatrix_sp

#define pip_options_print pip_options_print_sp

#define pip_matrix_print pip_matrix_print_sp
#define pip_vector_print pip_vector_print_sp
#define pip_newparm_print pip_newparm_print_sp
#define pip_list_print pip_list_print_sp
#define pip_quast_print pip_quast_print_sp

#define pip_matrix_free pip_matrix_free_sp
#define pip_vector_free pip_vector_free_sp
#define pip_newparm_free pip_newparm_free_sp
#define pip_list_free pip_list_free_sp
#define pip_quast_free pip_quast_free_sp
#define pip_options_free pip_options_free_sp

#define pip_matrix_alloc pip_matrix_alloc_sp
#define pip_matrix_read pip_matrix_read_sp
#define pip_options_init pip_options_init_sp

#define pip_init pip_init_sp
#define pip_close pip_close_sp

#define pip_solve pip_solve_sp

#define sol_quast_edit sol_quast_edit_sp

#define Entier piplib_int_t_sp

#endif

#ifdef PIPLIB_INT_DP

#define piplib_int_t piplib_int_t_dp
#define PipMatrix PipMatrix_dp
#define PipVector PipVector_dp
#define PipNewparm PipNewparm_dp
#define PipList PipList_dp
#define PipQuast PipQuast_dp
#define PipOptions PipOptions_dp
#define PipMatrix PipMatrix_dp

#define pip_options_print pip_options_print_dp

#define pip_matrix_print pip_matrix_print_dp
#define pip_vector_print pip_vector_print_dp
#define pip_newparm_print pip_newparm_print_dp
#define pip_list_print pip_list_print_dp
#define pip_quast_print pip_quast_print_dp

#define pip_matrix_free pip_matrix_free_dp
#define pip_vector_free pip_vector_free_dp
#define pip_newparm_free pip_newparm_free_dp
#define pip_list_free pip_list_free_dp
#define pip_quast_free pip_quast_free_dp
#define pip_options_free pip_options_free_dp

#define pip_matrix_alloc pip_matrix_alloc_dp
#define pip_matrix_read pip_matrix_read_dp
#define pip_options_init pip_options_init_dp

#define pip_init pip_init_dp
#define pip_close pip_close_dp

#define pip_solve pip_solve_dp

#define sol_quast_edit sol_quast_edit_dp

#define Entier piplib_int_t_dp

#endif
