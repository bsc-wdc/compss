
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                             int.h                               **
 **-----------------------------------------------------------------**
 **                   First version: 18/07/2011                     **
 **-----------------------------------------------------------------**


*****************************************************************************
* OpenScop: Structures and formats for polyhedral tools to talk together    *
*****************************************************************************
*    ,___,,_,__,,__,,__,,__,,_,__,,_,__,,__,,___,_,__,,_,__,                *
*    /   / /  //  //  //  // /   / /  //  //   / /  // /  /|,_,             *
*   /   / /  //  //  //  // /   / /  //  //   / /  // /  / / /\             *
*  |~~~|~|~~~|~~~|~~~|~~~|~|~~~|~|~~~|~~~|~~~|~|~~~|~|~~~|/_/  \            *
*  | G |C| P | = | L | P |=| = |C| = | = | = |=| = |=| C |\  \ /\           *
*  | R |l| o | = | e | l |=| = |a| = | = | = |=| = |=| L | \# \ /\          *
*  | A |a| l | = | t | u |=| = |n| = | = | = |=| = |=| o | |\# \  \         *
*  | P |n| l | = | s | t |=| = |d| = | = | = | |   |=| o | | \# \  \        *
*  | H | | y |   | e | o | | = |l|   |   | = | |   | | G | |  \  \  \       *
*  | I | |   |   | e |   | |   | |   |   |   | |   | |   | |   \  \  \      *
*  | T | |   |   |   |   | |   | |   |   |   | |   | |   | |    \  \  \     *
*  | E | |   |   |   |   | |   | |   |   |   | |   | |   | |     \  \  \    *
*  | * |*| * | * | * | * |*| * |*| * | * | * |*| * |*| * | /      \* \  \   *
*  | O |p| e | n | S | c |o| p |-| L | i | b |r| a |r| y |/        \  \ /   *
*  '---'-'---'---'---'---'-'---'-'---'---'---'-'---'-'---'          '--'    *
*                                                                           *
* Copyright (C) 2008 University Paris-Sud 11 and INRIA                      *
*                                                                           *
* (3-clause BSD license)                                                    *
* Redistribution and use in source  and binary forms, with or without       *
* modification, are permitted provided that the following conditions        *
* are met:                                                                  *
*                                                                           *
* 1. Redistributions of source code must retain the above copyright notice, *
*    this list of conditions and the following disclaimer.                  *
* 2. Redistributions in binary form must reproduce the above copyright      *
*    notice, this list of conditions and the following disclaimer in the    *
*    documentation and/or other materials provided with the distribution.   *
* 3. The name of the author may not be used to endorse or promote products  *
*    derived from this software without specific prior written permission.  *
*                                                                           *
* THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR      *
* IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES *
* OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.   *
* IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,          *
* INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT  *
* NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, *
* DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY     *
* THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT       *
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF  *
* THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.         *
*                                                                           *
* OpenScop Library, a library to manipulate OpenScop formats and data       *
* structures. Written by:                                                   *
* Cedric Bastoul     <Cedric.Bastoul@u-psud.fr> and                         *
* Louis-Noel Pouchet <Louis-Noel.pouchet@inria.fr>                          *
*                                                                           *
*****************************************************************************/


#ifndef OSL_INT_H
# define OSL_INT_H

# ifdef OSL_GMP_IS_HERE
#  include <gmp.h>
# endif
#include <stdlib.h>
#include <stdio.h>

# if defined(__cplusplus)
extern "C"
{
# endif

/**
 * The osl_int_t union stores an OpenScop integer element.
 */
union osl_int {
    long int  sp; /**< Single precision int */
    long long dp; /**< Double precision int */
#ifdef OSL_GMP_IS_HERE
    mpz_t*    mp; /**< Pointer to a multiple precision int */
#else
    void*     mp; /**< Pointer to a multiple precision int */
#endif
};
typedef union osl_int               osl_int_t;
typedef union osl_int       *       osl_int_p;
typedef union osl_int const         osl_const_int_t;
typedef union osl_int       * const osl_int_const_p;
typedef union osl_int const *       osl_const_int_p;
typedef union osl_int const * const osl_const_int_const_p;


/*+***************************************************************************
 *                                Basic Functions                            *
 *****************************************************************************/


int       osl_int_is_precision_supported(int);
void      osl_int_dump_precision(FILE *, int);
void      osl_int_init(int, osl_int_const_p);
osl_int_p osl_int_malloc(int);
void      osl_int_assign(int, osl_int_const_p, osl_const_int_t);
void      osl_int_set_si(int, osl_int_const_p, int);
int       osl_int_get_si(int, osl_const_int_t);
double    osl_int_get_d(int, osl_const_int_t);
void      osl_int_init_set(int, osl_int_const_p, osl_const_int_t);
void      osl_int_init_set_si(int, osl_int_const_p, int);
void      osl_int_swap(int, osl_int_const_p, osl_int_const_p);
void      osl_int_clear(int, osl_int_const_p);
void      osl_int_free(int, osl_int_const_p);
void      osl_int_print(FILE *, int, osl_const_int_t);
void      osl_int_sprint(char *, int, osl_const_int_t);
void      osl_int_sprint_txt(char *, int, osl_const_int_t);
int       osl_int_sscanf(char*, int, osl_int_const_p);
void      osl_int_sread(char **, int, osl_int_const_p);


/*+***************************************************************************
 *                            Arithmetic Operations                          *
 *****************************************************************************/


void      osl_int_increment(int, osl_int_const_p, osl_const_int_t);
void      osl_int_decrement(int, osl_int_const_p, osl_const_int_t);
void      osl_int_add(int, osl_int_const_p, osl_const_int_t, osl_const_int_t);
void      osl_int_add_si(int, osl_int_const_p, osl_const_int_t, int);
void      osl_int_sub(int, osl_int_const_p, osl_const_int_t, osl_const_int_t);
void      osl_int_mul(int, osl_int_const_p, osl_const_int_t, osl_const_int_t);
void      osl_int_mul_si(int, osl_int_const_p, osl_const_int_t, int);
void      osl_int_div_exact(int const, osl_int_const_p,
                            osl_const_int_t, osl_const_int_t);
void      osl_int_floor_div_q(int const, osl_int_const_p,
                              osl_const_int_t, osl_const_int_t);
void      osl_int_floor_div_r(int const, osl_int_const_p,
                              osl_const_int_t, osl_const_int_t);
void      osl_int_floor_div_q_r(int const, osl_int_const_p, osl_int_const_p,
                                osl_const_int_t, osl_const_int_t);
void      osl_int_mod(int const, osl_int_const_p,
                      osl_const_int_t, osl_const_int_t);
void      osl_int_gcd(int const, osl_int_const_p,
                      osl_const_int_t, osl_const_int_t);
void      osl_int_lcm(int const, osl_int_const_p,
                      osl_const_int_t, osl_const_int_t);
void      osl_int_oppose(int, osl_int_const_p, osl_const_int_t);
void      osl_int_abs(int, osl_int_const_p, osl_const_int_t);
size_t    osl_int_size_in_base_2(int const, osl_const_int_t);
size_t    osl_int_size_in_base_10(int const, osl_const_int_t);


/*+***************************************************************************
 *                            Conditional Operations                         *
 *****************************************************************************/


int       osl_int_eq(int, osl_const_int_t, osl_const_int_t);
int       osl_int_ne(int, osl_const_int_t, osl_const_int_t);
int       osl_int_lt(int, osl_const_int_t, osl_const_int_t);
int       osl_int_le(int, osl_const_int_t, osl_const_int_t);
int       osl_int_gt(int, osl_const_int_t, osl_const_int_t);
int       osl_int_ge(int, osl_const_int_t, osl_const_int_t);
int       osl_int_pos(int, osl_const_int_t);
int       osl_int_neg(int, osl_const_int_t);
int       osl_int_zero(int, osl_const_int_t);
int       osl_int_one(int, osl_const_int_t);
int       osl_int_mone(int, osl_const_int_t);
int       osl_int_divisible(int, osl_const_int_t, osl_const_int_t);


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/


void      osl_int_set_precision(int const, int const, osl_int_p);


# if defined(__cplusplus)
}
# endif

#endif /* define OSL_INT_H */
