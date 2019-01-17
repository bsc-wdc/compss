
/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (                     ddv.h                               **
 **----  \#/  --------------------------------------------------------**
 **    .-"#'-.         First version: February 4th 2010               **
 **--- |"-.-"| -------------------------------------------------------**
       |     |
       |     |
******** |     | *************************************************************
* CAnDL  '-._,-' the Chunky Analyzer for Dependences in Loops (experimental) *
******************************************************************************
*                                                                            *
* Copyright (C) 2003-2008 Cedric Bastoul                                     *
*                                                                            *
* This is free software; you can redistribute it and/or modify it under the  *
* terms of the GNU General Public License as published by the Free Software  *
* Foundation; either version 2 of the License, or (at your option) any later *
* version.                                                                   *
*                                                                            *
* This software is distributed in the hope that it will be useful, but       *
* WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY *
* or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License   *
* for more details.                                                          *
*                                                                            *
* You should have received a copy of the GNU General Public License along    *
* with software; if not, write to the Free Software Foundation, Inc.,        *
* 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA                     *
*                                                                            *
* CAnDL, the Chunky Dependence Analyzer                                      *
* Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                         *
*                                                                            *
******************************************************************************/

/**
 * \file ddv.h
 * \author Louis-Noel Pouchet
 */

#ifndef CANDL_DDV_H
# define CANDL_DDV_H

# include <stdio.h>

# if defined(__cplusplus)
extern "C"
{
# endif

struct osl_scop;
struct osl_dependence;

/******************************************************************************
 *                        Dependence Distance structures                      *
 ******************************************************************************/

/**
 * A DDV is a vector of elements, each of which has a type and
 * optionally a scalar value.
 *
 * Ex: DDV1 is ('=', '=', '<')
 * Ex: DDV2 is ('1', '1', '*')
 *
 */


/**
 * Types of elements are defined with the e_dv_type enum.
 * '=' -> candl_dv_eq
 * '>' -> candl_dv_plus
 * '<' -> candl_dv_minus
 * '*' -> candl_dv_star
 * 'x' -> candl_dv_scalar (x is some integer)
 *
 */
enum dv_type {
    candl_dv_scalar, candl_dv_plus, candl_dv_minus, candl_dv_star, candl_dv_eq
};
typedef enum dv_type e_dv_type;

/**
 * Elements are tuples, <type,value> where 'value' is defined iff
 * 'type' is candl_dv_scalar.
 *
 */
struct dv_descriptor {
    e_dv_type type;
    int value;
};
typedef struct dv_descriptor s_dv_descriptor;

/**
 * DDV are a chained list of vectors of s_dv_descriptor.
 *
 */
struct candl_ddv {
    int loop_id;
    int length;
    int deptype;
    s_dv_descriptor* data;
    struct candl_ddv* next;
};

typedef struct candl_ddv CandlDDV;

/******************************************************************************
 *                         Memory deallocation function                       *
 ******************************************************************************/

/**
 * candl_ddv_malloc: Allocate an empty ddv.
 *
 *
 */
CandlDDV*
candl_ddv_malloc();

/**
 * candl_ddv_alloc: Allocate a ddv for a loop of depth 'size'.
 *
 *
 */
CandlDDV*
candl_ddv_alloc(int);

/**
 * candl_ddv_free: Free a ddv.
 *
 *
 */
void
candl_ddv_free(CandlDDV*);

/**
 * candl_ddv_set_type_at: Set the type of a ddv component. Type is one of
 * '=', '>', '<', '*' or 'constant' as defined by the enum e_dv_type.
 *
 *
 */
void
candl_ddv_set_type_at(CandlDDV*, e_dv_type, int);

/**
 * candl_ddv_set_value_at: Set the scalar value of a ddv
 * component. This is meaningful only if the type of this component is
 * 'constant'.
 *
 *
 */
void
candl_ddv_set_value_at(CandlDDV*, int, int);

/**
 * candl_ddv_set_type: Set the type of the dependence in
 * CANDL_UNSET, CANDL_RAW, CANDL_WAR, CANDL_WAW, CANDL_RAR.
 *
 */
void
candl_ddv_set_deptype(CandlDDV*, int);

/******************************************************************************
 *                          Structure display function                        *
 ******************************************************************************/

/**
 * candl_ddv_print: print a ddv.
 *
 */
void
candl_ddv_print(FILE*, CandlDDV*);

/******************************************************************************
 *                            Processing functions                            *
 ******************************************************************************/

/**
 * candl_ddv_extract_in_loop: Create a chained list of dependence distance
 * vectors, from the list of polyhedral dependences. Only dependences
 * involving statements surrounded by loop 'loop_id' are considered. One
 * ddv is generated per polyhedral dependence.
 *
 */
CandlDDV*
candl_ddv_extract_in_loop(struct osl_scop*, struct osl_dependence*, int);

/**
 * candl_loops_are_permutable: output 1 if the 2 loops are permutable.
 *
 *
 */
int
candl_loops_are_permutable(struct osl_scop*, struct osl_dependence*, int, int);

# if defined(__cplusplus)
}
# endif
#endif /* define CANDL_DDV_H */

