
/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (               piplib-wrapper.c                          **
 **----  \#/  --------------------------------------------------------**
 **    .-"#'-.        First version: January 31st 2012                **
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
* terms of the GNU Lesser General Public License as published by the Free    *
* Software Foundation; either version 3 of the License, or (at your option)  *
* any later version.                                                         *
*                                                                            *
* This software is distributed in the hope that it will be useful, but       *
* WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY *
* or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License   *
* for more details.                                                          *
*                                                                            *
* You should have received a copy of the GNU Lesser General Public License   *
* along with software; if not, write to the Free Software Foundation, Inc.,  *
* 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA                     *
*                                                                            *
* CAnDL, the Chunky Dependence Analyzer                                      *
* Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                         *
*                                                                            *
******************************************************************************/
/**
 * \file piplib-wrapper.h
 * \author Louis-Noel Pouchet
 */


#ifndef CANDL_PIPLIB_WRAPPER_H
# define CANDL_PIPLIB_WRAPPER_H

#include <osl/osl.h>
#include <candl/piplib.h>

# if defined(__cplusplus)
extern "C"
{
# endif

PipMatrix*      pip_relation2matrix(osl_relation_t*);
osl_relation_t* pip_matrix2relation(PipMatrix*);
int             pip_has_rational_point(osl_relation_t*, osl_relation_t*, int);
PipQuast*       pip_solve_osl(osl_relation_t*, osl_relation_t*,
                              int, PipOptions*);
int quast_are_equal (PipQuast*, PipQuast*, int);
int             piplist_are_equal(PipList*, PipList*, int);
osl_relation_t* pip_quast_to_polyhedra(PipQuast*, int, int);
osl_relation_t* pip_quast_no_solution_to_polyhedra(PipQuast*, int, int);

# if defined(__cplusplus)
}
# endif
#endif /* define CANDL_PIPLIB_WRAPPER_H */

