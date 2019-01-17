
/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (                    matrix.h                             **
 **----  \#/  --------------------------------------------------------**
 **    .-"#'-.        First version: december 9th 2005                **
 **--- |"-.-"| -------------------------------------------------------**
       |     |
       |     |
******** |     | *************************************************************
* CAnDL  '-._,-' the Chunky Analyzer for Dependences in Loops (experimental) *
******************************************************************************
*                                                                            *
* Copyright (C) 2005-2008 Cedric Bastoul                                     *
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

#ifndef CANDL_MATRIX_H
# define CANDL_MATRIX_H

# include <candl/violation.h>

# if defined(__cplusplus)
extern "C"
{
# endif

struct osl_relation;
struct osl_dependence;

candl_violation_p candl_matrix_violation(struct osl_dependence*,
        struct osl_relation*, struct osl_relation*,
        int, int);
int               candl_matrix_check_point(struct osl_relation*,
        struct osl_relation*);

# if defined(__cplusplus)
}
# endif
#endif
