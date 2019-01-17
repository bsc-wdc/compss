
/**------ ( ----------------------------------------------------------**
 **       )\                      CAnDL                               **
 **----- /  ) --------------------------------------------------------**
 **     ( * (                  violation.h                            **
 **----  \#/  --------------------------------------------------------**
 **    .-"#'-.        First version: december 12th 2005               **
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


#ifndef CANDL_VIOLATION_H
# define CANDL_VIOLATION_H

# include <stdio.h>
# include <candl/options.h>

# if defined(__cplusplus)
extern "C"
{
# endif

struct osl_scop;
struct osl_dependence;
struct osl_relation;

/**
 * CandlViolation structure:
 * this structure contains all informations about a data dependence violation.
 *

  Violation domain structure
                                     ________________________________________________________________________________________________
                                   /       source (output) |       target (input)  |                   local dims                     \
                                __ |_______________________|_______________________|___________________________________________________|_____________
                              / eq |output |output |output |output |output |output |ld dom |ld acc |ld scatt |ld dom |ld acc |ld scatt |           |  \
                              | in |domain |access |scatt  |domain |access |scatt  |source |source |source   |target |target |target   |parameters | 1 |
         _____________________|____|_______|_______|_______|_______|_______|_______|_______|_______|_________|_______|_______|_________|___________|___|
             |Domain source   | X  |   X   :       :       |       :       :       |   X   :       :         |       :       :         |     X     | X |
             |________________|____|_______:_______:_______|_______:_______:_______|_______:_______:_________|_______:_______:_________|___________|___|
             |Domain target   | X  |       :       :       |   X   :       :       |       :       :         |   X   :       :         |     X     | X |
             |________________|____|_______:_______:_______|_______:_______:_______|_______:_______:_________|_______:_______:_________|___________|___|
  Dependence |Access source   | X  |   X   :   X   :       |       :       :       |       :   X   :         |       :       :         |     X     | X |
  system     |________________|____|_______:_______:_______|_______:_______:_______|_______:_______:_________|_______:_______:_________|___________|___|
             |Access target   | X  |       :       :       |   X   :   X   :       |       :       :         |       :   X   :         |     X     | X |
             |________________|____|_______:_______:_______|_______:_______:_______|_______:_______:_________|_______:_______:_________|___________|___|
             |Access equality |    |       :  Id   :       |       :  -Id  :       |       :       :         |       :       :         |           |   |
             |________________|____|_______:_______:_______|_______:_______:_______|_______:_______:_________|_______:_______:_________|___________|___|    | 0    : 0..depth-1
             |Precedence      | X  |  Id   :       :       |  -Id  :       :       |       :       :         |       :       :         |           | X | <--| 0|-1 : depth
         ===============================================================================================================================================
             |Scattering      |    |       :       :       |       :       :       |       :       :         |       :       :         |           |   |
             |source          | X  |   X   :       :   X   |       :       :       |       :       :    X    |       :       :         |     X     | X |
             |________________|____|_______:_______:_______|_______:_______:_______|_______:_______:_________|_______:_______:_________|___________|___|
             |Scattering      |    |       :       :       |       :       :       |       :       :         |       :       :         |           |   |
             |target          | X  |       :       :       |   X   :       :   X   |       :       :         |       :       :    X    |     X     | X |
             |________________|____|_______:_______:_______|_______:_______:_______|_______:_______:_________|_______:_______:_________|___________|___|
             |Equality at     |    |       :       :       |       :       :       |       :       :         |       :       :         |           |   |
             |1 ... dim_1     |    |       :       :  Id   |       :       : -Id   |       :       :         |       :       :         |           |   |
             |________________|____|_______:_______:_______|_______:_______:_______|_______:_______:_________|_______:_______:_________|___________|___|
         at  |Scat source >   |    |       :       :       |       :       :       |       :       :         |       :       :         |           |   |
         dim |Scat target     | 1  |       :       :   1   |       :       :  -1   |       :       :         |       :       :         |           |-1 |
             \________________|____|_______:_______:_______|_______:_______:_______|_______:_______:_________|_______:_______:_________|___________|___/

                                                      (1)                     (2)                      (3)                       (4)
*/

struct candl_violation {
    struct osl_dependence* dependence;    /**< Pointer to violated dependence. */
    int dimension;                        /**< Violation dimension. */
    struct osl_relation* domain;          /**< Violation polyhedron. */
    struct candl_violation* next;         /**< Pointer to next violation. */

    int source_nb_output_dims_scattering; // (1)
    int target_nb_output_dims_scattering; // (2)
    int source_nb_local_dims_scattering;  // (3)
    int target_nb_local_dims_scattering;  // (4)
};
typedef struct candl_violation  candl_violation_t;
typedef struct candl_violation* candl_violation_p;

/******************************************************************************
 *                          Structure display function                        *
 ******************************************************************************/
void              candl_violation_idump(FILE*, candl_violation_p, int);
void              candl_violation_dump(FILE*, candl_violation_p);
void              candl_violation_pprint(FILE*, candl_violation_p);
void              candl_violation_view(candl_violation_p);

/******************************************************************************
 *                         Memory deallocation function                       *
 ******************************************************************************/
void              candl_violation_free(candl_violation_p);

/******************************************************************************
 *                             Processing functions                           *
 ******************************************************************************/
candl_violation_p candl_violation_malloc();
void              candl_violation_add(candl_violation_p*, candl_violation_p*,
                                      candl_violation_p);
void              candl_violation_append(candl_violation_p*, candl_violation_p);
candl_violation_p candl_violation_single(struct osl_scop*, struct osl_dependence*,
        struct osl_scop*, candl_options_p);
candl_violation_p candl_violation(struct osl_scop*, struct osl_scop*,
                                  struct osl_dependence**, candl_options_p);

# if defined(__cplusplus)
}
# endif
#endif /* define CANDL_VIOLATION_H */

