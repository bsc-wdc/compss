
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                      extensions/dependence.h                    **
 **-----------------------------------------------------------------**
 **                   First version: 02/07/2012                     **
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

#ifndef OSL_DEPENDENCE_H
# define OSL_DEPENDENCE_H

# include <stdio.h>
# include <osl/interface.h>
# include <osl/statement.h>
# include <osl/relation.h>

# if defined(__cplusplus)
extern "C"
{
# endif


# define OSL_URI_DEPENDENCE "dependence"

# define OSL_DEPENDENCE_RAW          1
# define OSL_DEPENDENCE_WAR          2
# define OSL_DEPENDENCE_WAW          3
# define OSL_DEPENDENCE_RAR          4
# define OSL_DEPENDENCE_RAW_SCALPRIV 5


# define OSL_DEPENDENCE_EQUAL 1
# define OSL_DEPENDENCE_POSIT 2
# define OSL_DEPENDENCE_LATER 3
# define OSL_DEPENDENCE_NEVER 4

# define OSL_DEPENDENCE_ASSIGNMENT  1
# define OSL_DEPENDENCE_P_REDUCTION 2
# define OSL_DEPENDENCE_M_REDUCTION 3
# define OSL_DEPENDENCE_T_REDUCTION 4

/**
 * osl_dependence structure:
 * this structure contains all the informations about a data dependence, it is
 * also a node of the linked list of all dependences of the dependence graph.


  Dependence domain structure
                          ________________________________________________________________
                        / source (output) | target (input)  |          local dims          \
                     __ |_________________|_________________|_______________________________|_____________
                   / eq | output | output | output | output |ld dom |ld acc |ld dom |ld acc |           |  \
                   | in | domain | access | domain | access |source |source |target |target |parameters | 1 |
   ________________|____|________|________|________|________|_______|_______|_______|_______|___________|___|
  |Domain source   | X  |   X    :        |        :        |   X   :       |       :       |     X     | X |
  |________________|____|________:________|________:________|_______:_______|_______:_______|___________|___|
  |Domain target   | X  |        :        |    X   :        |       :       |   X   :       |     X     | X |
  |________________|____|________:________|________:________|_______:_______|_______:_______|___________|___|
  |Access source   | X  |   X    :   X    |        :        |       :   X   |       :       |     X     | X |
  |________________|____|________:________|________:________|_______:_______|_______:_______|___________|___|
  |Access target   | X  |        :        |    X   :   X    |       :       |       :   X   |     X     | X |
  |________________|____|________:________|________:________|_______:_______|_______:_______|___________|___|
  |Access equality |    |        :  Id    |        :  -Id   |       :       |       :       |           |   |
  |________________|____|________:________|________:________|_______:_______|_______:_______|___________|___|    | 0    : 0..depth-1
  |Precedence      | X  |  Id    :        |   -Id  :        |       :       |       :       |           | X | <--| 0|-1 : depth
  \________________|____|________:________|________:________|_______:_______|_______:_______|___________|___/

                           (1)      (2)      (3)      (4)      (5)     (6)     (7)     (8)
 */
struct osl_dependence {
    int label_source;
    int label_target;
    int ref_source;              /**< Position of source reference in the array access list. */
    int ref_target;              /**< Position of target reference in the array access list. */
    int depth;                   /**< Dependence level. */
    int type;                    /**< Dependence type: a dependence from source
                                 *   to target can be:
				                         *   - OSL_DEPENDENCE_UNSET if the dependence type
                                 *     is still not set,
				                         *   - OSL_DEPENDENCE_RAW if source writes M and
				                         *     target read M (flow-dependence),
				                         *   - OSL_DEPENDENCE_WAR if source reads M and
				                         *     target writes M (anti-dependence),
				                         *   - OSL_DEPENDENCE_WAW if source writes M and
				                         *     target writes M too (output-dependence)
				                         *   - OSL_DEPENDENCE_RAR if source reads M and
				                         *     target reads M too (input-dependence).
				                         */
    osl_relation_p domain;         /**< Dependence polyhedron. */

    /* Other useful information */

    int source_nb_output_dims_domain; // (1)
    int source_nb_output_dims_access; // (2)

    int target_nb_output_dims_domain; // (3)
    int target_nb_output_dims_access; // (4)

    int source_nb_local_dims_domain; // (5)
    int source_nb_local_dims_access; // (6)
    int target_nb_local_dims_domain; // (7)
    int target_nb_local_dims_access; // (8)

    void* usr;			 /**< User field, for library users
				    convenience. */
    struct osl_dependence * next; /**< Pointer to next dependence */

    /* These attributes are not filled by osl
     * You can use the function candl_dependence_init_fields of CandL
     */

    osl_relation_p ref_source_access_ptr;     /**< Pointer to the source access. */
    osl_relation_p ref_target_access_ptr;     /**< Pointer to the target access. */

    osl_statement_p stmt_source_ptr;      /**< Pointer to source statement. */
    osl_statement_p stmt_target_ptr;      /**< Pointer to target statement. */
};

typedef struct osl_dependence   osl_dependence_t;
typedef struct osl_dependence * osl_dependence_p;


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/
void            osl_dependence_idump(FILE *, osl_dependence_p, int);
void            osl_dependence_dump(FILE *, osl_dependence_p);
char *          osl_dependence_sprint(osl_dependence_p);
void            osl_dependence_print(FILE *, osl_dependence_p);


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/
osl_dependence_p     osl_dependence_sread(char **);
osl_dependence_p     osl_dependence_psread(char **, int);

/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/
osl_dependence_p     osl_dependence_malloc(void);
void                 osl_dependence_free(osl_dependence_p);

/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/
osl_dependence_p      osl_dependence_clone(osl_dependence_p);
int                   osl_dependence_equal(osl_dependence_p, osl_dependence_p);
void                  osl_dependence_add(osl_dependence_p*, osl_dependence_p*,
        osl_dependence_p);
int                   osl_nb_dependences(osl_dependence_p);
osl_interface_p       osl_dependence_interface(void);

# if defined(__cplusplus)
}
# endif

#endif /* define OSL_DEPENDENCE_H */
