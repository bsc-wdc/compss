
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                     extensions/irregular.h                        **
 **-----------------------------------------------------------------**
 **                   First version: 07/12/2010                     **
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


#ifndef OSL_IRREGULAR_H
# define OSL_IRREGULAR_H

# include <stdio.h>
# include <osl/macros.h>
# include <osl/strings.h>
# include <osl/interface.h>

# if defined(__cplusplus)
extern "C"
{
# endif


# define OSL_URI_IRREGULAR        "irregular"
# define OSL_TAG_IRREGULAR_START  "<"  OSL_URI_IRREGULAR ">"
# define OSL_TAG_IRREGULAR_STOP   "</" OSL_URI_IRREGULAR ">"


/**
 * The osl_irregular_t structure stores an irregular extension to the core
 * OpenScop representation. It contains a list of predicates (in their textual
 * representation), and for each statement, its list of associated predicates.
 * The list of predicates contains both control and exit predicates (see
 * Benabderrhamane et al.'s paper at CC'2010), control predicates are listed
 * first, then come exit predicates.
 */
struct osl_irregular {
    // List of predicates (textual representation).
    int nb_control;      /**< Number of control predicates in the SCoP. */
    int nb_exit;         /**< Number of exit predicates in the SCoP. */
    int * nb_iterators;  /**< nb_iterators[i]: #iterators for ith predicate. */
    char *** iterators;  /**< iterators[i]: array of (nb_control + nb_exit)
                            arrays of nb_iterators[i] strings. Each element
                            corresponds to the list of original iterators
                            for the ith predicate. */
    char ** body;        /**< body[i]: original source code of ith predicate. */

    // List of associated predicates for each statement.
    int nb_statements;   /**< Number of statements in the SCoP. */
    int * nb_predicates; /**< nb_predicates[i]: #predicates for ith statement. */
    int ** predicates;   /**< predicates[i]: array of nb_predicates[i] predicates
                            corresponding to the list of predicates associated
                            to the ith statement. */
};
typedef struct osl_irregular   osl_irregular_t;
typedef struct osl_irregular * osl_irregular_p;


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/
void            osl_irregular_idump(FILE *, osl_irregular_p, int);
void            osl_irregular_dump(FILE *, osl_irregular_p);
char *          osl_irregular_sprint(osl_irregular_p);


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/
osl_irregular_p osl_irregular_sread(char **);


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/
osl_irregular_p osl_irregular_malloc(void);
void            osl_irregular_free(osl_irregular_p);


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/
osl_irregular_p osl_irregular_clone(osl_irregular_p);
int             osl_irregular_equal(osl_irregular_p, osl_irregular_p);
osl_irregular_p osl_irregular_add_control(osl_irregular_p, char**, int, char*);
osl_irregular_p osl_irregular_add_exit(osl_irregular_p, char**, int, char*);
osl_irregular_p osl_irregular_add_predicates(osl_irregular_p, int*, int);
osl_interface_p osl_irregular_interface(void);


# if defined(__cplusplus)
}
# endif

#endif /* define OSL_IRREGULAR_H */
