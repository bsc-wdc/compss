
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                            body.h                               **
 **-----------------------------------------------------------------**
 **                   First version: 25/06/2011                     **
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


#ifndef OSL_BODY_H
# define OSL_BODY_H

# include <stdio.h>
# include <osl/strings.h>
# include <osl/interface.h>

# if defined(__cplusplus)
extern "C"
{
# endif

# define OSL_URI_BODY "body"

/**
 * The osl_body_t structure stores a statement body in a textual form.
 * The complete original expression (directly copy-pasted from the original
 * code) is in the expression field while the textual forms of the original
 * iterators are in the iterators field. They may be used for substitutions
 * inside the expression.
 */
struct osl_body {
    osl_strings_p iterators;  /**< Original iterators */
    osl_strings_p expression; /**< Original statement expression */
};
typedef struct osl_body   osl_body_t;
typedef struct osl_body * osl_body_p;
typedef struct osl_body const         osl_const_body_t;
typedef struct osl_body       * const osl_body_const_p;
typedef struct osl_body const *       osl_const_body_p;
typedef struct osl_body const * const osl_const_body_const_p;


/*---------------------------------------------------------------------------+
 |                          Structure display function                       |
 +---------------------------------------------------------------------------*/
void            osl_body_idump(FILE *, osl_body_p, int);
void            osl_body_dump(FILE *, osl_body_p);
char *          osl_body_sprint(osl_body_p);
void            osl_body_print(FILE *, osl_body_p);

// SCoPLib Compatibility
void            osl_body_print_scoplib(FILE * file, osl_body_p body);

/*****************************************************************************
 *                              Reading function                             *
 *****************************************************************************/
osl_body_p      osl_body_sread(char **);


/*+***************************************************************************
 *                   Memory allocation/deallocation function                 *
 *****************************************************************************/
osl_body_p      osl_body_malloc(void);
void            osl_body_free(osl_body_p);


/*+***************************************************************************
 *                           Processing functions                            *
 *****************************************************************************/
osl_body_p      osl_body_clone(osl_body_p);
int             osl_body_equal(osl_body_p, osl_body_p);
osl_interface_p osl_body_interface(void);

# if defined(__cplusplus)
}
# endif
#endif /* define OSL_BODY_H */
