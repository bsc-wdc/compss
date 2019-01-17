
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                           vector.h                              **
 **-----------------------------------------------------------------**
 **                   First version: 01/05/2008                     **
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


#ifndef OSL_VECTOR_H
# define OSL_VECTOR_H

# include <stdio.h>
# include <osl/int.h>


# if defined(__cplusplus)
extern "C"
{
# endif


/**
 * The osl_vector_t structure stores a vector information in the PolyLib
 * format (the first entry has a specific meaning). When a vector
 * describes a linear constraint, a 0 means it is an equality == 0, a 1 means
 * an inequality >= 0. When the vector describes an array access, a number
 * different than 0 is the array identifier.
 */
struct osl_vector {
    int precision; /**< Precision of the integer elements. */
    int size;      /**< Number of vector entries */
    osl_int_t* v;  /**< An array of integer values */
};
typedef struct osl_vector   osl_vector_t;
typedef struct osl_vector * osl_vector_p;


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/
void         osl_vector_idump(FILE *, osl_vector_p, int);
void         osl_vector_dump(FILE *, osl_vector_p);


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/
osl_vector_p osl_vector_pmalloc(int, int);
osl_vector_p osl_vector_malloc(int);
void         osl_vector_free(osl_vector_p);


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/
osl_vector_p osl_vector_add_scalar(osl_vector_p, int);
osl_vector_p osl_vector_mul_scalar(osl_vector_p, int);
osl_vector_p osl_vector_add(osl_vector_p, osl_vector_p);
osl_vector_p osl_vector_sub(osl_vector_p, osl_vector_p);
void         osl_vector_tag_inequality(osl_vector_p);
void         osl_vector_tag_equality(osl_vector_p);
int          osl_vector_equal(osl_vector_p, osl_vector_p);
int          osl_vector_is_scalar(osl_vector_p);

# if defined(__cplusplus)
}
# endif
#endif /* define OSL_VECTOR_H */
