
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                 extensions/pluto_unroll.h                       **
 **-----------------------------------------------------------------**
 **                   First version: 26/06/2014                     **
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
* Copyright (C) 2014 Inria                                                  *
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


#ifndef OSL_PLUTO_UNROLL_H
#define OSL_PLUTO_UNROLL_H


#include <stdio.h>
#include <stdbool.h>

#include "../strings.h"
#include "../interface.h"


#if defined(__cplusplus)
extern "C"
{
#endif


#define OSL_URI_PLUTO_UNROLL "pluto_unroll"


/**
 * The osl_pluto_unroll_t structure stores the pluto_unroll
 * that Pluto wants to unroll
 */
struct osl_pluto_unroll {
    char*        iter;              /**< \brief \0 terminated iterator name */
    bool         jam;               /**< \brief true if jam, false otherwise */
    unsigned int factor;            /**< \brief unroll factor */
    struct osl_pluto_unroll * next; /**< \brief next { iter, jam, factor } */
};
typedef struct osl_pluto_unroll   osl_pluto_unroll_t;
typedef struct osl_pluto_unroll * osl_pluto_unroll_p;


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/
void   osl_pluto_unroll_idump(FILE *, osl_pluto_unroll_p, int);
void   osl_pluto_unroll_dump(FILE *, osl_pluto_unroll_p);
char * osl_pluto_unroll_sprint(osl_pluto_unroll_p);


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/
osl_pluto_unroll_p osl_pluto_unroll_sread(char**);


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/
osl_pluto_unroll_p osl_pluto_unroll_malloc(void);
void               osl_pluto_unroll_free(osl_pluto_unroll_p);


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/
void               osl_pluto_unroll_fill(osl_pluto_unroll_p,
        char const * const,
        bool, unsigned int);
osl_pluto_unroll_p osl_pluto_unroll_clone(osl_pluto_unroll_p);
int                osl_pluto_unroll_equal_one(osl_pluto_unroll_p,
        osl_pluto_unroll_p);
int                osl_pluto_unroll_equal(osl_pluto_unroll_p,
        osl_pluto_unroll_p);
osl_strings_p      osl_pluto_unroll_to_strings(osl_pluto_unroll_p);
osl_interface_p    osl_pluto_unroll_interface(void);


#if defined(__cplusplus)
}
#endif


#endif /* define OSL_PLUTO_UNROLL_H */
