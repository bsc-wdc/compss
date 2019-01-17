
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                          interface.h                            **
 **-----------------------------------------------------------------**
 **                   First version: 15/07/2011                     **
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


#ifndef OSL_INTERFACE_H
# define OSL_INTERFACE_H

# include <stdio.h>

# if defined(__cplusplus)
extern "C"
{
# endif


typedef void   (*osl_idump_f) (FILE *, void *, int);
typedef char * (*osl_sprint_f)(void *);
typedef void * (*osl_sread_f) (char **);
typedef void * (*osl_malloc_f)(void);
typedef void   (*osl_free_f)  (void *);
typedef void * (*osl_clone_f) (void *);
typedef int    (*osl_equal_f) (void *, void *);


/**
 * The osl_interface structure stores the URI and base
 * functions pointers an openscop object implementation has to offer. It
 * is a node in a NULL-terminated list of interfaces.
 */
struct osl_interface {
    char * URI;                  /**< Unique identifier string */
    osl_idump_f  idump;          /**< Pointer to idump function */
    osl_sprint_f sprint;         /**< Pointer to sprint function */
    osl_sread_f  sread;          /**< Pointer to sread function */
    osl_malloc_f malloc;         /**< Pointer to malloc function */
    osl_free_f   free;           /**< Pointer to free function */
    osl_clone_f  clone;          /**< Pointer to clone function */
    osl_equal_f  equal;          /**< Pointer to equal function */
    struct osl_interface * next; /**< Next interface in the list */
};
typedef struct osl_interface   osl_interface_t;
typedef struct osl_interface * osl_interface_p;


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/
void            osl_interface_idump(FILE *, osl_interface_p, int);
void            osl_interface_dump(FILE *, osl_interface_p);


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/
void            osl_interface_add(osl_interface_p *, osl_interface_p);
osl_interface_p osl_interface_malloc(void);
void            osl_interface_free(osl_interface_p);


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/
int             osl_interface_number(osl_interface_p);
osl_interface_p osl_interface_nclone(osl_interface_p, int);
osl_interface_p osl_interface_clone(osl_interface_p);
int             osl_interface_equal(osl_interface_p, osl_interface_p);
osl_interface_p osl_interface_lookup(osl_interface_p, char *);
osl_interface_p osl_interface_get_default_registry(void);

# if defined(__cplusplus)
}
# endif

#endif /* define OSL_INTERFACE_H */
