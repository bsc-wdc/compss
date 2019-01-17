
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                      extensions/arrays.h                        **
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


#ifndef OSL_ARRAYS_H
# define OSL_ARRAYS_H

# include <stdio.h>
# include <osl/strings.h>
# include <osl/interface.h>

# if defined(__cplusplus)
extern "C"
{
# endif


# define OSL_URI_ARRAYS        "arrays"


/**
 * The osl_arrays_t structure stores a set of array textual names in
 * the extension part of the OpenScop representation. Each name has a
 * name string and an identifier: the ith name as name string names[i]
 * and identifier id[i].
 */
struct osl_arrays {
    int nb_names;      /**< Number of names. */
    int  *  id;        /**< Array of nb_names identifiers. */
    char ** names;     /**< Array of nb_names names. */
};
typedef struct osl_arrays   osl_arrays_t;
typedef struct osl_arrays * osl_arrays_p;


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/
void            osl_arrays_idump(FILE *, osl_arrays_p, int);
void            osl_arrays_dump(FILE *, osl_arrays_p);
char *          osl_arrays_sprint(osl_arrays_p);


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/
osl_arrays_p    osl_arrays_sread(char **);


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/
osl_arrays_p    osl_arrays_malloc(void);
void            osl_arrays_free(osl_arrays_p);


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/
osl_arrays_p    osl_arrays_clone(osl_arrays_p);
int             osl_arrays_equal(osl_arrays_p, osl_arrays_p);
osl_strings_p   osl_arrays_to_strings(osl_arrays_p);
int             osl_arrays_add(osl_arrays_p, int, char*);
size_t          osl_arrays_get_index_from_id(osl_arrays_p, int);
size_t          osl_arrays_get_index_from_name(osl_arrays_p, char*);
osl_interface_p osl_arrays_interface(void);

# if defined(__cplusplus)
}
# endif

#endif /* define OSL_ARRAYS_H */
