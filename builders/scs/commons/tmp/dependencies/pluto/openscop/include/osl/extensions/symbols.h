
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                     extensions/symbols.h                        **
 **-----------------------------------------------------------------**
 **                   First version: 07/03/2012                     **
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
* Prasanth Chatharasi <prasanth@iith.ac.in>			             *
*                                                                           *
*****************************************************************************/


#ifndef OSL_SYMBOLS_H
# define OSL_SYMBOLS_H

# include <stdio.h>
# include <osl/interface.h>
# include <osl/relation.h>
# include <osl/generic.h>

# if defined(__cplusplus)
extern "C"
{
# endif

# define OSL_URI_SYMBOLS        "symbols"

/**
 * The osl_symbols_t structure stores information regarding the symbols.
 */
struct osl_symbols {
    int           type;        /**< Symbol type (variable, iterator...) */
    int           generated;   /**< Flag to determine its origin */
    int           nb_dims;     /**< Number of array dimensions */
    osl_generic_p identifier;  /**< Symbol identifier */
    osl_generic_p datatype;    /**< Symbol Datatype (int, float...) */
    osl_generic_p scope;       /**< Scope of symbol */
    osl_generic_p extent;      /**< Limits of dimensions in Symbol */

    void*         usr;         /**< A user defined field */
    struct osl_symbols* next;
};
typedef struct osl_symbols  osl_symbols_t;
typedef struct osl_symbols* osl_symbols_p;


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/
void            osl_symbols_idump(FILE *, osl_symbols_p, int);
void            osl_symbols_dump(FILE *, osl_symbols_p);
char *          osl_symbols_sprint(osl_symbols_p);


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/
osl_symbols_p   osl_symbols_sread(char **);


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/
osl_symbols_p   osl_symbols_malloc(void);
void            osl_symbols_free(osl_symbols_p);


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/
void            osl_symbols_add(osl_symbols_p*, osl_symbols_p);
osl_symbols_p   osl_symbols_nclone(osl_symbols_p, int);
osl_symbols_p   osl_symbols_clone(osl_symbols_p);
int             osl_symbols_equal(osl_symbols_p, osl_symbols_p);
osl_symbols_p   osl_symbols_lookup(osl_symbols_p, osl_generic_p);
osl_symbols_p   osl_symbols_remove(osl_symbols_p*, osl_symbols_p);
int             osl_symbols_get_nb_symbols(osl_symbols_p);
osl_interface_p osl_symbols_interface(void);


# if defined(__cplusplus)
}
# endif

#endif /* define OSL_SYMBOLS_H */
