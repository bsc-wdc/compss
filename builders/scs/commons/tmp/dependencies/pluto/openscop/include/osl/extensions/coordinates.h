
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                     extensions/coordinates.h                        **
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


#ifndef OSL_COORDINATES_H
# define OSL_COORDINATES_H

# include <stdio.h>
# include <osl/interface.h>

# if defined(__cplusplus)
extern "C"
{
# endif


# define OSL_URI_COORDINATES "coordinates"


/**
 * The osl_coordinates_t structure stores a coordinates extension to the core
 * OpenScop representation. It provides information about the SCoP location in
 * the original source file (file name, starting and ending lines/columns,
 * indentation level).
 */
struct osl_coordinates {
    char* name;         /**< File name (may include the file path as well). */
    int   line_start;   /**< Starting line of the SCoP. */
    int   line_end;     /**< Ending line of the SCoP. */
    int   column_start; /**< Starting column of the SCoP in the starting line. */
    int   column_end;   /**< Ending column of the SCoP in the ending line. */
    int   indent;       /**< Indentation (number of spaces starting each line).*/
};
typedef struct osl_coordinates  osl_coordinates_t;
typedef struct osl_coordinates* osl_coordinates_p;


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/
void              osl_coordinates_idump(FILE*, osl_coordinates_p, int);
void              osl_coordinates_dump(FILE*, osl_coordinates_p);
char*             osl_coordinates_sprint(osl_coordinates_p);


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/
osl_coordinates_p osl_coordinates_sread(char**);


/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/
osl_coordinates_p osl_coordinates_malloc(void);
void              osl_coordinates_free(osl_coordinates_p);


/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/
osl_coordinates_p osl_coordinates_clone(osl_coordinates_p);
int               osl_coordinates_equal(osl_coordinates_p, osl_coordinates_p);
osl_interface_p   osl_coordinates_interface(void);


# if defined(__cplusplus)
}
# endif

#endif /* define OSL_COORDINATES_H */
