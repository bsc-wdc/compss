
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                           strings.h                             **
 **-----------------------------------------------------------------**
 **                   First version: 13/07/2011                     **
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


#ifndef OSL_STRINGS_H
# define OSL_STRINGS_H

# include <stdio.h>
# include <osl/interface.h>

# if defined(__cplusplus)
extern "C"
{
# endif

# define OSL_URI_STRINGS "strings"

/* The "strings" type is simply a NULL-terminated array of C character
 * strings, i.e. a char **. It is encapsulated into a structure to allow
 * its manipulation through a generic type.
 */
struct osl_strings {
    char ** string; /**< NULL-terminated array of character strings */
};
typedef struct osl_strings               osl_strings_t;
typedef struct osl_strings       *       osl_strings_p;
typedef struct osl_strings const         osl_const_strings_t;
typedef struct osl_strings       * const osl_strings_const_p;
typedef struct osl_strings const *       osl_const_strings_p;
typedef struct osl_strings const * const osl_const_strings_const_p;


/*+***************************************************************************
 *                          Structure display function                       *
 *****************************************************************************/
void            osl_strings_idump(FILE *, osl_strings_p, int);
void            osl_strings_dump(FILE *, osl_strings_p);
char *          osl_strings_sprint(osl_strings_p);
void            osl_strings_print(FILE *, osl_strings_p);


/*****************************************************************************
 *                               Reading function                            *
 *****************************************************************************/
osl_strings_p   osl_strings_sread(char **);
osl_strings_p   osl_strings_read(FILE *);

/*+***************************************************************************
 *                    Memory allocation/deallocation function                *
 *****************************************************************************/
osl_strings_p   osl_strings_malloc(void);
void            osl_strings_free(osl_strings_p);

/*+***************************************************************************
 *                            Processing functions                           *
 *****************************************************************************/
osl_strings_p   osl_strings_clone(osl_strings_p);
void            osl_strings_add(osl_strings_p, char const * const);
size_t          osl_strings_find(osl_strings_p, char const * const);
int             osl_strings_equal(osl_strings_p, osl_strings_p);
size_t          osl_strings_size(osl_const_strings_const_p);
osl_strings_p   osl_strings_encapsulate(char *);
osl_interface_p osl_strings_interface(void);
osl_strings_p   osl_strings_generate(char *, int);
void osl_strings_add_strings(osl_strings_p *, osl_strings_p, osl_strings_p);


# if defined(__cplusplus)
}
# endif

# if defined(__cplusplus)
#include <vector>
#include <string>

namespace osl {
/**
 * @brief Convert osl_strings to std::vector<std::string>
 * @param[in] s An osl_strings
 * @return the std::vector<std::string>
 */
inline
std::vector<std::string> osl_strings_to_cpp(osl_const_strings_const_p s) {
    if (s != nullptr && s->string != nullptr) {
        return std::vector<std::string>(s->string,
                                        s->string + osl_strings_size(s));
    } else {
        return std::vector<std::string>();
    }
}
}
# endif

#endif /* define OSL_STRINGS_H */
