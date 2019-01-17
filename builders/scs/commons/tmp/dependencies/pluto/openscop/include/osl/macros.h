
/*+-----------------------------------------------------------------**
 **                       OpenScop Library                          **
 **-----------------------------------------------------------------**
 **                           macros.h                              **
 **-----------------------------------------------------------------**
 **                   First version: 30/04/2008                     **
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


#ifndef OSL_MACROS_H
# define OSL_MACROS_H

# include "util.h"


# define OSL_DEBUG                 0       // 1 for debug mode, 0 otherwise.

# define OSL_URI_SCOP              "OpenScop"

# define OSL_PRECISION_ENV         "OSL_PRECISION"
# define OSL_PRECISION_ENV_SP      "32"
# define OSL_PRECISION_ENV_DP      "64"
# define OSL_PRECISION_ENV_MP      "0"
# define OSL_PRECISION_SP          32
# define OSL_PRECISION_DP          64
# define OSL_PRECISION_MP          0

# define OSL_FMT_SP                "%4ld"
# define OSL_FMT_DP                "%4lld"
# define OSL_FMT_MP                "%4s"
# define OSL_FMT_LENGTH            4       // Should be the same as FMT_*P.
# define OSL_FMT_TXT_SP            "%ld"
# define OSL_FMT_TXT_DP            "%lld"
# define OSL_FMT_TXT_MP            "%s"


# define OSL_BACKEND_C             0
# define OSL_BACKEND_FORTRAN       1
# define OSL_UNDEFINED             -1
# define OSL_MAX_STRING            2048
# define OSL_MIN_STRING		   100
# define OSL_MAX_ARRAYS            128

# define OSL_TYPE_GENERIC          0
# define OSL_TYPE_STRING           1
# define OSL_TYPE_CONTEXT          2
# define OSL_TYPE_DOMAIN           3
# define OSL_TYPE_SCATTERING       4
# define OSL_TYPE_ACCESS           5
# define OSL_TYPE_READ             6
# define OSL_TYPE_WRITE            7
# define OSL_TYPE_MAY_WRITE        8

# define OSL_FAKE_ARRAY            "fakearray"

# define OSL_SYMBOL_TYPE_ITERATOR	1
# define OSL_SYMBOL_TYPE_PARAMETER	2
# define OSL_SYMBOL_TYPE_ARRAY		3
# define OSL_SYMBOL_TYPE_FUNCTION	4

# define OSL_STRING_UNDEFINED      "UNDEFINED"
# define OSL_STRING_CONTEXT        "CONTEXT"
# define OSL_STRING_DOMAIN         "DOMAIN"
# define OSL_STRING_SCATTERING     "SCATTERING"
# define OSL_STRING_READ           "READ"
# define OSL_STRING_WRITE          "WRITE"
# define OSL_STRING_MAY_WRITE      "MAY_WRITE"

/*+***************************************************************************
 *                               UTILITY MACROS                              *
 *****************************************************************************/

# define OSL_coucou(n)                                                     \
         do {                                                              \
           int i = n +0;                                                   \
           fprintf(stderr,"[osl] Coucou %d (%s).\n", i, __func__);         \
         } while (0)

# define OSL_debug(msg)                                                    \
         do {                                                              \
           if (OSL_DEBUG)                                                  \
             fprintf(stderr,"[osl] Debug: " msg " (%s).\n", __func__);     \
         } while (0)

# define OSL_info(msg)                                                     \
         do {                                                              \
           fprintf(stderr,"[osl] Info: " msg " (%s).\n", __func__);        \
         } while (0)

# define OSL_warning(msg)                                                  \
         do {                                                              \
           fprintf(stderr,"[osl] Warning: " msg " (%s).\n", __func__);     \
         } while (0)

# define OSL_error(msg)                                                    \
         do {                                                              \
           fprintf(stderr,"[osl] Error: " msg " (%s).\n", __func__);       \
           exit(1);                                                        \
         } while (0)

# define OSL_overflow(msg) OSL_error(msg)

# define OSL_malloc(ptr, type, size)                                       \
         do {                                                              \
           if (((ptr) = (type)malloc(size)) == NULL)                       \
             OSL_error("memory overflow");                                 \
         } while (0)

# define OSL_realloc(ptr, type, size)                                      \
         do {                                                              \
           if (((ptr) = (type)realloc(ptr, size)) == NULL)                 \
             OSL_error("memory overflow");                                 \
         } while (0)

# define OSL_strdup(destination, source)                                   \
         do {                                                              \
           if (source != NULL) {                                           \
             if (((destination) = osl_util_strdup(source)) == NULL)                 \
               OSL_error("memory overflow");                               \
           }                                                               \
           else {                                                          \
             destination = NULL;                                           \
             OSL_warning("strdup of a NULL string");                       \
           }                                                               \
         } while (0)

# define OSL_max(x,y) ((x) > (y)? (x) : (y))

# define OSL_min(x,y) ((x) < (y)? (x) : (y))


#endif /* define OSL_MACROS_H */
