/*
    This file is part of PolyLib.

    PolyLib is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    PolyLib is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with PolyLib.  If not, see <http://www.gnu.org/licenses/>.
*/

/* errormsg.c
     COPYRIGHT
          Both this software and its documentation are

              Copyright 1993 by IRISA /Universite de Rennes I - France,
              Copyright 1995,1996 by BYU, Provo, Utah
                         all rights reserved.

*/

#include <stdio.h>
#include <polylib/polylib.h>

extern int Pol_status;

/*  This function allows either error messages to be sent to Mathematica,
    or messages to be printed to stderr.

    If MATHLINK is defined, then a command Message[f::msgname] is sent to
    Mathematica. If not, one prints on stderr. The difference with errormsg
    is that the control should be returned immediately to Mathematica after
    the call to errormsg1. Therefore, no Compound statement is sent to
    Mathematica.
*/
void errormsg1(const char *f, const char *msgname, const char *msg)
{
  Pol_status = 1;

#ifdef MATHLINK
  MLPutFunction(stdlink,"Message",1);
  MLPutFunction(stdlink,"MessageName",2);
  MLPutSymbol(stdlink,f);
  MLPutString(stdlink,msgname);
#else
#ifndef NO_MESSAGES
    fprintf(stderr, "?%s: %s\n", f, msg);
#endif
#endif

}
