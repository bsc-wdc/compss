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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <polylib/polylib.h>

/****************************************************/
/* Read_ParamNames() :                              */
/* Reads FILE *in for the parameter names           */
/* if in==NULL or not enough parameters on input,   */
/*  use default names                               */
/* returns an n-array of strings                    */
/****************************************************/
const char **Read_ParamNames(FILE *in,int m)
{
  char **param_name;
  int c, i, j, f;
  char s[1024],param[32];
  
  if(!in)
    f = 0;
  else
    do
      f = (fgets(s, 1024, in)!=NULL);
    while (f && (*s=='#' || *s=='\n'));
  
  param_name = (char **)malloc(m*sizeof(char *));
  i = 0;
  if(f) {
    c = 0;
    for(;i<m;++i) {
      j=0;
      for(;;++c) {
	if(s[c]==' ') {
	  if(j==0)
	    continue;
	  else
	    break;
	}
	if(s[c]=='\n' || s[c]==0)
	  break;
	param[j++] = s[c];
      }

      /* Not enough parameters on input, end */
      if(j==0)
	break;
      param[j] = 0;
      param_name[i] = (char *)malloc( (j+1)*sizeof(char) );
      strcpy(param_name[i],param);
    }
  }
  
  /* Not enough parameters on input : use default names */
  if(!f || i!=m) {
    for(;i<m;++i) {
      param_name[i] = (char *) malloc(2*sizeof(char));
      sprintf(param_name[i], "%c", PCHAR+i+1);
    }
  }
  return (const char**)param_name;
} /* Read_ParamNames */

void Free_ParamNames(const char **params, int m)
{
    while (--m >= 0)
	free((char *)params[m]);
    free(params);
}
