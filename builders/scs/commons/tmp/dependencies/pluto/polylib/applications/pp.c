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

#include <polylib/polylib.h>

#define WS 0

int main() {
	
  Matrix *a, *b;
  Polyhedron *A, *B;
  Param_Polyhedron *PA;
  Param_Domain *P;
  Param_Vertices *V;
  int nbPV, i, j;
  const char **param_name;
  
  a = Matrix_Read();
  if(!a || a->NbColumns == 0) {
    fprintf(stderr,"Input error: empty matrix\n");
    exit(0);
  }
  A = Constraints2Polyhedron(a, WS);
  Matrix_Free(a);
  b = Matrix_Read();
  
  if(!b || b->NbColumns == 0) {
    fprintf(stderr, "Input error: empty matrix\n");
    exit(0);
  }
  B = Constraints2Polyhedron(b, WS);
  Matrix_Free(b);
  
  /* Read the name of the parameters */
  param_name = Read_ParamNames(stdin, B->Dimension);
  PA = Polyhedron2Param_Domain(A,B,WS);
  if(!PA || PA->D==NULL) {
    printf("---------------------------------------\n");
    printf("Empty polyhedron\n");
    return 0;
  }
  nbPV = PA->nbV;
  Domain_Free(A);
  Domain_Free(B);

  if (PA->Rays->NbRows > 0) {
    printf( "---------------------------------------\n" );
    printf( "Overall rays :\n");
    for (i = 0; i < PA->Rays->NbRows; i++) {
      if (value_zero_p(PA->Rays->p[i][0]))
        printf("Line:   [");
      else
        printf("Ray:    [");
      for (j = 1; j < PA->Rays->NbColumns-1; j++) {
        value_print(stdout,P_VALUE_FMT,PA->Rays->p[i][j]);
      }
      printf(" ]\n");
    }
  }

  /*****************************/
  /* Scan the validity domains */
  for(P=PA->D;P;P=P->next) {
    
    /* prints current val. dom. */
    printf( "---------------------------------------\n" );
    printf( "Domain :\n");
    Print_Domain( stdout, P->Domain, param_name );
    
    /* scan the vertices */
    printf( "Vertices :\n");
    FORALL_PVertex_in_ParamPolyhedron(V,P,PA) {
	
      /* prints each vertex */
      Print_Vertex( stdout, V->Vertex, param_name );
      printf( "\n" );
    }
    END_FORALL_PVertex_in_ParamPolyhedron;
  }
  /*****************************/
  
  Param_Polyhedron_Free( PA );
  free(param_name);
  
  return 0;
} /* main */ 

