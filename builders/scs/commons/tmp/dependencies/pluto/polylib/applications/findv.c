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

int main() {

  Matrix *a, *b;
  Polyhedron *A, *B;
  Param_Polyhedron *PA;
  const char **param_name;
  
  a = Matrix_Read();
  A = Constraints2Polyhedron(a,200);
  Matrix_Free(a);
  
  b = Matrix_Read();
  B = Constraints2Polyhedron(b,200);
  Matrix_Free(b);
  
  /* Read the name of the parameters */
  param_name = Read_ParamNames(stdin,B->Dimension);  
  PA = Polyhedron2Param_Vertices(A,B,500);  
  Param_Vertices_Print(stdout,PA->V,param_name);
  Domain_Free(A);
  Domain_Free(B);
  Param_Polyhedron_Free( PA );
  free(param_name);
  return 0;
} /* main */



