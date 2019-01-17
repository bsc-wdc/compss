/*       Rays 2 Polyhedron
 */
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


#define WS 0

#include <stdio.h>
#include <stdlib.h>
#include <polylib/polylib.h>

int main() {
	
  Matrix *R;
  Polyhedron *P; 
  
  R = Matrix_Read();
  if(R->NbColumns < 2) {
    printf("Wrong input: %d columns\n", R->NbColumns );
    Matrix_Free(R);
    exit(1);
  }
  P = Rays2Polyhedron(R,WS);
  Matrix_Free(R);
  Polyhedron_Print(stdout,P_VALUE_FMT,P);  
  Domain_Free(P);
  return 0;
}

