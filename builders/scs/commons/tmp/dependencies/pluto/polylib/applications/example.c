
/* 
  main.c
    This file along with Zpolyhedron.c, polyhedron.c, Lattice.c,  
    Matop.c SolveDio.c, matrix.c and vector.c does the following :

     - Intersection of two Z-Domains.
     - Difference of two Z-domains. 
     - Image of a Z-domain by a invertible, 
        affine rational function. 
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


#include <polylib/polylib.h>

int main () {
  
  Matrix *a, *b; 
  Polyhedron *P;
  ZPolyhedron *Z1, *Z2, *Z3, *Z4;
  
  a = Matrix_Read ();
  b = Matrix_Read ();
  P = Constraints2Polyhedron (b, 200);
  Z1 = ZPolyhedron_Alloc (a, P);
  
  Matrix_Free (a);
  Matrix_Free (b);
  Domain_Free (P);
  
  a = Matrix_Read ();
  b = Matrix_Read ();
  P = Constraints2Polyhedron (b, 200);
  Z2 = ZPolyhedron_Alloc (a, P);
  
  Matrix_Free (a); 
  Matrix_Free (b); 
  Domain_Free (P);
  
  Z3 = ZDomainIntersection (Z1, Z2);
  printf ("\nZ3 = Z1 and Z2");
  ZDomainPrint(stdout,P_VALUE_FMT, Z3);

  a = Matrix_Read ();
  Z4 = ZDomainImage (Z1, a);
  printf ("\nZ4 = image (Z1 by a)");
  ZDomainPrint (stdout,P_VALUE_FMT, Z4);

  Matrix_Free (a);
  ZDomain_Free (Z1);
  ZDomain_Free (Z2);
  ZDomain_Free (Z3);
  ZDomain_Free (Z4);
  
  return 0;
} /* main */
