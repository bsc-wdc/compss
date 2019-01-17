/*       Polyhedron disjoint intersections
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


/*
 disjoint_union_sep computes the disjoint union of the given list of domains.
 input:
    (integer) # of polyhedra
    list of polyhedra in the usual matrix (constraints) format

 output:
    list of polyhedra (constraint matrices) having no integer point in common
*/

#include <stdio.h>
#include <stdlib.h>

#include <polylib/polylib.h>

#define WS 0


/* Procedure to print constraints of a domain */
void AffContraintes(Polyhedron *p)
{
	for( ;p;p=p->next)
	{
		Polyhedron_PrintConstraints(stdout, P_VALUE_FMT, p );
		printf("\n");
	}
}


int main() {
  
	int np, i;

	Matrix *a;
	Polyhedron *A, *tmp, *DD;

	scanf( "%d", &np );

	A = NULL;
	for( i=0 ; i<np ; i++ )
	{
		a = Matrix_Read();
		tmp = Constraints2Polyhedron(a,WS);
		Matrix_Free(a);
		tmp ->next = A;
		A = tmp;
	}


	DD = Disjoint_Domain( A, 0, WS );

	AffContraintes(DD);

	Domain_Free( DD );
	Domain_Free( A );

	return 0;
}



