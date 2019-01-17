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

#ifndef _ehrhart_H_
#define _ehrhart_H_

/*********************** User defines ******************************/

/* Print all overflow warnings, or just one per domain             */
/* #define ALL_OVERFLOW_WARNINGS */

/******************* End of user defines ***************************/


#ifndef ALL_OVERFLOW_WARNINGS
extern int overflow_warning_flag;
#endif

#if defined(__cplusplus)
extern "C" {
#endif

extern void count_points ( int pos, Polyhedron *P, Value *context, Value *res );
extern void eadd ( evalue *e1, evalue *res );
extern enode *ecopy ( enode *e );
extern void edot ( enode *v1, enode *v2, evalue *res );
extern enode *new_enode( enode_type type,int size, int pos );
extern void free_evalue_refs ( evalue *e );
extern Enumeration *Polyhedron_Enumerate(Polyhedron *P, Polyhedron *C,
				         unsigned MAXRAYS, const char **pname);
extern void print_enode(FILE *DST, enode *p, const char **pname);
extern void print_evalue(FILE *DST, evalue *e, const char **pname);
extern void reduce_evalue (evalue *e);
extern void Enumeration_Free(Enumeration *en);
extern Enumeration *Ehrhart_Quick_Apx(Matrix * M, Matrix * C, 
				      Matrix ** Validity_Lattice, 
				      unsigned MAXRAYS);
extern Enumeration * Enumeration_zero(unsigned int nbParms, 
				      unsigned int maxRays);

#if defined(__cplusplus)
}
#endif

#endif /* _ehrhart_H_ */
