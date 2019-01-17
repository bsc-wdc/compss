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

#ifndef _polyhedron_H_
#define _polyhedron_H_

/* Make sure the Constraint member is valid */
#define POL_ENSURE_INEQUALITIES(P) 				    		\
	if (F_ISSET(P, POL_VALID) && !F_ISSET(P, POL_INEQUALITIES)) 		\
	    Polyhedron_Compute_Dual(P);
/* Make sure the Ray member is valid */
#define POL_ENSURE_POINTS(P) 					    		\
	if (F_ISSET(P, POL_VALID) && !F_ISSET(P, POL_POINTS)) 			\
	    Polyhedron_Compute_Dual(P);
/* Make sure the Constraint member is valid and non-redundant */
#define POL_ENSURE_FACETS(P) 					    		\
	if (F_ISSET(P, POL_VALID) && !F_ISSET(P, POL_FACETS)) 			\
	    Polyhedron_Compute_Dual(P);
/* Make sure the Ray member is valid and non-redundant */
#define POL_ENSURE_VERTICES(P) 					    		\
	if (F_ISSET(P, POL_VALID) && !F_ISSET(P, POL_VERTICES))			\
	    Polyhedron_Compute_Dual(P);

#if defined(__cplusplus)
extern "C" {
#endif

extern void Polyhedron_Compute_Dual(Polyhedron *P);

Polyhedron *DomainConstraintSimplify(Polyhedron *P, unsigned MaxRays);

extern Polyhedron *AddConstraints(Value *Con,unsigned NbConstraints,
				   Polyhedron *Pol,unsigned NbMaxRays );
extern Polyhedron *AddPolyToDomain(Polyhedron *Pol,Polyhedron *PolDomain);
extern Polyhedron *AddRays (Value *Ray2,unsigned NbRay2,Polyhedron
			    *Pol1,unsigned NbMaxRays);
extern Polyhedron *align_context(Polyhedron *Pol,int align_dimension,
				  int NbMaxRays);
extern Polyhedron *Constraints2Polyhedron(Matrix *Constraints, unsigned
					  NbMaxRays);
extern Polyhedron *Disjoint_Domain( Polyhedron *Pol, int flag, unsigned
               NbMaxRays );
extern Polyhedron *DomainAddConstraints(Polyhedron *Pol,Matrix *Mat,
					unsigned NbMaxRays);
extern Polyhedron *DomainAddRays(Polyhedron *Pol,Matrix *Ray,
				 unsigned NbMaxConstrs);
extern Polyhedron *DomainConvex(Polyhedron *Pol,unsigned NbMaxConstrs);
extern Interval *DomainCost(Polyhedron *Pol,Value *Cost);
extern Polyhedron *DomainDifference(Polyhedron *Pol1,Polyhedron *Pol2,
				    unsigned NbMaxRays);
extern Polyhedron *DomainImage(Polyhedron *Pol,Matrix *Func,unsigned
			       NbMaxConstrs);
extern Polyhedron *DomainIntersection(Polyhedron *Pol1,Polyhedron
				      *Pol2, unsigned NbMaxRays);
extern Polyhedron *DomainPreimage(Polyhedron *Pol,Matrix *Func,
				  unsigned NbMaxRays);
extern Polyhedron *DomainSimplify(Polyhedron *Pol1, Polyhedron *Pol2,
				  unsigned NbMaxRays);
extern Polyhedron *Stras_DomainSimplify(Polyhedron *Pol1, Polyhedron *Pol2,
					unsigned NbMaxRays);
extern Polyhedron *DomainUnion(Polyhedron *Pol1,Polyhedron *Pol2,
			       unsigned NbMaxRays);
extern Polyhedron *Domain_Copy(Polyhedron *Pol);
extern void Domain_Free (Polyhedron *Pol);
extern void Domain_PrintConstraints(FILE *Dst, const char *Format,
				    Polyhedron *Pol);
extern Polyhedron *Empty_Polyhedron(unsigned Dimension);
extern int Gauss(Matrix *Mat,int NbEq,int Dimension);
extern int lower_upper_bounds(int pos,Polyhedron *P,Value *context,
			      Value *LBp,Value *UBp);
extern Matrix *Polyhedron2Constraints(Polyhedron *Pol);
extern Matrix *Polyhedron2Rays(Polyhedron *Pol);
extern int PolyhedronIncludes(Polyhedron *Pol1,Polyhedron *Pol2);
extern Polyhedron* Polyhedron_Alloc(unsigned Dimension,unsigned
				    NbConstraints,unsigned NbRays);
extern Polyhedron *Polyhedron_Copy(Polyhedron *Pol);
extern void Polyhedron_Free(Polyhedron *Pol);
extern Polyhedron *Polyhedron_Image(Polyhedron *Pol,Matrix *Func,
				    unsigned NbMaxConstrs);
extern Polyhedron *Polyhedron_Preimage(Polyhedron *Pol,Matrix *Func,
				       unsigned NbMaxRays);
extern void Polyhedron_Print(FILE *Dst, const char *Format, const Polyhedron *Pol);
extern void Polyhedron_PrintConstraints(FILE *Dst, const char *Format,
                Polyhedron *Pol);
extern Polyhedron *Polyhedron_Scan(Polyhedron *D,Polyhedron *C,
				   unsigned MAXRAYS);
extern void PolyPrint(Polyhedron *Pol);
extern Polyhedron *Rays2Polyhedron(Matrix *Ray,unsigned NbMaxConstrs);
extern Polyhedron *SubConstraint(Value *Con,Polyhedron *Pol,
				 unsigned NbMaxRays, int Pass);
extern Polyhedron *Universe_Polyhedron (unsigned Dimension);

#if defined(__cplusplus)
}
#endif

#endif /* _polyhedron_H_ */
