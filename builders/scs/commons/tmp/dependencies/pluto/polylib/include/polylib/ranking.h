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

/**
 * Tools to compute the ranking function of an iteration J: the number of
 * integer points in P that are lexicographically inferior to J 
 * @author B. Meister <meister@icps.u-strasbg.fr>
 * 6/2005
 * LSIIT-ICPS, UMR 7005 CNRS Universite Louis Pasteur
 * HiPEAC Network
 */

#ifndef __BM_POLYLIB_RANKING_H__
#define __BM_POLYLIB_RANKING_H__
#include <polylib/polylib.h>

/*
 * Returns a list of polytopes needed to compute
 * the number of points in P that are lexicographically
 * smaller than a given point in D.
 * Only the first dim dimensions are taken into account
 * for computing the lexsmaller relation.
 * The remaining variables are assumed to be extra
 * existential/control variables.
 * When P == D, this is the conventional ranking function.
 * P and D are assumed to have the same parameter domain C.
 *
 * The first polyhedron in the list returned is the
 * updated context: a combination of D and C or an extended C.
 *
 * The order of the variables in the remaining polyhedra is
 * - first dim variables of P
 * - existential variables of P
 * - existential variables of D
 * - first dim variables of D
 * - the parameters
 */
Polyhedron *LexSmaller(Polyhedron *P, Polyhedron *D, unsigned dim,
			Polyhedron *C, unsigned MAXRAYS);

/*
 * Returns the number of points in P that are lexicographically
 * smaller than a given point in D.
 * Only the first dim dimensions are taken into account
 * for computing the lexsmaller relation.
 * The remaining variables are assumed to be extra
 * existential/control variables.
 * When P == D, this is the conventional ranking function.
 * P and D are assumed to have the same parameter domain C.
 * The variables in the Enumeration correspond to the first dim variables
 * in D followed by the parameters of D (the variables of C).
 */
Enumeration *Polyhedron_LexSmallerEnumerate(Polyhedron *P, Polyhedron *D, 
					    unsigned dim,
					    Polyhedron *C, unsigned MAXRAYS);

/*
 * Returns a function that assigns a unique number to each point in the
 * polytope P ranging from zero to (number of points in P)-1.
 * The order of the numbers corresponds to the lexicographical order.
 *
 * C is the parameter context of the polytope
 */
Enumeration *Polyhedron_Ranking(Polyhedron *P, Polyhedron *C, unsigned MAXRAYS);

#endif /* __BM_POLYLIB_RANKING_H__ */
