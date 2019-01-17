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

/** homogenization.h -- Bavo Nootaert **/
#ifndef HOMOGENIZATION_H
#define HOMOGENIZATTON_H

#include <polylib/polylib.h>

Polyhedron *homogenize(Polyhedron *P, unsigned MAXRAYS);

void dehomogenize_evalue(evalue *ep,  int nb_param);
void dehomogenize_enode(enode *p, int nb_param);
void dehomogenize_enumeration(Enumeration *en, int nb_param, int maxRays);
Polyhedron *dehomogenize_polyhedron(Polyhedron *p, int maxRays);

#endif
