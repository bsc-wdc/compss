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

#ifndef _EXT_EHRHART_H_
#define _EXT_EHRHART_H_

extern Enumeration *Domain_Enumerate(Polyhedron *D, Polyhedron *C,
				     unsigned MAXRAYS, const char **pn);

extern void new_eadd (evalue *e1,evalue *res);

extern void Scalar_product(Value *p1,Value *p2,unsigned length, Value *r);

#endif
