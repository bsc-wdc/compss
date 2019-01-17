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

#ifndef _SolveDio_h_
#define _SolveDio_h_

#if defined(__cplusplus)
extern "C" {
#endif

extern int SolveDiophantine ( Matrix *M, Matrix **U, Vector **X );

#if defined(__cplusplus)
}
#endif

#endif /* _SolveDio_h_ */
