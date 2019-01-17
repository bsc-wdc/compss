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

/* Version "abort" de l'assert de /usr/include/assert.h 
 * Il est installe dans Linear de maniere a masquer /usr/include/assert.h
 *
 * Il faut faire un include de <stdio.h> pour l'utiliser.
 */

# ifndef NDEBUG
# define _assert(ex)	{if (!(ex)){(void)fprintf(stderr,"Assertion failed: file \"%s\", line %d\n", __FILE__, __LINE__);(void) abort();}}
# define assert(ex)	_assert(ex)
# else
# define _assert(ex)
# define assert(ex)
# endif
