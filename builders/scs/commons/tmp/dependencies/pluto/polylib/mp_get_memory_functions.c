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
#include <gmp.h>

void mp_get_memory_functions(
		void *(**alloc_func_ptr) (size_t),
		void *(**realloc_func_ptr) (void *, size_t, size_t),
		void (**free_func_ptr) (void *, size_t))
{
    if (alloc_func_ptr)
	*alloc_func_ptr = __gmp_allocate_func;
    if (realloc_func_ptr)
	*realloc_func_ptr = __gmp_reallocate_func;
    if (free_func_ptr)
	*free_func_ptr = __gmp_free_func;
}
