# Try to find the piplib library

# PIPLIB_GMP_FOUND       - System has piplib lib
# PIPLIB_GMP_INCLUDE_DIR - The piplib include directory
# PIPLIB_GMP_LIBRARY     - Library needed to use piplib


if (PIPLIB_GMP_INCLUDE_DIR AND PIPLIB_GMP_LIBRARY)
	# Already in cache, be silent
	set(PIPLIB_GMP_FIND_QUIETLY TRUE)
endif()

find_path(PIPLIB_GMP_INCLUDE_DIR NAMES piplib/piplib_gmp.h)
find_library(PIPLIB_GMP_LIBRARY NAMES piplib_gmp)

if (PIPLIB_GMP_LIBRARY AND PIPLIB_GMP_INCLUDE_DIR)
	message(STATUS "Library piplib_gmp found =) ${PIPLIB_GMP_LIBRARY}")
else()
	message(STATUS "Library piplib_gmp not found =(")
endif()


include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(PIPLIB_GMP DEFAULT_MSG PIPLIB_GMP_INCLUDE_DIR PIPLIB_GMP_LIBRARY)

mark_as_advanced(PIPLIB_GMP_INCLUDE_DIR PIPLIB_GMP_LIBRARY)
