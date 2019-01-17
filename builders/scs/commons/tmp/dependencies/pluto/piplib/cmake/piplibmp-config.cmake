# Try to find the piplib library

# PIPLIBMP_FOUND       - System has piplib lib
# PIPLIBMP_INCLUDE_DIR - The piplib include directory
# PIPLIBMP_LIBRARY     - Library needed to use piplib


if (PIPLIBMP_INCLUDE_DIR AND PIPLIBMP_LIBRARY)
	# Already in cache, be silent
	set(PIPLIBMP_FIND_QUIETLY TRUE)
endif()

find_path(PIPLIBMP_INCLUDE_DIR NAMES piplib/piplibMP.h)
find_library(PIPLIBMP_LIBRARY NAMES piplib_gmp)

if (PIPLIBMP_LIBRARY AND PIPLIBMP_INCLUDE_DIR)
	message(STATUS "Library piplibMP found =) ${PIPLIBMP_LIBRARY}")
else()
	message(STATUS "Library piplibMP not found =(")
endif()


include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(PIPLIBMP DEFAULT_MSG PIPLIBMP_INCLUDE_DIR PIPLIBMP_LIBRARY)

mark_as_advanced(PIPLIBMP_INCLUDE_DIR PIPLIBMP_LIBRARY)
