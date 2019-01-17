# Try to find the isl library

# ISL_FOUND       - System has isl lib
# ISL_INCLUDE_DIR - The isl include directory
# ISL_LIBRARY     - Library needed to use isl


if (ISL_INCLUDE_DIR AND ISL_LIBRARY)
	# Already in cache, be silent
	set(ISL_FIND_QUIETLY TRUE)
endif()

find_path(ISL_INCLUDE_DIR NAMES isl/version.h)
find_library(ISL_LIBRARY NAMES isl)

if (ISL_LIBRARY AND ISL_INCLUDE_DIR)
	message(STATUS "Library isl found =) ${ISL_LIBRARY}")
else()
	message(STATUS "Library isl not found =(")
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(ISL DEFAULT_MSG ISL_INCLUDE_DIR ISL_LIBRARY)

mark_as_advanced(ISL_INCLUDE_DIR ISL_LIBRARY)
