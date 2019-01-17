# Try to find the cloog-isl library

# CLOOG_ISL_FOUND       - System has cloog-isl lib
# CLOOG_ISL_INCLUDE_DIR - The cloog-isl include directory
# CLOOG_ISL_LIBRARY     - Library needed to use cloog-isl


if (CLOOG_ISL_INCLUDE_DIR AND CLOOG_ISL_LIBRARY)
	# Already in cache, be silent
	set(CLOOG_ISL_FIND_QUIETLY TRUE)
endif()

find_path(CLOOG_ISL_INCLUDE_DIR NAMES cloog/isl/cloog.h)
find_library(CLOOG_ISL_LIBRARY NAMES cloog-isl)

if (CLOOG_ISL_LIBRARY AND CLOOG_ISL_INCLUDE_DIR)
	message(STATUS "Library cloog-isl found =) ${CLOOG_ISL_LIBRARY}")
else()
	message(STATUS "Library cloog-isl not found =(")
endif()


include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(CLOOG_ISL DEFAULT_MSG CLOOG_ISL_INCLUDE_DIR CLOOG_ISL_LIBRARY)

mark_as_advanced(CLOOG_ISL_INCLUDE_DIR CLOOG_ISL_LIBRARY)
