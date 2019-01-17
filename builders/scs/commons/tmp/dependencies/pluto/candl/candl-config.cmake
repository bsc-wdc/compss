# Try to find the candl library

# CANDL_FOUND       - System has candl lib
# CANDL_INCLUDE_DIR - The candl include directory
# CANDL_LIBRARY     - Library needed to use candl


if (CANDL_INCLUDE_DIR AND CANDL_LIBRARY)
	# Already in cache, be silent
	set(CANDL_FIND_QUIETLY TRUE)
endif()

find_path(CANDL_INCLUDE_DIR NAMES candl/candl.h)
find_library(CANDL_LIBRARY NAMES candl)

if (CANDL_LIBRARY AND CANDL_INCLUDE_DIR)
	message(STATUS "Library candl found =) ${CANDL_LIBRARY}")
else()
	message(STATUS "Library candl not found =(")
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(CANDL DEFAULT_MSG CANDL_INCLUDE_DIR CANDL_LIBRARY)

mark_as_advanced(CANDL_INCLUDE_DIR CANDL_LIBRARY)
