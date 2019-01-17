# Try to find the piplib library

# PIPLIB64_FOUND       - System has piplib lib
# PIPLIB64_INCLUDE_DIR - The piplib include directory
# PIPLIB64_LIBRARY     - Library needed to use piplib


if (PIPLIB64_INCLUDE_DIR AND PIPLIB64_LIBRARY)
	# Already in cache, be silent
	set(PIPLIB64_FIND_QUIETLY TRUE)
endif()

find_path(PIPLIB64_INCLUDE_DIR NAMES piplib/piplib64.h)
find_library(PIPLIB64_LIBRARY NAMES piplib_dp)

if (PIPLIB64_LIBRARY AND PIPLIB64_INCLUDE_DIR)
	message(STATUS "Library piplib64 found =) ${PIPLIB64_LIBRARY}")
else()
	message(STATUS "Library piplib64 not found =(")
endif()


include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(PIPLIB64 DEFAULT_MSG PIPLIB64_INCLUDE_DIR PIPLIB64_LIBRARY)

mark_as_advanced(PIPLIB64_INCLUDE_DIR PIPLIB64_LIBRARY)
