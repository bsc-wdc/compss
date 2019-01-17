# Try to find the piplib library

# PIPLIB_DP_FOUND       - System has piplib lib
# PIPLIB_DP_INCLUDE_DIR - The piplib include directory
# PIPLIB_DP_LIBRARY     - Library needed to use piplib


if (PIPLIB_DP_INCLUDE_DIR AND PIPLIB_DP_LIBRARY)
	# Already in cache, be silent
	set(PIPLIB_DP_FIND_QUIETLY TRUE)
endif()

find_path(PIPLIB_DP_INCLUDE_DIR NAMES piplib/piplib_dp.h)
find_library(PIPLIB_DP_LIBRARY NAMES piplib_dp)

if (PIPLIB_DP_LIBRARY AND PIPLIB_DP_INCLUDE_DIR)
	message(STATUS "Library piplib_dp found =) ${PIPLIB_DP_LIBRARY}")
else()
	message(STATUS "Library piplib_dp not found =(")
endif()


include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(PIPLIB_DP DEFAULT_MSG PIPLIB_DP_INCLUDE_DIR PIPLIB_DP_LIBRARY)

mark_as_advanced(PIPLIB_DP_INCLUDE_DIR PIPLIB_DP_LIBRARY)
