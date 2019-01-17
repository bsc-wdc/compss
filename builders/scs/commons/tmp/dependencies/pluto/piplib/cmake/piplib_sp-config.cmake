# Try to find the piplib library

# PIPLIB_SP_FOUND       - System has piplib lib
# PIPLIB_SP_INCLUDE_DIR - The piplib include directory
# PIPLIB_SP_LIBRARY     - Library needed to use piplib


if (PIPLIB_SP_INCLUDE_DIR AND PIPLIB_SP_LIBRARY)
	# Already in cache, be silent
	set(PIPLIB_SP_FIND_QUIETLY TRUE)
endif()

find_path(PIPLIB_SP_INCLUDE_DIR NAMES piplib/piplib_sp.h)
find_library(PIPLIB_SP_LIBRARY NAMES piplib_sp)

if (PIPLIB_SP_LIBRARY AND PIPLIB_SP_INCLUDE_DIR)
	message(STATUS "Library piplib_sp found =) ${PIPLIB_SP_LIBRARY}")
else()
	message(STATUS "Library piplib_sp not found =(")
endif()


include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(PIPLIB_SP DEFAULT_MSG PIPLIB_SP_INCLUDE_DIR PIPLIB_SP_LIBRARY)

mark_as_advanced(PIPLIB_SP_INCLUDE_DIR PIPLIB_SP_LIBRARY)
