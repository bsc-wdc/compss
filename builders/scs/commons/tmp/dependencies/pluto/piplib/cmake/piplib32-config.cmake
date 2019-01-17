# Try to find the piplib library

# PIPLIB32_FOUND       - System has piplib lib
# PIPLIB32_INCLUDE_DIR - The piplib include directory
# PIPLIB32_LIBRARY     - Library needed to use piplib


if (PIPLIB32_INCLUDE_DIR AND PIPLIB32_LIBRARY)
	# Already in cache, be silent
	set(PIPLIB32_FIND_QUIETLY TRUE)
endif()

find_path(PIPLIB32_INCLUDE_DIR NAMES piplib/piplib32.h)
find_library(PIPLIB32_LIBRARY NAMES piplib_sp)

if (PIPLIB32_LIBRARY AND PIPLIB32_INCLUDE_DIR)
	message(STATUS "Library piplib32 found =) ${PIPLIB32_LIBRARY}")
else()
	message(STATUS "Library piplib32 not found =(")
endif()


include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(PIPLIB32 DEFAULT_MSG PIPLIB32_INCLUDE_DIR PIPLIB32_LIBRARY)

mark_as_advanced(PIPLIB32_INCLUDE_DIR PIPLIB32_LIBRARY)
