# Try to find the osl library

# OSL_FOUND       - System has osl lib
# OSL_INCLUDE_DIR - The osl include directory
# OSL_LIBRARY     - Library needed to use osl


if (OSL_INCLUDE_DIR AND OSL_LIBRARY)
	# Already in cache, be silent
	set(OSL_FIND_QUIETLY TRUE)
endif()

find_path(OSL_INCLUDE_DIR NAMES osl/osl.h)
find_library(OSL_LIBRARY NAMES osl)

if (OSL_LIBRARY AND OSL_INCLUDE_DIR)
	message(STATUS "Library osl found =) ${OSL_LIBRARY}")
else()
	message(STATUS "Library osl not found =(")
endif()


include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(OSL DEFAULT_MSG OSL_INCLUDE_DIR OSL_LIBRARY)

mark_as_advanced(OSL_INCLUDE_DIR OSL_LIBRARY)
