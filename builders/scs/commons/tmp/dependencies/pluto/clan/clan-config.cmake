# Try to find the clan library

# CLAN_FOUND       - System has clan lib
# CLAN_INCLUDE_DIR - The clan include directory
# CLAN_LIBRARY     - Library needed to use clan


if (CLAN_INCLUDE_DIR AND CLAN_LIBRARY)
	# Already in cache, be silent
	set(CLAN_FIND_QUIETLY TRUE)
endif()

find_path(CLAN_INCLUDE_DIR NAMES clan/clan.h)
find_library(CLAN_LIBRARY NAMES clan)

if (CLAN_LIBRARY AND CLAN_INCLUDE_DIR)
	message(STATUS "Library clan found =) ${CLAN_LIBRARY}")
else()
	message(STATUS "Library clan not found =(")
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(CLAN DEFAULT_MSG CLAN_INCLUDE_DIR CLAN_LIBRARY)

mark_as_advanced(CLAN_INCLUDE_DIR CLAN_LIBRARY)
