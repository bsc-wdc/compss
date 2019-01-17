# Version 1.1
# Public Domain
# Written by Maxime SCHMITT <maxime.schmitt@etu.unistra.fr>

#/////////////////////////////////////////////////////////////////////////////#
#                                                                             #
# Search for GNU Multiple Precision on the system                             #
# Call with find_package(GMP)                                                 #
# The module defines:                                                         #
#   - GMP_FOUND        - If GMP was found                                     #
#   - GMP_INCLUDE_DIRS - the GMP include directories                          #
#   - GMP_LIBRARIES    - the GMP library directories                          #
#   - GMP_VERSION      - the GMP library version                              #
#                                                                             #
#/////////////////////////////////////////////////////////////////////////////#

if (GMP_INCLUDE_DIR AND GMP_LIBRARIES)
  set(GMP_FIND_QUIETLY TRUE)
endif()

# Headers
find_path(GMP_INCLUDE_DIR NAMES gmp.h)

# library
find_library(GMP_LIBRARIES NAMES gmp libgmp)

# Version
set(filename "${GMP_INCLUDE_DIR}/gmp.h")
if (NOT EXISTS ${filename} AND NOT quiet)
  message(AUTHOR_WARNING "Unable to find ${filename}")
endif()
file(READ "${filename}" gmp_header)
set(gmp_match_major "__GNU_MP_VERSION")
set(gmp_match_minor "__GNU_MP_VERSION_MINOR")
set(gmp_match_patch "__GNU_MP_VERSION_PATCHLEVEL")
string(REGEX REPLACE ".*#[ \t]*define[ \t]*${gmp_match_major}[ \t]*([0-9]).*"
  "\\1" gmp_version_major "${gmp_header}")
string(REGEX REPLACE ".*#[ \t]*define[ \t]*${gmp_match_minor}[ \t]*([0-9]).*"
  "\\1" gmp_version_minor "${gmp_header}")
string(REGEX REPLACE ".*#[ \t]*define[ \t]*${gmp_match_patch}[ \t]*([0-9]).*"
  "\\1" gmp_version_patch "${gmp_header}")
if (gmp_version_major STREQUAL gmp_header OR
    gmp_version_minor STREQUAL gmp_header OR
    gmp_version_patch STREQUAL gmp_header)
  message(AUTHOR_WARNING "Unable to find gmp version")
else()
  set(GMP_VERSION "${gmp_version_major}.${gmp_version_minor}.${gmp_version_patch}")
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(GMP FOUND_VAR GMP_FOUND
  REQUIRED_VARS GMP_INCLUDE_DIR GMP_LIBRARIES
  VERSION_VAR GMP_VERSION)

mark_as_advanced(GMP_INCLUDE_DIR GMP_LIBRARIES GMP_VERSION)
