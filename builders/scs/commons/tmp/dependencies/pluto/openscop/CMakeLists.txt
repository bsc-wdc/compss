cmake_minimum_required(VERSION 3.1)

#///////////////////////////////////////////////////////////////////#
#                              PROJECT                              #
#///////////////////////////////////////////////////////////////////#

set(OSL_VERSION_MAJOR 0)
set(OSL_VERSION_MINOR 9)
set(OSL_VERSION_PATCH 2)
set(PACKAGE_VERSION
  ${OSL_VERSION_MAJOR}.${OSL_VERSION_MINOR}.${OSL_VERSION_PATCH})

project(osl
  LANGUAGES C)

# Default to Debug build
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
  message(STATUS "No building type advertised, default to Release")
endif()

#///////////////////////////////////////////////////////////////////#
#                           DEPENDENCIES                            #
#///////////////////////////////////////////////////////////////////#

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules)

find_package(GMP)
if(GMP_FOUND)
  add_library(gmp INTERFACE IMPORTED)
  set_property(TARGET gmp
    PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${GMP_INCLUDE_DIRS})
  set_property(TARGET gmp
    PROPERTY INTERFACE_LINK_LIBRARIES ${GMP_LIBRARIES})
else()
  message(STATUS "GMP not found, using old style integers")
endif()

#///////////////////////////////////////////////////////////////////#
#                        COMPILATION OPTIONS                        #
#///////////////////////////////////////////////////////////////////#

option(BUILD_SHARED_LIBS
  "Build as shared lib (default). Set to FALSE if you want static library"
  TRUE)

include(cmake/compiler-flags.cmake)

set(CMAKE_C_FLAGS_RELEASE "-O3 -mtune=generic")
set(CMAKE_C_FLAGS_DEBUG
  "${COMPILER_AVALIABLE_WARNINGS} ${COMPILER_ADDRESS_SANITIZER_FLAG} ${COMPILER_UNDEFINED_SANITIZER_FLAG} -Og -g")
set(CMAKE_C_FLAGS_OPTIMIZED
  "${COMPILER_LTO_FLAG} ${COMPILER_MARCH_NATIVE} -O3")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELEASE} -g")

# Use full RPATH on build tree
set(CMAKE_SKIP_BUILD_RPATH FALSE)
# Do not build with install RPATH
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
# Set the RPATH when install
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
# Only set RPATH if the installation directory is not a system directory
LIST(FIND
  CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib"
  isSystemDir)
if("${isSystemDir}" STREQUAL "-1")
  set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
else()
  set(CMAKE_INSTALL_RPATH "")
endif()

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/include/osl/scop.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/include/osl/scop.h)

add_subdirectory(source)

#///////////////////////////////////////////////////////////////////#
#                           DOCUMENTATION                           #
#///////////////////////////////////////////////////////////////////#

add_subdirectory(doc)

#///////////////////////////////////////////////////////////////////#
#                               TEST                                #
#///////////////////////////////////////////////////////////////////#

enable_testing()
add_subdirectory(tests)

#///////////////////////////////////////////////////////////////////#
#                             INSTALL                               #
#///////////////////////////////////////////////////////////////////#

install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/"
  DESTINATION include FILES_MATCHING PATTERN "*.h")
install(DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include/"
  DESTINATION include FILES_MATCHING PATTERN "*.h")

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/cmake_uninstall.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
  IMMEDIATE @ONLY)
add_custom_target(uninstall
  COMMAND ${CMAKE_COMMAND} -P
  ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)

#///////////////////////////////////////////////////////////////////#
#                           USER INFOS                              #
#///////////////////////////////////////////////////////////////////#

message(STATUS "")
message(STATUS "************************* OSL *************************")
message(STATUS "")
message(STATUS "Configuration done.")

string(TOUPPER ${CMAKE_BUILD_TYPE} CURRENT_BUILD_TYPE)
if (CURRENT_BUILD_TYPE MATCHES RELEASE)
  message(STATUS "Build type is Release")
  message(STATUS "C compiler = ${CMAKE_C_COMPILER}")
  message(STATUS "C flags    =  ${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_RELEASE}")
elseif(CURRENT_BUILD_TYPE MATCHES OPTIMIZED)
  message(STATUS "C compiler = ${CMAKE_C_COMPILER}")
  message(STATUS "C flags    = ${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_OPTIMIZED}")
elseif(CURRENT_BUILD_TYPE MATCHES RELWITHDEBINFO)
  message(STATUS "C compiler = ${CMAKE_C_COMPILER}")
  message(STATUS "C flags    = ${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_RELWITHDEBINFO}")
else(CURRENT_BUILD_TYPE MATCHES DEBUG)
  message(STATUS "C compiler = ${CMAKE_C_COMPILER}")
  message(STATUS "C flags    = ${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_DEBUG}")
else(CURRENT_BUILD_TYPE MATCHES DEBUG)
  message(STATUS "C compiler = ${CMAKE_C_COMPILER}")
  message(STATUS "C flags    = ${CMAKE_C_FLAGS}")
endif(CURRENT_BUILD_TYPE MATCHES RELEASE)

message(STATUS "")
message(STATUS "You can execute:")
message(STATUS "    make          # To compile osl library")
if (PYTHONINTERP_FOUND)
  message(STATUS "    make test     # To execute tests")
endif()
message(STATUS "    make install  # To install library, include and CMake module")
message(STATUS "                  # If you need root access:")
message(STATUS "                  #     sudo make install")
message(STATUS "                  #     su -c \"make install\"")
if(DOXYGEN_FOUND)
  message(STATUS "    make doxygen  # To generate the Doxygen")
endif()
if(texi2pdf_exe)
  message(STATUS "    make doc      # To generate the documentation")
endif()
message(STATUS "")
message(STATUS "******************************************************")
message(STATUS "")
