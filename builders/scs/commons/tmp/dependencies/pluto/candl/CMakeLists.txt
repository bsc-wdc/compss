cmake_minimum_required(VERSION 2.8)


set(PACKAGE_VERSION "0.6.2")
set(RELEASE "${PACKAGE_VERSION}")
set(BITS "32")
set(DEFINE_HAS_ISL_LIB "")
set(top_srcdir "${CMAKE_CURRENT_SOURCE_DIR}")

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)


# User's settings - C Flags

# 	set(release "TRUE")
	set(release "FALSE")

	# Release
	if (release)
		set(CMAKE_C_FLAGS "-O3")
	# Debug # valgrind --show-reachable=yes --leak-check=full -v exe
	else()
		set(CMAKE_C_FLAGS "-O0 -g3")
	endif()

# User's settings - General C Flags
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -pedantic -std=c99")


# Build doxygen
	find_package(Doxygen)
	if (DOXYGEN_FOUND)
		configure_file("doc/Doxyfile.in" "Doxyfile")
		add_custom_target(
			doxygen
			${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
			WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
			COMMENT "Generating API documentation with Doxygen" VERBATIM
		)
	else()
		message (STATUS "Doxygen not found :( API documentation can not be built")
	endif()

# Build documentation

	# doc
	find_program(texi2pdf_exe texi2pdf)
	if (texi2pdf_exe)
		add_custom_target(
			doc
			${texi2pdf_exe} ${CMAKE_CURRENT_SOURCE_DIR}/doc/candl.texi --output=${CMAKE_CURRENT_BINARY_DIR}/candl.pdf
			WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
			COMMENT "Generating documentation (pdf) (with texi2pdf)" VERBATIM
		)
	else()
		message (STATUS "texi2pdf not found :( Documentation can not be built")
	endif()


# osl
	find_package(osl REQUIRED)

# GMP & piplib
	message(STATUS "---")
	find_library(gmp_LIB gmp)
	if (gmp_LIB)
		message (STATUS "Library gmp found =) ${gmp_LIB}")
		set(BITS "MP")
		add_definitions("-DCANDL_LINEAR_VALUE_IS_MP")
		# piplib_gmp
		find_package(piplib_gmp REQUIRED)
	else()
		message(STATUS "Library gmp not found :(")
		add_definitions("-DCANDL_LINEAR_VALUE_IS_LONG_LONG")
		# piplib_dp
		find_package(piplib_dp REQUIRED)
	endif()

# Include directories (to use #include <> instead of #include "")

	# include/candl/macros.h
	configure_file("include/candl/macros.h.in" "include/candl/macros.h")
	configure_file("include/candl/piplib.h.in" "include/candl/piplib.h")
	include_directories("${CMAKE_CURRENT_BINARY_DIR}/include")
	# candl
	include_directories("./include")


# Compiler log
	message(STATUS "---")
	message(STATUS "C compiler = ${CMAKE_C_COMPILER}")
	if (release)
		message(STATUS "Mode Release")
	else()
		message(STATUS "Mode Debug")
	endif()
	message(STATUS "C flags    = ${CMAKE_C_FLAGS}")


# Library

	message(STATUS "---")

	# files .c
	file(
		GLOB_RECURSE
		sources
		source/*
	)
	string(REPLACE "${CMAKE_CURRENT_SOURCE_DIR}/source/candl.c;" "" sources "${sources}") # with ;
	string(REPLACE "${CMAKE_CURRENT_SOURCE_DIR}/source/candl.c" "" sources "${sources}")  # without ;

	# Shared
	add_library(
		candl
		SHARED
		${sources}
	)
	target_link_libraries(candl ${OSL_LIBRARY})
	if (gmp_LIB)
		target_link_libraries(candl ${PIPLIB_GMP_LIBRARY})
                target_link_libraries(candl ${gmp_LIB})
	else()
		target_link_libraries(candl ${PIPLIB_DP_LIBRARY})
	endif()
	message(STATUS "Add candl library (shared)")

	# Static
	add_library(
		candl_static
		STATIC
		${sources}
	)
	set_target_properties(candl_static PROPERTIES OUTPUT_NAME candl)
	if (gmp_LIB)
		target_link_libraries(candl_static ${PIPLIB_GMP_LIBRARY})
                target_link_libraries(candl ${gmp_LIB})
	else()
		target_link_libraries(candl_static ${PIPLIB_DP_LIBRARY})
	endif()
	target_link_libraries(candl_static ${OSL_LIBRARY})
	message(STATUS "Add candl library (static)")


# Executables & tests

	message(STATUS "---") # candl

	message(STATUS "Add executable candl")
	add_executable(candl_exe "source/candl.c")
	set_target_properties(candl_exe PROPERTIES OUTPUT_NAME "candl")
	target_link_libraries(candl_exe candl_static ${OSL_LIBRARY})
	if (gmp_LIB)
		target_link_libraries(candl_exe candl_static ${gmp_LIB})
	endif()

	# candl test
	find_program(bash_exe bash)
	if (bash_exe)
	
		message(STATUS "---")

		enable_testing()
		
		file(
			GLOB_RECURSE
			tests_unitary
			tests/unitary/*.c
		)

		foreach(test ${tests_unitary})
			string(REPLACE ".c" "" test "${test}")
			message(STATUS "Add Unitary test ${test}")
			add_test(
				"tests_unitary_${test}"
				#"${bash_exe}"
				"${CMAKE_CURRENT_SOURCE_DIR}/tests/checker.sh"
				"${test}"
				"${test}"
				"0"
				"candl"
			)
			set_tests_properties("tests_unitary_${test}" PROPERTIES ENVIRONMENT "top_builddir=${CMAKE_CURRENT_BINARY_DIR}")
		endforeach()

		file(
			GLOB_RECURSE
			tests_transformations_must_fail
			tests/transformations/must_fail/*.c
		)

		foreach(test ${tests_transformations_must_fail})
			string(REPLACE ".c" "" test "${test}")
			message(STATUS "Add Transformation must fail test ${test}")
			add_test(
				"tests_transformations_must_fail_${test}"
				"${bash_exe}"
				"${CMAKE_CURRENT_SOURCE_DIR}/tests/checker.sh"
				"${test}"
				"${test}"
				"1"
				"candl"
			)
			set_tests_properties("tests_transformations_must_fail_${test}" PROPERTIES ENVIRONMENT "top_builddir=${CMAKE_CURRENT_BINARY_DIR}")
		endforeach()

		file(
			GLOB_RECURSE
			tests_transformations_working
			tests/transformations/working/*.c
		)

		foreach(test ${tests_transformations_working})
			string(REPLACE ".c" "" test "${test}")
			message(STATUS "Add Transformation working test ${test}")
			add_test(
				"tests_transformations_working_${test}"
				"${bash_exe}"
				"${CMAKE_CURRENT_SOURCE_DIR}/tests/checker.sh"
				"${test}"
				"${test}"
				"1"
				"candl"
			)
			set_tests_properties("tests_transformations_working_${test}" PROPERTIES ENVIRONMENT "top_builddir=${CMAKE_CURRENT_BINARY_DIR}")
		endforeach()

	endif()


# Install

	install(TARGETS candl LIBRARY DESTINATION lib)
	install(TARGETS candl_static ARCHIVE DESTINATION lib)
	install(DIRECTORY include/ DESTINATION include FILES_MATCHING PATTERN "*.h")
	install(DIRECTORY include/ DESTINATION include FILES_MATCHING PATTERN "*.hpp")
	install(DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include/" DESTINATION include FILES_MATCHING PATTERN "*.h")
	install(FILES candl-config.cmake DESTINATION lib/candl)
	install(TARGETS candl_exe RUNTIME DESTINATION bin)


# Little help

	message(STATUS "You can execute:")
	message(STATUS "    make         # To compile candl library & candl")
	if (bash_exe)
		message(STATUS "    make test    # To execute tests")
		message(STATUS "                   (with the first candl in the $PATH (?))")
	endif()
	message(STATUS "    make install # To install library, include and CMake module")
	message(STATUS "                 # If you need root access:")
	message(STATUS "                 #     sudo make install")
	message(STATUS "                 #     su -c \"make install\"")
	if (DOXYGEN_FOUND)
		message(STATUS "    make doxygen # To generate the Doxygen")
	endif()
	if( texi2pdf_exe)
		message(STATUS "    make doc     # To generate the documentation")
	endif()

	message(STATUS "---")
