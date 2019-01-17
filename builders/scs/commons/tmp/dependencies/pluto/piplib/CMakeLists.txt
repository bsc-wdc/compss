cmake_minimum_required(VERSION 2.8)


set(VERSION "1.4.0")


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
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -std=c99 -pedantic")


# Build documentation
	find_program(texi2pdf_exe texi2pdf)
	if(texi2pdf_exe)
		file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/doc DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
		configure_file("${CMAKE_CURRENT_BINARY_DIR}/doc/gitversion.texi.in" "${CMAKE_CURRENT_BINARY_DIR}/doc/gitversion.texi")
		add_custom_target(
			doc
			${texi2pdf_exe} piplib.texi --output=${CMAKE_CURRENT_BINARY_DIR}/piplib.pdf
			WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/doc"
			COMMENT "Generating documentation (pdf) (with texi2pdf)" VERBATIM
		)
	else()
		message (STATUS "texi2pdf not found :( Documentation can not be built")
	endif()


# GMP
	message(STATUS "---")
	find_library(gmp_LIB gmp)
	if (gmp_LIB)
		message (STATUS "Library gmp found =) ${gmp_LIB}")
	else()
		message (STATUS "Library gmp not found :(")
	endif()

# Include directories (to use #include <> instead of #include "")

	# ./include/piplib/version.h
	configure_file("./include/piplib/version.h.in" "./include/piplib/version.h")
	include_directories("${CMAKE_CURRENT_BINARY_DIR}/include/piplib")
	# piplib
	set(pip_INCLUDE "./include")
	message(STATUS "Include pip = ${pip_INCLUDE}")
	include_directories("${pip_INCLUDE}")


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

	# files .c
	file(
		GLOB_RECURSE
		sources
		source/*
	)

	message(STATUS "---")

	# sp
	message(STATUS "Add library piplib_sp (shared)")
	add_library(
		piplib_sp
		SHARED
		${sources}
	)
	set_target_properties(piplib_sp PROPERTIES COMPILE_DEFINITIONS PIPLIB_INT_SP)

	message(STATUS "Add library piplib_sp (static)")
	add_library(
		piplib_sp_static
		STATIC
		${sources}
	)
	set_target_properties(piplib_sp_static PROPERTIES OUTPUT_NAME piplib_sp)
	set_target_properties(piplib_sp_static PROPERTIES COMPILE_DEFINITIONS PIPLIB_INT_SP)
	
	# dp
	message(STATUS "Add library piplib_dp (shared)")
	add_library(
		piplib_dp
		SHARED
		${sources}
	)
	set_target_properties(piplib_dp PROPERTIES COMPILE_DEFINITIONS PIPLIB_INT_DP)

	message(STATUS "Add library piplib_dp (static)")
	add_library(
		piplib_dp_static
		STATIC
		${sources}
	)
	set_target_properties(piplib_dp_static PROPERTIES OUTPUT_NAME piplib_dp)
	set_target_properties(piplib_dp_static PROPERTIES COMPILE_DEFINITIONS PIPLIB_INT_DP)

	# GMP
	if (gmp_LIB)
		message(STATUS "Add library piplib_gmp (shared)")
		add_library(
			piplib_gmp
			SHARED
			${sources}
		)
		set_target_properties(piplib_gmp PROPERTIES COMPILE_DEFINITIONS PIPLIB_INT_GMP)
		target_link_libraries(piplib_gmp ${gmp_LIB})

		message(STATUS "Add library piplib_gmp (static)")
		add_library(
			piplib_gmp_static
			STATIC
			${sources}
		)
		set_target_properties(piplib_gmp_static PROPERTIES OUTPUT_NAME piplib_gmp)
		set_target_properties(piplib_gmp_static PROPERTIES COMPILE_DEFINITIONS PIPLIB_INT_GMP)
		target_link_libraries(piplib_gmp_static ${gmp_LIB})
	endif()


# Executables & tests

	message(STATUS "---")

	set(exe "pip_sp")
	message(STATUS "Add executable ${exe}")
	add_executable(${exe} "source/maind.c")
	set_target_properties(${exe} PROPERTIES COMPILE_DEFINITIONS PIPLIB_INT_SP)
	target_link_libraries(${exe} piplib_sp_static)

	set(exe "pip_dp")
	message(STATUS "Add executable ${exe}")
	add_executable(${exe} "source/maind.c")
	set_target_properties(${exe} PROPERTIES COMPILE_DEFINITIONS PIPLIB_INT_DP)
	target_link_libraries(${exe} piplib_dp_static)

	if(gmp_LIB)
		set(exe "pip_gmp")
		message(STATUS "Add executable ${exe}")
		add_executable(${exe} "source/maind.c")
		set_target_properties(${exe} PROPERTIES COMPILE_DEFINITIONS PIPLIB_INT_GMP)
		target_link_libraries(${exe} piplib_gmp_static)
	endif()

	set(exe "exemple_sp")
	message(STATUS "Add executable ${exe}")
	add_executable(${exe} "example/example.c")
	set_target_properties(${exe} PROPERTIES COMPILE_DEFINITIONS PIPLIB_INT_SP)
	target_link_libraries(${exe} piplib_sp_static)

	set(exe "exemple_dp")
	message(STATUS "Add executable ${exe}")
	add_executable(${exe} "example/example.c")
	set_target_properties(${exe} PROPERTIES COMPILE_DEFINITIONS PIPLIB_INT_DP)
	target_link_libraries(${exe} piplib_dp_static)

	if(gmp_LIB)
		set(exe "exemple_gmp")
		message(STATUS "Add executable ${exe}")
		add_executable(${exe} "example/example.c")
		set_target_properties(${exe} PROPERTIES COMPILE_DEFINITIONS PIPLIB_INT_GMP)
		target_link_libraries(${exe} piplib_gmp_static)
	endif()

	# Test
	find_package(PythonInterp REQUIRED)
	if (PYTHONINTERP_FOUND)

		enable_testing()

		file(
			GLOB_RECURSE
			examples
			example/*.pip
		)

		foreach(test ${examples}) # Tests in test directory are ignored...
			message(STATUS "Add Test ${test}")
			STRING(REPLACE ".pip" ".ll" test_output "${test}")
			add_test(
				"${test}_pip_sp"
				"${CMAKE_CURRENT_SOURCE_DIR}/example/check_source_result.py"
				"${test}"
				"${test_output}"
				"${CMAKE_CURRENT_BINARY_DIR}/exemple_sp"
			)
			add_test(
				"${test}_pip_dp"
				"${CMAKE_CURRENT_SOURCE_DIR}/example/check_source_result.py"
				"${test}"
				"${test_output}"
				"${CMAKE_CURRENT_BINARY_DIR}/exemple_dp"
			)
			if(gmp_LIB)
				add_test(
					"${test}_pip_gmp"
					"${CMAKE_CURRENT_SOURCE_DIR}/example/check_source_result.py"
					"${test}"
					"${test_output}"
					"${CMAKE_CURRENT_BINARY_DIR}/exemple_gmp"
				)
			endif()
		endforeach()

	endif()


# Install

	# .h
	install(DIRECTORY include/ DESTINATION include FILES_MATCHING PATTERN "*.h")
	install(DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include/" DESTINATION include FILES_MATCHING PATTERN "*.h")
	# Libraries
	install(TARGETS piplib_sp LIBRARY DESTINATION lib)
	install(TARGETS piplib_dp LIBRARY DESTINATION lib)
	install(TARGETS piplib_gmp LIBRARY DESTINATION lib)
	install(TARGETS piplib_sp_static ARCHIVE DESTINATION lib)
	install(TARGETS piplib_dp_static ARCHIVE DESTINATION lib)
	install(TARGETS piplib_gmp_static ARCHIVE DESTINATION lib)
	# .cmake
	install(DIRECTORY include/ DESTINATION include FILES_MATCHING PATTERN "*.h")
	install(FILES cmake/piplib32-config.cmake DESTINATION lib/piplib32)
	install(FILES cmake/piplib32-config.cmake DESTINATION lib/piplib64)
	install(FILES cmake/piplibmp-config.cmake DESTINATION lib/piplibmp)
	install(FILES cmake/piplib_sp-config.cmake DESTINATION lib/piplib_sp)
	install(FILES cmake/piplib_dp-config.cmake DESTINATION lib/piplib_dp)
	install(FILES cmake/piplib_gmp-config.cmake DESTINATION lib/piplib_gmp)


# Little help

	message(STATUS "---")
	message(STATUS "You can execute:")
	message(STATUS "    make         # To compile all PipLib libraries")
	if (PYTHONINTERP_FOUND)
		message(STATUS "    make test    # To execute tests")
	endif()
	message(STATUS "    make install # To install library, include and CMake module")
	message(STATUS "                 # If you need root access:")
	message(STATUS "                 #     sudo make install")
	message(STATUS "                 #     su -c \"make install\"")
	if(texi2pdf_exe)
		message(STATUS "    make doc     # To generate the documentation")
	endif()

	message(STATUS "---")
