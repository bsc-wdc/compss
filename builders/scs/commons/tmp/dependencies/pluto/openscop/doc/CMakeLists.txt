find_package(Doxygen)
if(DOXYGEN_FOUND)
  if(NOT TARGET doxygen)
    add_custom_target(doxygen)
  endif()
  set(top_srcdir "${PROJECT_SOURCE_DIR}")
  configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in"
    "${CMAKE_CURRENT_BINARY_DIR}/Doxyfile")
  add_custom_target(
    osl-doxygen
    ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generating API documentation with Doxygen" VERBATIM)
  add_dependencies(doxygen osl-doxygen)
else()
  message (STATUS "Doxygen not found :( API documentation can not be built")
endif()

find_program(texi2pdf_exe texi2pdf)
if(texi2pdf_exe)
  if(NOT TARGET doc)
    add_custom_target(doc)
  endif()
  add_custom_target(
    osl-doc
    ${texi2pdf_exe} ${CMAKE_CURRENT_SOURCE_DIR}/openscop.texi --output=${CMAKE_CURRENT_BINARY_DIR}/openscop.pdf
    WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
    COMMENT "Generating documentation (pdf) (with texi2pdf)" VERBATIM)
  add_dependencies(doc osl-doc)
else()
  message (STATUS "texi2pdf not found :( Documentation can not be built")
endif()
