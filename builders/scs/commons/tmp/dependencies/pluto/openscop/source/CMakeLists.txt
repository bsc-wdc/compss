add_subdirectory(extensions)

add_library (osl
  body.c
  generic.c
  int.c
  interface.c
  names.c
  relation.c
  relation_list.c
  scop.c
  statement.c
  strings.c
  util.c
  vector.c
  ${OSL_EXTENSIONS_CFILES})

target_include_directories(osl PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
  $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>
  $<INSTALL_INTERFACE:include>)

set_property(TARGET osl PROPERTY C_STANDARD 11)
set_property(TARGET osl PROPERTY C_STANDARD 11)

install (TARGETS osl EXPORT osl-config
  INCLUDES DESTINATION include
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib/static
  )

install (EXPORT osl-config DESTINATION lib/osl/)

if (GMP_FOUND)
  target_link_libraries(osl
    PRIVATE gmp)
  target_compile_definitions(osl
    PUBLIC OSL_GMP_IS_HERE)
endif (GMP_FOUND)
