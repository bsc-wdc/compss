set(test_executables
  osl_int.c
  osl_pluto_unroll.c
  osl_relation_set_precision.c
  )

set(test_scops_executable osl_test.c)

set(test_scops
  polynom.scop
  test_clay.scop
  test_coordinates.scop
  test_empty_statement.scop
  test_just_access.scop
  test_just_body.scop
  test_just_domain.scop
  test_just_extbody.scop
  test_just_loop.scop
  test_just_pluto_unroll.scop
  test_just_scattering.scop
  test_just_statement_extension.scop
  test_matmult.scop
  test_no_statement.scop
  test_scop_list.scop
  test_symbols2.scop
  test_symbols.scop
  )

foreach(test_source ${test_executables})
  string(REPLACE ".c" "" test_name ${test_source})
  add_executable(${test_name} ${test_source})
  target_link_libraries(${test_name} osl)
  add_test(
    NAME "test_${test_name}"
    COMMAND ${test_name})
endforeach(test_source ${test_executables})

## Scop tests

add_executable(osl_tester ${test_scops_executable})
target_link_libraries(osl_tester osl)

foreach(scop ${test_scops})
  add_test(
    NAME ${scop}
    COMMAND osl_tester "${CMAKE_CURRENT_SOURCE_DIR}/${scop}")
endforeach(scop ${test_scops})
