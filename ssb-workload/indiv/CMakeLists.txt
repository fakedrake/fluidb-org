cmake_minimum_required(VERSION 3.16)

add_custom_target(individual)
foreach(I RANGE 1 12)
  message (STATUS "Adding executable baseline ${I}")
  add_executable("indiv_query${I}" "query${I}.cpp")
  target_link_libraries("indiv_query${I}" PUBLIC bama)
  add_dependencies(individual "indiv_query${I}")
endforeach()
