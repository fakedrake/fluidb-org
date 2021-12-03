cmake_minimum_required(VERSION 3.16)

add_custom_target(workload)
foreach(I RANGE 1 30)
  message (STATUS "Adding executable ${I}")
  add_executable("query${I}" "query${I}.cpp")
  target_link_libraries("query${I}" PUBLIC bama)
  add_dependencies(workload "query${I}")
endforeach()
