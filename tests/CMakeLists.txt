if (NOT BUILD_TESTING)
  return()
endif()

include(CTest)

koinos_add_test(
  mempool_tests
    SOURCES
      main.cpp
      mempool_tests.cpp
      block_applicator_tests.cpp)

target_link_libraries(
  mempool_tests
    PRIVATE
      mempool
      Koinos::crypto
      Koinos::exception
      Koinos::log
      Koinos::proto
      Koinos::state_db
      Koinos::util)

target_include_directories(
  mempool_tests
    PUBLIC
    ${PROJECT_SOURCE_DIR}/src # private headers
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>  # <prefix>/include
)

koinos_add_format(TARGET mempool_tests)

koinos_coverage(
  EXECUTABLE
    mempool_tests
  EXCLUDE
    "tests/*")
