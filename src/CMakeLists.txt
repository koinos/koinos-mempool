add_library(mempool
  koinos/mempool/block_applicator.cpp
  koinos/mempool/mempool.cpp
  koinos/mempool/state.cpp

  koinos/mempool/block_applicator.hpp
  koinos/mempool/mempool.hpp
  koinos/mempool/state.hpp)

target_link_libraries(
  mempool
    PUBLIC
      Koinos::exception
      Koinos::log
      Koinos::state_db
      Koinos::util)

target_include_directories(
  mempool
    PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR})

koinos_add_format(TARGET mempool)

add_executable(koinos_mempool koinos_mempool.cpp)
target_link_libraries(
  koinos_mempool
    PUBLIC
      mempool
      Koinos::crypto
      Koinos::exception
      Koinos::git
      Koinos::log
      Koinos::mq
      Koinos::proto
      Boost::exception
      Boost::program_options
      yaml-cpp)

target_include_directories(
  koinos_mempool
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

koinos_add_format(TARGET koinos_mempool)

koinos_install(TARGETS koinos_mempool)
