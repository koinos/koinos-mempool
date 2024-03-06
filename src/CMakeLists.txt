# libkoinos_mempool

add_library(mempool
  koinos/mempool/mempool.cpp
  koinos/mempool/state.cpp

  koinos/mempool/mempool.hpp
  koinos/mempool/state.hpp)

target_link_libraries(
  mempool
    PUBLIC
      Koinos::exception 
      Koinos::log 
      Koinos::state_db
      Koinos::util 
)
target_include_directories(
  mempool
    PRIVATE
      ${PROJECT_SOURCE_DIR}/src
)

koinos_add_format(TARGET mempool)

# koinos_mempool

add_executable(koinos_mempool main.cpp)
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
      yaml-cpp
)
target_include_directories(
  koinos_mempool
    PRIVATE
      ${PROJECT_SOURCE_DIR}/src
)

koinos_install(TARGETS koinos_mempool)

koinos_add_format(TARGET koinos_mempool)
