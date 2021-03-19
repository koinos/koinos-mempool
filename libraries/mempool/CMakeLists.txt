file(GLOB HEADERS "include/koinos/mempool/*.hpp")
add_library(koinos_mempool_lib
            mempool.cpp
            ${HEADERS})
target_link_libraries(koinos_mempool_lib Koinos::exception Koinos::log Koinos::util)
target_include_directories(koinos_mempool_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
add_library(Koinos::mempool ALIAS koinos_mempool_lib)
