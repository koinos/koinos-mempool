file(GLOB HEADERS "include/*.hpp")

add_executable(koinos_mempool
               main.cpp
               ${HEADERS})
target_link_libraries(koinos_mempool PUBLIC Koinos::mempool Koinos::exception Koinos::proto Koinos::log Koinos::mq Boost::program_options yaml-cpp)
target_include_directories(koinos_mempool PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
install(TARGETS
   koinos_mempool
   RUNTIME DESTINATION bin
   LIBRARY DESTINATION lib
   ARCHIVE DESTINATION lib
)
