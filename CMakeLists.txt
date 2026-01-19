cmake_minimum_required(VERSION 3.20)
project(DBMS LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_executable(dbms
  src/main.cpp
  src/api_server.cpp
  src/auth.cpp
  src/ddl.cpp
  src/dml.cpp
  src/http_server.cpp
  src/parser.cpp
  src/query.cpp
  src/storage_engine.cpp
  src/storage_engine_txn.cpp
  src/path_utils.cpp

  src/txn/lock_manager.cpp
  src/txn/log_manager.cpp
  src/txn/recovery.cpp
  src/txn/txn_manager.cpp
)

target_include_directories(dbms PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

find_package(Threads REQUIRED)
target_link_libraries(dbms PRIVATE Threads::Threads)

if (WIN32)
  target_link_libraries(dbms PRIVATE ws2_32)
endif()

if (MSVC)
  target_compile_definitions(dbms PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)
endif()

add_custom_command(TARGET dbms POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy_directory
          ${CMAKE_SOURCE_DIR}/docs
          $<TARGET_FILE_DIR:dbms>/docs
)
