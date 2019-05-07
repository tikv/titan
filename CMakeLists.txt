cmake_minimum_required(VERSION 3.0)
project(titan)
enable_language(CXX)
enable_language(C)

if (NOT DEFINED ROCKSDB_DIR)
  message(FATAL_ERROR "ROCKSDB_DIR is not defined.")
endif()

include(cmake/rocksdb_flags.cmake)

include_directories(${ROCKSDB_DIR})
include_directories(${ROCKSDB_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/src)

set(SOURCES
        src/blob_file_builder.cc
        src/blob_file_cache.cc
        src/blob_file_iterator.cc
        src/blob_file_reader.cc
        src/blob_file_size_collector.cc
        src/base_db_listener.cc
        src/blob_format.cc
        src/blob_gc.cc
        src/blob_gc_job.cc
        src/blob_gc_picker.cc
        src/db.cc
        src/db_impl.cc
        src/db_impl_files.cc
        src/db_impl_gc.cc
        src/options.cc
        src/table_builder.cc
        src/table_factory.cc
        src/util.cc
        src/version.cc
        src/version_edit.cc
        src/version_set.cc)

add_library(titan STATIC ${SOURCES})

option(WITH_TITAN_TESTS "Build with tests." ON)
option(WITH_TITAN_TOOLS "Build with tools." ON)

if (CMAKE_BUILD_TYPE STREQUAL "Release")
  set(WITH_TITAN_TESTS OFF)
  set(WITH_TITAN_TOOLS OFF)
endif()

if (WITH_TITAN_TESTS OR WITH_TITAN_TOOLS)
  add_subdirectory(${ROCKSDB_DIR} rocksdb EXCLUDE_FROM_ALL)
endif()

if (WITH_TITAN_TESTS)
  include(CTest)
  find_package(Threads REQUIRED)
  find_package(GTest REQUIRED)
  set(TESTS
        blob_file_iterator_test
        blob_file_size_collector_test
        blob_file_test
        blob_format_test
        blob_gc_job_test
        blob_gc_picker_test
        table_builder_test
        titan_db_test
        util_test
        version_test)
  set(TEST_LIBS
        titan
        rocksdb
        testharness
        testutillib
        Threads::Threads
        GTest::GTest)
        
  foreach(test ${TESTS})
      add_executable(titan_${test} src/${test}.cc)
      target_include_directories(titan_${test} PRIVATE ${GTEST_INCLUDE_DIRS})
      target_link_libraries(titan_${test} ${TEST_LIBS})
      add_test(titan_${test} titan_${test})
  endforeach(test ${TESTS})  
endif()

if (WITH_TITAN_TOOLS)
  find_package(gflags REQUIRED)
  add_definitions(-DGFLAGS)

  set(TOOLS_LIBS
        titan
        rocksdb)

  add_executable(titandb_stress tools/titandb_stress.cc)
  target_include_directories(titandb_stress PRIVATE ${gflags_INCLUDE_DIR})
  target_link_libraries(titandb_stress ${TOOLS_LIBS})

  add_executable(titandb_bench tools/db_bench.cc tools/db_bench_tool.cc)
  target_include_directories(titandb_bench PRIVATE ${gflags_INCLUDE_DIR})
  target_link_libraries(titandb_bench ${TOOLS_LIBS})
endif()
