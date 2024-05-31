cmake_minimum_required(VERSION 3.0)
project(titan)
enable_language(CXX)
enable_language(C)
find_package(Git)

if (NOT ROCKSDB_GIT_REPO)
  set(ROCKSDB_GIT_REPO "https://github.com/v01dstar/rocksdb.git")
endif()

if (NOT ROCKSDB_GIT_BRANCH)
  set(ROCKSDB_GIT_BRANCH "8.10-tikv") # should at least or newer than commit dcf2f8d56092285381be2acbf8f04b8aeeb7ad79
endif()

if (NOT DEFINED ROCKSDB_DIR)
  if (GIT_FOUND)
    if (WIN32)
      execute_process(COMMAND $ENV{COMSPEC} /C ${GIT_EXECUTABLE} clone --branch=${ROCKSDB_GIT_BRANCH} ${ROCKSDB_GIT_REPO})
    else()
      execute_process(COMMAND ${GIT_EXECUTABLE} clone --branch=${ROCKSDB_GIT_BRANCH} ${ROCKSDB_GIT_REPO})
    endif()
    set(ROCKSDB_DIR "${CMAKE_BINARY_DIR}/rocksdb")
  endif()
endif()

if (NOT DEFINED ROCKSDB_DIR)
  message(FATAL_ERROR "ROCKSDB_DIR is not defined.")
endif()

get_filename_component(CMAKE_MODULE_PATH "${ROCKSDB_DIR}/cmake/modules/" ABSOLUTE)
include(cmake/rocksdb_flags.cmake)

include_directories(${ROCKSDB_DIR})
include_directories(${ROCKSDB_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/util)

file(GLOB SOURCES src/*.cc)
file(GLOB TEST_SOURCES src/*test.cc)
list(REMOVE_ITEM SOURCES ${TEST_SOURCES})

add_library(titan STATIC ${SOURCES} $<TARGET_OBJECTS:titan_build_version>)

option(WITH_TITAN_TESTS "Build with tests." ON)
option(WITH_TITAN_TOOLS "Build with tools." ON)
option(TRAVIS "Building in Travis." OFF)
option(CODE_COVERAGE "Generate code coverage report." OFF)

if (CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-array-bounds")
endif()

if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(WITH_TITAN_TESTS OFF)
endif()

if(GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
  if(WIN32)
    execute_process(COMMAND $ENV{COMSPEC} /C ${GIT_EXECUTABLE} rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
  else()
    execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
  endif()
else()
  set(GIT_SHA 0)
endif()
string(REGEX REPLACE "[^0-9a-f]+" "" GIT_SHA "${GIT_SHA}")

set(TITAN_BUILD_VERSION_CC ${CMAKE_BINARY_DIR}/titan_build_version.cc)
configure_file(util/titan_build_version.cc.in ${TITAN_BUILD_VERSION_CC} @ONLY)
add_library(titan_build_version OBJECT ${TITAN_BUILD_VERSION_CC})
target_include_directories(titan_build_version PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/util)

if (TRAVIS)
  add_definitions(-DTRAVIS)
endif()

if (CODE_COVERAGE)
  target_compile_options(titan PRIVATE "--coverage")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
endif()

if(WITH_ASAN OR WITH_TSAN)
  find_package(Threads)
  if(CMAKE_USE_PTHREADS_INIT)
    link_libraries(pthread)
  endif()
endif()

if (WITH_TITAN_TESTS OR WITH_TITAN_TOOLS)
  add_subdirectory(${ROCKSDB_DIR} rocksdb EXCLUDE_FROM_ALL)
  # Check if -latomic is required or not
  if (NOT MSVC)
    set(CMAKE_REQUIRED_FLAGS "--std=c++17")
    CHECK_CXX_SOURCE_COMPILES("
#include <atomic>
std::atomic<uint64_t> x(0);
int main() {
  uint64_t i = x.load(std::memory_order_relaxed);
  bool b = x.is_lock_free();
  return 0;
}
" BUILTIN_ATOMIC)
    if (NOT BUILTIN_ATOMIC)
      #TODO: Check if -latomic exists
      list(APPEND THIRDPARTY_LIBS atomic)
    endif()
  endif()
endif()

# Check if -latomic is required or not
if (NOT MSVC)
  set(CMAKE_REQUIRED_FLAGS "--std=c++17")
  CHECK_CXX_SOURCE_COMPILES("
#include <atomic>
std::atomic<uint64_t> x(0);
int main() {
  uint64_t i = x.load(std::memory_order_relaxed);
  bool b = x.is_lock_free();
  return 0;
}
" BUILTIN_ATOMIC)
  if (NOT BUILTIN_ATOMIC)
    #TODO: Check if -latomic exists
    list(APPEND THIRDPARTY_LIBS atomic)
  endif()
endif()

if (WITH_TITAN_TESTS AND (CMAKE_BUILD_TYPE STREQUAL "Debug"))
  include(CTest)
  include_directories(SYSTEM ${ROCKSDB_DIR}/third-party/gtest-1.8.1/fused-src)

  set(TEST_LIBS
        titan
        testutillib
        testharness
        gtest)

  set(TESTS
        blob_file_iterator_test
        blob_file_size_collector_test
        blob_file_test
        blob_format_test
        blob_gc_job_test
        blob_gc_picker_test
        gc_stats_test
        table_builder_test
        thread_safety_test
        titan_db_test
        titan_checkpoint_test
        titan_options_test
        util_test
        compaction_filter_test
        version_test)

  foreach(test ${TESTS})
    add_executable(titan_${test} src/${test}.cc)
    target_link_libraries(titan_${test} ${TEST_LIBS})
    add_test(titan_${test} titan_${test})
  endforeach(test ${TESTS})
endif()

if (WITH_TITAN_TOOLS)
  set(TOOLS_LIBS titan rocksdb)

  if (NOT TRAVIS)
    find_package(gflags REQUIRED)
  else()
    # Hack: On Travis (with Ubuntu xenial or before), libgflags-dev package doesn't come with
    # gflags-config.cmake, so find_package will fail. Hard-code gflag path for now.
    if (NOT APPLE)
      set(gflags_INCLUDE_DIR "/usr/include/gflags")
      list(APPEND TOOLS_LIBS "/usr/lib/x86_64-linux-gnu/libgflags.a")
    endif()
  endif()
  add_definitions(-DGFLAGS)

  add_executable(titandb_stress tools/titandb_stress.cc)
  target_include_directories(titandb_stress PRIVATE ${GFLAGS_INCLUDE_DIR})
  target_link_libraries(titandb_stress ${TOOLS_LIBS})

  add_executable(titandb_bench tools/db_bench.cc tools/db_bench_tool.cc)
  target_include_directories(titandb_bench PRIVATE ${GFLAGS_INCLUDE_DIR})
  target_link_libraries(titandb_bench ${TOOLS_LIBS})

  add_executable(titan_manifest_dump tools/manifest_dump.cc)
  target_include_directories(titan_manifest_dump PRIVATE ${GFLAGS_INCLUDE_DIR})
  target_link_libraries(titan_manifest_dump ${TOOLS_LIBS})
  
  add_executable(titan_blob_file_dump tools/blob_file_dump.cc)
  target_include_directories(titan_blob_file_dump PRIVATE ${GFLAGS_INCLUDE_DIR})
  target_link_libraries(titan_blob_file_dump ${TOOLS_LIBS})
endif()

# Installation - copy lib/ and include/

include(GNUInstallDirs)
install(DIRECTORY include/titan
  COMPONENT devel
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
install(TARGETS titan
  COMPONENT devel
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
