cmake_minimum_required(VERSION 3.20.2)

# Set extension name here
set(TARGET_NAME openzl)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

# ---- Vendor OpenZL (native link, no zli/make_canonical_parquet subprocess) ----
# Only the pieces actually needed to build+run the "parquet" compression graph
# and the serial (de)compression contexts: core openzl, the C++ bindings, and
# the two custom_parsers libraries the parquet profile calls into. Explicitly
# OFF everything else (CLI, generic tools, tests, examples, parquet-tools/Arrow)
# so this stays a lean, dependency-free build -- no Arrow anywhere.
set(OPENZL_BUILD_CPP ON CACHE BOOL "" FORCE)
set(OPENZL_BUILD_CUSTOM_PARSERS ON CACHE BOOL "" FORCE)
set(OPENZL_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(OPENZL_BUILD_CLI OFF CACHE BOOL "" FORCE)
set(OPENZL_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(OPENZL_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(OPENZL_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(OPENZL_BUILD_PARQUET_TOOLS OFF CACHE BOOL "" FORCE)
set(OPENZL_BUILD_PYTHON_EXT OFF CACHE BOOL "" FORCE)
set(OPENZL_BUILD_PYTHON_DEMO OFF CACHE BOOL "" FORCE)
set(OPENZL_INSTALL OFF CACHE BOOL "" FORCE)

# DuckDB's own build adds its (older/partial) vendored zstd/include to the
# directory-scoped include path, which is inherited by every add_subdirectory
# below it -- including OpenZL's. Left alone, OpenZL's C sources resolve
# `#include "zstd.h"` to DuckDB's copy instead of the one it actually vendors
# and needs (newer zstd APIs like ZSTD_reset_session_and_parameters). Prepend
# OpenZL's own zstd headers so they win.
include_directories(BEFORE third_party/openzl/deps/zstd/lib)

# Local patch to the vendored OpenZL (we can't push to facebook/openzl, and a
# commit inside the submodule wouldn't exist on any remote a fresh clone could
# fetch): fixes the parquet lexer's token-scratch overallocation that crashed
# any multi-GB input -- see patches/openzl-parquet-token-bound.patch and the
# README's "Known limitations". Applied idempotently at configure time:
# `git apply --check --reverse` succeeds only if it's already applied.
set(OPENZL_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/openzl)
set(OPENZL_PARQUET_PATCH ${CMAKE_CURRENT_SOURCE_DIR}/patches/openzl-parquet-token-bound.patch)
execute_process(
  COMMAND git apply --check --reverse ${OPENZL_PARQUET_PATCH}
  WORKING_DIRECTORY ${OPENZL_SRC_DIR}
  RESULT_VARIABLE OPENZL_PATCH_ALREADY_APPLIED
  OUTPUT_QUIET ERROR_QUIET)
if(NOT OPENZL_PATCH_ALREADY_APPLIED EQUAL 0)
  execute_process(
    COMMAND git apply ${OPENZL_PARQUET_PATCH}
    WORKING_DIRECTORY ${OPENZL_SRC_DIR}
    RESULT_VARIABLE OPENZL_PATCH_RESULT
    ERROR_VARIABLE OPENZL_PATCH_ERROR)
  if(NOT OPENZL_PATCH_RESULT EQUAL 0)
    message(FATAL_ERROR "Failed to apply ${OPENZL_PARQUET_PATCH} to the vendored OpenZL "
                        "(is third_party/openzl at the pinned commit?): ${OPENZL_PATCH_ERROR}")
  endif()
endif()

add_subdirectory(third_party/openzl EXCLUDE_FROM_ALL)

# ---- Training support (openzl::training::train()) ----
# OpenZL's own tools/training/ subsystem (clustering/ACE/dict trainers) is
# only built when OPENZL_BUILD_TOOLS or OPENZL_BUILD_CLI is ON -- both
# deliberately OFF above, since either would also drag in argument-parsing
# tools, SDDL tools, and (worse) the ml_selector's xgboost dependency, none
# of which the actual training entry point needs. Those flags are plain
# `set()` calls inside OpenZL's tools/CMakeLists.txt (not CACHE variables),
# so they can't be overridden from our scope once that file runs -- instead
# of fighting that, compile the handful of source directories training
# actually needs (training/, plus its small io/logger deps) ourselves,
# bypassing OpenZL's own tools/ subdirectory gating entirely.
file(GLOB_RECURSE OPENZL_TRAINING_SOURCES CONFIGURE_DEPENDS
  "third_party/openzl/tools/training/*.cpp")
list(FILTER OPENZL_TRAINING_SOURCES EXCLUDE REGEX "/tests/")
file(GLOB OPENZL_IO_SOURCES CONFIGURE_DEPENDS "third_party/openzl/tools/io/*.cpp")
file(GLOB OPENZL_LOGGER_SOURCES CONFIGURE_DEPENDS "third_party/openzl/tools/logger/*.cpp")

add_library(openzl_training STATIC
  ${OPENZL_TRAINING_SOURCES}
  ${OPENZL_IO_SOURCES}
  ${OPENZL_LOGGER_SOURCES})
target_include_directories(openzl_training PRIVATE
  third_party/openzl
  third_party/openzl/include
  third_party/openzl/cpp/include)
# zstd's advanced dictionary-training API (ZDICT_*) used by the dict trainer
# is only declared under this macro -- it's outside zstd's stable ABI.
target_compile_definitions(openzl_training PUBLIC ZDICT_STATIC_LINKING_ONLY)
set_target_properties(openzl_training PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_source_files_properties(
  ${OPENZL_TRAINING_SOURCES} ${OPENZL_IO_SOURCES} ${OPENZL_LOGGER_SOURCES}
  PROPERTIES COMPILE_OPTIONS "-std=c++17")
target_link_libraries(openzl_training PUBLIC openzl openzl_cpp custom_parsers)
add_dependencies(openzl_training openzl openzl_cpp custom_parsers)

set(EXTENSION_SOURCES src/openzl_extension.cpp src/openzl_bridge.cpp src/openzl_train_bridge.cpp src/openzl_file_system.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# DuckDB's own top-level CMakeLists.txt sets CMAKE_CXX_STANDARD 11 as a CACHE
# variable (our own attempt to set it to 17 above is a no-op: a non-FORCE
# cache set() never overwrites an existing cache entry, and DuckDB's own
# top-level project is configured first). OpenZL's C++ headers require C++17
# (poly::string_view/poly::optional silently degrade to bogus fallback types
# otherwise) -- but ONLY src/openzl_bridge.cpp actually touches those headers.
# Forcing C++17 on the whole extension target (via CXX_STANDARD) instead
# broke the link: DuckDB's own sources see CopyFunctionCatalogEntry::Name (a
# static constexpr member, implicitly `inline` under C++17 but not C++11) at
# C++11, and a C++17 TU pulling in the same header via
# copy_function_catalog_entry.hpp disagrees on its linkage, causing a
# "multiple definition" link error. Scoping the flag to the one file that
# needs it avoids the whole class of mismatch.
set_source_files_properties(src/openzl_bridge.cpp src/openzl_train_bridge.cpp PROPERTIES COMPILE_OPTIONS "-std=c++17")

foreach(TGT ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
  target_include_directories(${TGT} PRIVATE
    third_party/openzl/include
    third_party/openzl/cpp/include
    third_party/openzl)
  target_link_libraries(${TGT} openzl openzl_cpp parquet_graph shared_components openzl_training)
endforeach()

# No install(EXPORT ...) here: exporting the static extension target would
# require CMake to also export every transitive link dependency, including
# OpenZL's own internal vendored deps (openzl_deps, zstd, xxhash, ...) --
# a chain we don't control and don't need, since this extension is only ever
# consumed as the loadable openzl.duckdb_extension file produced below, not
# as part of DuckDB's bundled-static-extensions distribution mechanism.
install(
  TARGETS ${EXTENSION_NAME}
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
