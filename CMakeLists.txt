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
add_subdirectory(third_party/openzl EXCLUDE_FROM_ALL)

set(EXTENSION_SOURCES src/openzl_extension.cpp src/openzl_bridge.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

foreach(TGT ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
  target_include_directories(${TGT} PRIVATE
    third_party/openzl/include
    third_party/openzl/cpp/include
    third_party/openzl)
  target_link_libraries(${TGT} openzl openzl_cpp parquet_graph shared_components)
  # DuckDB's own top-level CMakeLists.txt sets CMAKE_CXX_STANDARD 11 as a
  # CACHE variable (our own attempt to set it to 17 above is a no-op: a
  # non-FORCE cache set() never overwrites an existing cache entry, and
  # DuckDB's own top-level project is configured first). OpenZL's C++
  # headers require C++17 (poly::string_view/poly::optional silently
  # degrade to bogus fallback types otherwise), so force it as a target
  # property, which does take precedence over the global default.
  set_target_properties(${TGT} PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
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
