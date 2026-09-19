# duckdb-openzl

A DuckDB extension bridging [OpenZL](https://github.com/facebook/openzl) (Meta's
format-aware compression framework) into SQL, for compressing/decompressing
Parquet files.

This repository is based on https://github.com/duckdb/extension-template.

## Status: native-linked

`src/openzl_bridge.{hpp,cpp}` links OpenZL's C++ library directly
(`openzl::CCtx` / `openzl::DCtx` / `openzl::Compressor`, plus the "parquet"
graph builders from OpenZL's `custom_parsers`) -- there is no runtime
dependency on the `zli` CLI binary or any subprocess call. OpenZL is vendored
as a git submodule (`third_party/openzl`) and built as part of this
extension's own CMake configuration.

The bridge has **no DuckDB dependency** on purpose: the same translation unit
is meant to be reusable as-is by a future Postgres extension. Callers only
see `openzl_bridge::Decompress(...)` / `CompressParquet(...)`.

(An earlier revision of this extension shelled out to the `zli` CLI tool as a
proof of concept, mirroring how the `wireduck` extension shells out to
`tshark`. See git history if that's of interest -- it's been fully replaced.)

## Why this shape

OpenZL doesn't slot into Parquet's own compression codec enum (SNAPPY, GZIP,
ZSTD, ...) -- no Parquet reader anywhere would know how to decode an
OpenZL-compressed column chunk inside a real `.parquet` file. Instead,
OpenZL's `parquet` profile takes a *canonicalized* (decoded: uncompressed,
plain-encoded) parquet file's bytes and produces a separate `.zl` archive in
its own container format. That `.zl` file is not a valid parquet file, but
decompressing it yields the canonical bytes back, which *are* directly valid,
queryable parquet -- no re-encoding step needed.

So this extension wraps rather than replaces Parquet: compress a `.parquet`
file down to a much smaller `.zl` archive for storage, decompress it back to
a `.parquet` file whenever you actually need to query it.

**Canonicalization is DuckDB's job, not this extension's.** OpenZL's `parquet`
profile only accepts *canonical* parquet: uncompressed, plain-encoded, no
dictionary pages. Rather than linking Arrow's `make_canonical_parquet` tool
(which drags in Arrow/Parquet/Thrift/Snappy as a dependency), DuckDB's own
Parquet writer produces this form directly -- verified byte-for-byte
round-trip correct against real project data:

```sql
COPY tbl TO 'staging.parquet'
  (FORMAT PARQUET, COMPRESSION 'uncompressed', DICTIONARY_SIZE_LIMIT 0);
```

That keeps Arrow out of this project's dependency graph entirely.

## Functions

```sql
LOAD 'openzl';

-- Decompress an OpenZL archive back into a plain, queryable parquet file.
SELECT openzl_decompress('data.zl', 'data.parquet');
SELECT * FROM read_parquet('data.parquet');

-- Write already-canonical parquet, then OpenZL-compress it.
-- Does not delete the source.
COPY tbl TO 'staging.parquet'
  (FORMAT PARQUET, COMPRESSION 'uncompressed', DICTIONARY_SIZE_LIMIT 0);
SELECT openzl_compress('staging.parquet', 'data.zl');
```

Both functions return their output path on success and throw an `IOException`
on failure (missing input, input not canonical, OpenZL error, etc).

## How native linking works

- Building the "parquet" compression graph only needs
  `ZS2_createGraph_genericClustering` (`custom_parsers/shared_components`)
  and `ZL_Parquet_registerGraph` (`custom_parsers/parquet/parquet_graph.h`)
  -- both plain C APIs whose libraries (`shared_components`, `parquet_graph`)
  depend only on core `openzl`, not on the CLI's `tools/arg`
  argument-parsing machinery (`cli/utils/compress_profiles.cpp`, where
  `zli`'s `--profile parquet` flag is wired up, is a separate `utils`
  library that this extension does not link).
- Compression: build an `openzl::Compressor`, call the two functions above to
  register the graph, `selectStartingGraph`, then create an `openzl::CCtx`,
  set `CParam::FormatVersion` to `ZL_MAX_FORMAT_VERSION` (required when
  driving the C++ API directly -- `zli`'s CLI sets this implicitly), and call
  `compressSerial(bytes)`.
- Decompression: `openzl::DCtx().decompressSerial(bytes)` -- self-describing,
  no profile/graph/format-version setup needed on the way back.
- CMakeLists.txt vendors OpenZL via `add_subdirectory(third_party/openzl)`
  with everything except `OPENZL_BUILD_CPP`/`OPENZL_BUILD_CUSTOM_PARSERS`
  turned off (no CLI, no generic tools, no tests, no parquet-tools/Arrow),
  and links only `openzl` + `openzl_cpp` + `parquet_graph` +
  `shared_components` -- all in-tree OpenZL static libs, no Arrow, no
  `ExternalProject_Add`, no CLI arg-parser.

Two build gotchas worth knowing if you touch this again:
- DuckDB's own top-level `CMakeLists.txt` pins `CMAKE_CXX_STANDARD` to `11`
  as a CACHE variable, which is set *before* this extension's own
  (non-`FORCE`) attempt to set it to `17` -- a plain cache `set()` never
  overwrites an existing entry, so ours was silently ignored. OpenZL's C++
  headers require C++17 (`poly::string_view`/`poly::optional` silently
  degrade to bogus fallback types under C++11 with no compile error until
  you try to call something). Fixed via
  `set_target_properties(... PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)`
  directly on the extension targets, which *does* override the global
  default.
- DuckDB's build adds its own (older) vendored `zstd/include` to the
  directory-scoped include path, inherited by everything added via
  `add_subdirectory` afterward -- including OpenZL's. Left alone, OpenZL's C
  sources resolved `#include "zstd.h"` to DuckDB's copy instead of its own,
  missing symbols DuckDB's older/partial vendored copy doesn't declare
  (`ZSTD_reset_session_and_parameters`, etc). Fixed with
  `include_directories(BEFORE third_party/openzl/deps/zstd/lib)` before
  `add_subdirectory(third_party/openzl ...)`.

## Deliberately not yet implemented

- **Ergonomics**: a single `read_openzl('data.zl')` table function/macro
  instead of the current two-step `read_parquet(openzl_decompress(...))`, and
  a real `COPY tbl TO 'data.zl' (FORMAT OPENZL)` handler.
- **A Postgres extension** reusing `openzl_bridge.{hpp,cpp}` as-is.

## Building

```sh
git submodule update --init --recursive
make set_duckdb_version DUCKDB_GIT_VERSION=v1.5.5   # match your target DuckDB
make
```

Produces `./build/release/extension/openzl/openzl.duckdb_extension`, loadable
via `LOAD '<path>';` in an `-unsigned`-launched DuckDB (it isn't signed).
