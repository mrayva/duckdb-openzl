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

-- Single-call write: canonicalizes (via the catalog's own "parquet" copy
-- function, staged to a sibling temp file) and OpenZL-compresses in one COPY.
COPY tbl TO 'data.zl' (FORMAT OPENZL);

-- Single-call read: decompresses to a sibling .parquet file and reads it.
SELECT * FROM read_openzl('data.zl');
```

Lower-level, two-step equivalents are also available (what `FORMAT OPENZL` and
`read_openzl` are built on top of), useful when you want to control the
intermediate parquet file's location or inspect it directly:

```sql
-- Write already-canonical parquet, then OpenZL-compress it.
-- Does not delete the source.
COPY tbl TO 'staging.parquet'
  (FORMAT PARQUET, COMPRESSION 'uncompressed', DICTIONARY_SIZE_LIMIT 0);
SELECT openzl_compress('staging.parquet', 'data.zl');

-- Decompress an OpenZL archive back into a plain, queryable parquet file.
SELECT openzl_decompress('data.zl', 'data.parquet');
SELECT * FROM read_parquet('data.parquet');
```

All four throw an `IOException` on failure (missing input, input not
canonical, OpenZL error, etc). `read_openzl` and `openzl_decompress` write
their decompressed `.parquet` file to a deterministic sibling path (stripping
a trailing `.zl`, or appending `.parquet`) rather than a fresh temp file per
call: repeat calls reuse/overwrite it instead of accumulating files on disk,
which matters given how often disk space has been this project's actual
constraint. That means concurrent calls against the *same* archive path race
on that sibling file -- fine for the single-user/analytical use this
extension targets, but worth knowing. `COPY ... (FORMAT OPENZL)` cleans up
its own intermediate staging file (`<path>.openzl_staging.parquet`)
unconditionally, including on error.

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
  you try to call something), but only `src/openzl_bridge.cpp` touches them.
  Forcing C++17 on the *whole extension target* (via `CXX_STANDARD`) builds
  fine but fails to **link**: `duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp`
  declares `static constexpr const char *Name`, which is implicitly `inline`
  under C++17 but not C++11 -- compiling `openzl_extension.cpp` (which needs
  that header for the `COPY ... FORMAT OPENZL` support below) at a different
  standard than the rest of DuckDB disagrees on that symbol's linkage,
  producing a "multiple definition" error at final link time. Fixed by
  scoping the flag to only the one file that needs it:
  `set_source_files_properties(src/openzl_bridge.cpp PROPERTIES COMPILE_OPTIONS "-std=c++17")`.
- DuckDB's build adds its own (older) vendored `zstd/include` to the
  directory-scoped include path, inherited by everything added via
  `add_subdirectory` afterward -- including OpenZL's. Left alone, OpenZL's C
  sources resolved `#include "zstd.h"` to DuckDB's copy instead of its own,
  missing symbols DuckDB's older/partial vendored copy doesn't declare
  (`ZSTD_reset_session_and_parameters`, etc). Fixed with
  `include_directories(BEFORE third_party/openzl/deps/zstd/lib)` before
  `add_subdirectory(third_party/openzl ...)`.

## How the ergonomics functions work

- `read_openzl(path)` is a DuckDB table macro (`DefaultTableFunctionGenerator::CreateTableMacroInfo`),
  registered in C++ rather than via a SQL string executed at load time. Its
  body is literally `SELECT * FROM read_parquet(openzl_decompress(path, ...))`
  -- it doesn't need a dedicated table function implementation, just macro
  expansion over the two functions that already existed.
- `COPY ... (FORMAT OPENZL)` is a real `CopyFunction`, but it doesn't
  reimplement a parquet writer: its bind looks up the catalog's own
  registered `"parquet"` `CopyFunctionCatalogEntry`
  (`Catalog::GetEntry<CopyFunctionCatalogEntry>`), copies out its
  `CopyFunction` struct (forcing `compression=uncompressed`,
  `dictionary_size_limit=0` in the `CopyInfo` passed to *its* bind), and then
  forwards every `copy_to_initialize_global/local/sink/combine` call straight
  through to parquet's own function pointers, writing into a staging file
  next to the real destination (`<path>.openzl_staging.parquet`). Only
  `copy_to_finalize` differs: after letting parquet's own finalize close out
  the staging file, it OpenZL-compresses that file into the real destination
  and deletes it. `execution_mode` is left null, which defaults to
  `REGULAR_COPY_TO_FILE` -- the same single-threaded bind/sink/combine/finalize
  sequence every simple copy function supports, regardless of what other
  parallel/batch modes parquet's own copy function also implements.
- The real destination passed to `copy_to_initialize_global` is whatever
  DuckDB's planner decided it should be for this run -- which may itself be a
  `tmp_`-prefixed sibling that DuckDB renames into place after we return
  successfully, when the true target file already exists (`use_tmp_file`).
  Reading that argument at `copy_to_initialize_global` time rather than
  capturing the user's literal path at bind time makes overwriting an
  existing `.zl` archive behave correctly too.

## Deliberately not yet implemented

- **A Postgres extension** reusing `openzl_bridge.{hpp,cpp}` as-is.

## Building

```sh
git submodule update --init --recursive
make set_duckdb_version DUCKDB_GIT_VERSION=v1.5.5   # match your target DuckDB
make
```

Produces `./build/release/extension/openzl/openzl.duckdb_extension`, loadable
via `LOAD '<path>';` in an `-unsigned`-launched DuckDB (it isn't signed).
