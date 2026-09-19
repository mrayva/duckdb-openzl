# duckdb-openzl

A DuckDB extension bridging [OpenZL](https://github.com/facebook/openzl) (Meta's
format-aware compression framework) into SQL, for compressing/decompressing
Parquet files.

This repository is based on https://github.com/duckdb/extension-template.

## Status: proof of concept

The current implementation shells out to the `zli` CLI tool from a local
OpenZL build, rather than linking OpenZL's C++ library directly. This mirrors
how the `wireduck` extension shells out to `tshark` rather than linking
libwireshark.

The compress/decompress logic lives entirely in `src/openzl_bridge.{hpp,cpp}`,
which has **no DuckDB dependency** on purpose: the same translation unit is
meant to be reusable as-is by a future Postgres extension. A later iteration
can swap the bridge's *implementation* to call OpenZL's C++ API directly
(`openzl::CCtx` / `openzl::DCtx`) without changing any caller, since the
callers only see `openzl_bridge::Decompress(...)` / `CompressParquet(...)`.

## Why this shape

OpenZL doesn't slot into Parquet's own compression codec enum (SNAPPY, GZIP,
ZSTD, ...) -- no Parquet reader anywhere would know how to decode an
OpenZL-compressed column chunk inside a real `.parquet` file. Instead,
OpenZL's `parquet` profile takes a *canonicalized* (decoded: uncompressed,
plain-encoded) parquet file's bytes and produces a separate `.zl` archive in
its own container format. That `.zl` file is not a valid parquet file, but
decompressing it (`zli decompress`) yields the canonical bytes back, which
*are* directly valid, queryable parquet -- no re-encoding step needed.

So this extension wraps rather than replaces Parquet: compress a `.parquet`
file down to a much smaller `.zl` archive for storage, decompress it back to
a `.parquet` file whenever you actually need to query it.

**Canonicalization is DuckDB's job, not this extension's.** OpenZL's `parquet`
profile only accepts *canonical* parquet: uncompressed, plain-encoded, no
dictionary pages. Rather than shelling out to Arrow's `make_canonical_parquet`
tool (which drags in Arrow/Parquet/Thrift/Snappy as a build dependency),
DuckDB's own Parquet writer can produce this form directly -- verified
byte-for-byte round-trip correct against real project data:

```sql
COPY tbl TO 'staging.parquet'
  (FORMAT PARQUET, COMPRESSION 'uncompressed', DICTIONARY_SIZE_LIMIT 0);
```

That removes Arrow from this project's dependency graph entirely -- no
canonicalization tool needed at build time or runtime, either in this POC or
in a future native-linked version.

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
on failure (missing input, missing OpenZL binary, non-zero subprocess exit,
input not canonical, etc).

### Configuring the OpenZL binary path

By default the bridge looks for `zli` at `$HOME/openzl/zli`. Override with
the `OPENZL_ZLI_BIN` environment variable if your OpenZL build lives
elsewhere.

## Deliberately not yet implemented

- **Ergonomics**: a single `read_openzl('data.zl')` table function/macro
  instead of the current two-step `read_parquet(openzl_decompress(...))`, and
  a real `COPY tbl TO 'data.zl' (FORMAT OPENZL)` handler.
- **Native linking**: replacing the subprocess calls in `openzl_bridge.cpp`
  with direct calls into OpenZL's C++ library, removing the runtime dependency
  on the `zli` binary being present on `$PATH`/`$HOME`. Investigated the
  dependency shape in OpenZL's own tree (`~/openzl`) and confirmed this is
  lightweight, now that canonicalization is DuckDB's job (see above) rather
  than something this extension needs to link:
  - Building the "parquet" compression graph only needs
    `ZS2_createGraph_genericClustering` (`custom_parsers/shared_components`)
    and `ZL_Parquet_registerGraph_withChunkSize`
    (`custom_parsers/parquet/parquet_graph.h`) -- both plain C APIs whose
    libraries (`shared_components`, `parquet_graph`) depend only on core
    `openzl`, not on the CLI's `tools/arg` argument-parsing machinery
    (`cli/utils/compress_profiles.cpp`, where `zli`'s `--profile parquet`
    flag is wired up, is a separate `utils` library and does NOT need to be
    linked -- it just glues the two calls above to CLI flags).
  - Compression: build an `openzl::Compressor`, call the two functions above
    to register the graph, `selectStartingGraph`, then
    `openzl::CCtx::refCompressor(compressor).compressSerial(bytes)`.
  - Decompression: `openzl::DCtx().decompressSerial(bytes)` -- self-describing,
    no profile/graph setup needed on the way back.
  - Net result: native linking pulls in only `openzl` + `openzl_cpp` +
    `parquet_graph` + `shared_components`, all in-tree OpenZL static libs.
    No Arrow, no `ExternalProject_Add`, no CLI arg-parser.
- **A Postgres extension** reusing `openzl_bridge.{hpp,cpp}` as-is.

## Building

```sh
git submodule update --init --recursive
make set_duckdb_version DUCKDB_GIT_VERSION=v1.5.5   # match your target DuckDB
make
```

Produces `./build/release/extension/openzl/openzl.duckdb_extension`, loadable
via `LOAD '<path>';` in an `-unsigned`-launched DuckDB (it isn't signed).
