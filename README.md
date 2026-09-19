# duckdb-openzl

A DuckDB extension bridging [OpenZL](https://github.com/facebook/openzl) (Meta's
format-aware compression framework) into SQL, for compressing/decompressing
Parquet files.

This repository is based on https://github.com/duckdb/extension-template.

## Status: proof of concept

The current implementation shells out to the `zli` and `make_canonical_parquet`
CLI tools from a local OpenZL build, rather than linking OpenZL's C++ library
directly. This mirrors how the `wireduck` extension shells out to `tshark`
rather than linking libwireshark.

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

## Functions

```sql
LOAD 'openzl';

-- Decompress an OpenZL archive back into a plain, queryable parquet file.
SELECT openzl_decompress('data.zl', 'data.parquet');
SELECT * FROM read_parquet('data.parquet');

-- Canonicalize + OpenZL-compress a parquet file. Does not delete the source.
COPY tbl TO 'staging.parquet' (FORMAT PARQUET);
SELECT openzl_compress('staging.parquet', 'data.zl');
```

Both functions return their output path on success and throw an `IOException`
on failure (missing input, missing OpenZL binaries, non-zero subprocess exit,
etc).

### Configuring the OpenZL binary paths

By default the bridge looks for the tools at:
- `$HOME/openzl/zli`
- `$HOME/openzl/build_parquet/tools/parquet/make_canonical_parquet`

Override with the `OPENZL_ZLI_BIN` / `OPENZL_MAKE_CANONICAL_PARQUET_BIN`
environment variables if your OpenZL build lives elsewhere.

## Deliberately not yet implemented

- **Ergonomics**: a single `read_openzl('data.zl')` table function/macro
  instead of the current two-step `read_parquet(openzl_decompress(...))`, and
  a real `COPY tbl TO 'data.zl' (FORMAT OPENZL)` handler.
- **Native linking**: replacing the subprocess calls in `openzl_bridge.cpp`
  with direct calls into OpenZL's C++ library (`openzl::CCtx::compressSerial`,
  `openzl::DCtx::decompressSerial`), removing the runtime dependency on the
  `zli`/`make_canonical_parquet` binaries being present on `$PATH`/`$HOME`.
  This requires resolving how `zli`'s CLI internally builds the default
  `parquet` profile's `Compressor` object, since that logic currently lives in
  the CLI's own profile registry rather than a single library entry point.
- **A Postgres extension** reusing `openzl_bridge.{hpp,cpp}` as-is.

## Building

```sh
git submodule update --init --recursive
make set_duckdb_version DUCKDB_GIT_VERSION=v1.5.5   # match your target DuckDB
make
```

Produces `./build/release/extension/openzl/openzl.duckdb_extension`, loadable
via `LOAD '<path>';` in an `-unsigned`-launched DuckDB (it isn't signed).
