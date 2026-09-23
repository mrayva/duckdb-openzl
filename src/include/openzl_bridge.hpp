// Copyright (c) 2026.
//
// This header has no dependency on DuckDB (or any other host database) on
// purpose: it wraps OpenZL's compression graph as plain C++ functions, so
// the exact same translation unit can be reused as-is by a future Postgres
// extension (or anything else) without having to re-implement the
// graph-construction/error-handling logic.
//
// Links OpenZL's C++ library directly (openzl::CCtx / openzl::DCtx, plus the
// "parquet" graph builders from OpenZL's custom_parsers) rather than
// shelling out to the `zli` CLI -- see git history for the earlier subprocess
// based proof of concept.
#pragma once

#include <stdexcept>
#include <string>

namespace openzl_bridge {

// Thrown on any failure (missing input, malformed/non-canonical input,
// OpenZL error, I/O error, etc). The message is meant to be shown directly
// to the end user (e.g. re-thrown as a DuckDB/Postgres error).
struct Error : std::runtime_error {
	using std::runtime_error::runtime_error;
};

// Decompresses an OpenZL archive (.zl) at `input_path` into `output_path`.
// Since the OpenZL "parquet" profile compresses a *canonical* (uncompressed,
// plain-encoded) parquet file, the decompressed bytes ARE a valid, directly
// queryable parquet file -- no further re-encoding step is needed.
// Throws openzl_bridge::Error on failure.
void Decompress(const std::string &input_path, const std::string &output_path);

// OpenZL-compresses a parquet file into `output_zl_path`. Does NOT delete
// `input_parquet_path` -- that is a policy decision left to the caller.
//
// `input_parquet_path` MUST already be in OpenZL's canonical parquet form:
// uncompressed, plain-encoded, no dictionary pages. DuckDB itself can write
// this directly -- no separate canonicalization tool (e.g. Arrow's
// make_canonical_parquet) is needed:
//   COPY tbl TO 'staging.parquet'
//     (FORMAT PARQUET, COMPRESSION 'uncompressed', DICTIONARY_SIZE_LIMIT 0);
// Throws openzl_bridge::Error on failure, including if the input isn't
// already canonical (the OpenZL parquet graph will reject it).
//
// `trained_compressor_path`, if non-empty, points to a serialized compressor
// produced by Train() (see openzl_train_bridge.hpp): the input is compressed
// with that trained graph instead of the generic one. The trained compressor
// must have been trained against data shaped like `input_parquet_path` (same
// columns/types) -- OpenZL will throw if the graph doesn't apply.
//
// `compression_level` (1-9, default 9) is OpenZL's own generic-backend
// compression level; it applies whether or not a trained compressor is used.
//
// Throws openzl_bridge::Error if the input exceeds a safe size (see
// CompressParquet's .cpp for why) -- split the source into smaller chunks.
void CompressParquet(const std::string &input_parquet_path, const std::string &output_zl_path,
                      const std::string &trained_compressor_path = std::string(), int compression_level = 9);

// Same as CompressParquet(), except the trained compressor is passed as
// already-in-memory bytes (e.g. read from a BLOB column in a DuckDB table)
// rather than a file path -- for callers who keep trained compressors
// "inside the database" instead of as standalone files. `compressor_bytes`
// must be non-empty (there's no generic-graph fallback here; call
// CompressParquet() directly for that).
void CompressParquetWithCompressorBytes(const std::string &input_parquet_path, const std::string &output_zl_path,
                                         const std::string &compressor_bytes, int compression_level = 9);

} // namespace openzl_bridge
