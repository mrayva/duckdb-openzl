// Copyright (c) 2026.
//
// This header has no dependency on DuckDB (or any other host database) on
// purpose: it wraps the OpenZL CLI tools (zli, make_canonical_parquet) as
// plain C++ functions, so the exact same translation unit can be reused
// as-is by a future Postgres extension (or anything else) without having to
// re-implement the shell-out/error-handling logic.
//
// Today this shells out to the zli / make_canonical_parquet binaries built
// from https://github.com/facebook/openzl. A later iteration can swap the
// *implementation* of these functions to link OpenZL's C++ library directly
// (openzl::CCtx / openzl::DCtx) without changing any caller.
#pragma once

#include <stdexcept>
#include <string>

namespace openzl_bridge {

// Thrown on any failure (binary not found, non-zero exit, canonicalize
// produced no output, etc). The message is meant to be shown directly to
// the end user (e.g. re-thrown as a DuckDB/Postgres error).
struct Error : std::runtime_error {
	using std::runtime_error::runtime_error;
};

// Paths to the OpenZL CLI tools. Both default to where this project's own
// build places them, and can be overridden via the OPENZL_ZLI_BIN /
// OPENZL_MAKE_CANONICAL_PARQUET_BIN environment variables.
std::string ZliBinPath();
std::string MakeCanonicalParquetBinPath();

// Decompresses an OpenZL archive (.zl) at `input_path` into `output_path`.
// Since the OpenZL "parquet" profile compresses a *canonical* (uncompressed,
// plain-encoded) parquet file, the decompressed bytes ARE a valid, directly
// queryable parquet file -- no further re-encoding step is needed.
// Throws openzl_bridge::Error on failure.
void Decompress(const std::string &input_path, const std::string &output_path);

// Canonicalizes the parquet file at `input_parquet_path` and OpenZL-compresses
// it into `output_zl_path`. Does NOT delete `input_parquet_path` -- that is a
// policy decision left to the caller.
// Throws openzl_bridge::Error on failure.
void CompressParquet(const std::string &input_parquet_path, const std::string &output_zl_path);

} // namespace openzl_bridge
