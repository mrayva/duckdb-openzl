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

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace openzl_bridge {

// Thrown on any failure (missing input, malformed/non-canonical input,
// OpenZL error, I/O error, etc). The message is meant to be shown directly
// to the end user (e.g. re-thrown as a DuckDB/Postgres error).
struct Error : std::runtime_error {
	using std::runtime_error::runtime_error;
};

// Default cap on the canonical-parquet size handed to ONE OpenZL compress call:
// 500MB, OpenZL's documented limit (https://openzl.org/getting-started/
// library-limitations/: "Compressing payloads with size > 500MB will result in
// undefined behavior. Such inputs must be chunked before compression").
// Larger inputs did round-trip correctly in our tests (up to 7.46GB, after
// patches/openzl-parquet-token-bound.patch fixed an unrelated ~2GB lexer
// crash), but that is outside upstream's supported range, so going past this
// is opt-in via the runtime setting openzl_max_compress_bytes. Memory is the
// other reason for a cap: compression peaks at roughly 4x its input in RAM
// (measured; upstream documents up to ~10x). Every Compress* function takes an
// explicit `max_input_bytes` so callers can override it per call.
constexpr size_t kDefaultMaxCompressBytes = 500000000ULL;
// Default chunk size for COPY ... FORMAT OPENZL: comfortably under the cap even
// after a row group of overshoot; measured free in time and ratio.
constexpr size_t kDefaultChunkBytes = 256000000ULL;

// Which OpenZL graph compresses the parquet bytes, and how the frame is
// written. All fields are optional (defaults reproduce the historical
// behavior). Decompression needs none of this: any frame decodes on its own.
// Sentinel for GraphOptions::parquet_chunk_bytes: "use the default", which is
// kDefaultParquetChunkBytes when the frame format supports internal chunking
// (version >= 21) and no chunking otherwise. See ResolveParquetChunkBytes().
constexpr size_t kAutoParquetChunkBytes = static_cast<size_t>(-1);
// Upstream's `zli --profile parquet` default. Measured on 7 NYSE tables vs no
// chunking: peak RSS -12..-27% (avg ~-19%), size -2.3%..+2.3% (avg ~-0.1%).
constexpr size_t kDefaultParquetChunkBytes = 20000000ULL;

struct GraphOptions {
	// OpenZL frame format version to write: 0 = the newest this build supports
	// (ZL_MAX_FORMAT_VERSION); otherwise 8..that maximum. Pin it to make
	// archives readable by an older OpenZL. Ignored for a trained compressor
	// only in the sense that the trained graph is fixed; the version still
	// applies to the frame.
	int format_version = 0;
	// "parquet" (default): the parquet-aware graph, canonical parquet only.
	// "serial": generic byte compression, accepts any input.
	std::string profile = "parquet";
	// Parquet profile only: split the input into independently compressed
	// chunks of about this many bytes inside the frame. kAutoParquetChunkBytes
	// (the default) = 20MB when the format version allows it (>= 21), else none;
	// 0 = never; any other value is explicit and needs format version >= 21. A
	// trained compressor carries the value it was trained with instead.
	size_t parquet_chunk_bytes = kAutoParquetChunkBytes;
	// OpenZL's permissive compression mode (CParam::PermissiveCompression): when a stage of the graph rejects its input
	// (e.g. a trained graph's bit-unpack step whose exact-size assumption doesn't hold for some chunk), only that stage
	// falls back to generic compression instead of the whole compress call failing. Costs a little efficiency on such
	// streams and changes nothing where strict mode succeeds. -1 (default, "auto") = permissive exactly when a trained
	// compressor is used (matches upstream's `zli`, which runs permissive unless --strict); the generic graph stays
	// strict so that e.g. a non-canonical parquet input is reported, not silently compressed as opaque bytes. 0 =
	// strict, 1 = permissive.
	int permissive = -1;
};

// The chunk size actually used for `g` (resolves the auto sentinel).
size_t ResolveParquetChunkBytes(const GraphOptions &g);

// Throws openzl_bridge::Error if `g` is invalid (unknown profile, format
// version out of range, chunking with a format version that can't express it).
void ValidateGraphOptions(const GraphOptions &g);

// Decompresses an OpenZL archive (.zl) at `input_path` and returns the
// decompressed bytes directly (a valid, directly queryable parquet file's
// bytes -- OpenZL's "parquet" profile only ever compresses already-canonical
// parquet, so no re-encoding step is needed on the way back). Used by
// OpenzlFileSystem (openzl_file_system.hpp) to serve read_openzl() straight
// from memory, with no intermediate .parquet file ever touching disk.
// Throws openzl_bridge::Error on failure.
std::string DecompressToBuffer(const std::string &input_path);

// Decompresses an OpenZL archive (.zl) at `input_path` into `output_path`.
// Since the OpenZL "parquet" profile compresses a *canonical* (uncompressed,
// plain-encoded) parquet file, the decompressed bytes ARE a valid, directly
// queryable parquet file -- no further re-encoding step is needed.
// Throws openzl_bridge::Error on failure.
void Decompress(const std::string &input_path, const std::string &output_path);

// ---- Multi-chunk archives ----
//
// A very large table is compressed as several independent canonical-parquet
// chunks (each a complete parquet file with its own footer, compressed as its
// own OpenZL frame) so peak memory is bounded by the chunk size, not the table
// size. They're stored in ONE file:
//   "OZLCHNK1" | frame 0 | frame 1 | ... | (offset u64, length u64) per chunk
//   | chunk_count u64 | "OZLCHNK1"          (all integers little-endian)
// A single-chunk result is written as a plain OpenZL frame instead (no
// container), so small archives and every pre-chunking archive are unchanged.

struct ChunkRange {
	uint64_t offset;
	uint64_t length;
};

// True and fills `out` if `path` is a multi-chunk container; false if it's a
// plain single OpenZL frame. Throws openzl_bridge::Error on a corrupt container
// or an unreadable file.
bool ReadContainerIndex(const std::string &path, std::vector<ChunkRange> &out);

// Number of independent parquet chunks in the archive: 1 for a plain frame.
size_t ChunkCount(const std::string &path);

// Decompresses chunk `index` (0-based) of a multi-chunk container into memory,
// reading only that chunk's bytes from disk.
std::string DecompressChunkToBuffer(const std::string &path, size_t index);

// Builds a (possibly multi-chunk) archive one compressed chunk at a time.
// The first chunk is held in memory until a second arrives, so a table that
// fits in one chunk still produces a plain OpenZL frame, byte-identical to
// what CompressParquet* write. Finish() writes the trailer (or the lone
// plain frame); Abort() (also run by the destructor if Finish() wasn't
// reached) removes any partial file.
class ChunkedArchiveWriter {
public:
	explicit ChunkedArchiveWriter(std::string path);
	~ChunkedArchiveWriter();
	ChunkedArchiveWriter(const ChunkedArchiveWriter &) = delete;
	ChunkedArchiveWriter &operator=(const ChunkedArchiveWriter &) = delete;

	void Append(const std::string &compressed_frame);
	size_t NumChunks() const {
		return num_chunks_;
	}
	void Finish();
	void Abort();

private:
	std::string path_;
	std::string first_;
	std::ofstream out_;
	std::vector<ChunkRange> ranges_;
	uint64_t pos_ = 0;
	size_t num_chunks_ = 0;
	bool finished_ = false;
};

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
                     const std::string &trained_compressor_path = std::string(), int compression_level = 9,
                     size_t max_input_bytes = kDefaultMaxCompressBytes, const GraphOptions &graph = GraphOptions());

// Same as CompressParquet(), except the trained compressor is passed as
// already-in-memory bytes (e.g. read from a BLOB column in a DuckDB table)
// rather than a file path -- for callers who keep trained compressors
// "inside the database" instead of as standalone files. `compressor_bytes`
// must be non-empty (there's no generic-graph fallback here; call
// CompressParquet() directly for that).
void CompressParquetWithCompressorBytes(const std::string &input_parquet_path, const std::string &output_zl_path,
                                        const std::string &compressor_bytes, int compression_level = 9,
                                        size_t max_input_bytes = kDefaultMaxCompressBytes,
                                        const GraphOptions &graph = GraphOptions());

// Same as CompressParquet(), except the *input* is passed as already-in-memory
// canonical parquet bytes rather than a file path -- for callers who write
// their own staging parquet to an in-memory buffer instead of a temp file on
// disk (see OpenzlBufferFileSystem in openzl_file_system.hpp, used by
// COPY ... FORMAT OPENZL's IN_MEMORY option). `trained_compressor_path`, if
// non-empty, is still a file path (a compressor kept on disk) -- the two
// concerns are independent, and there's currently no call site that needs an
// in-memory *and* trained-as-bytes combination at once.
void CompressParquetBytes(const std::string &canonical_parquet_bytes, const std::string &output_zl_path,
                          const std::string &trained_compressor_path = std::string(), int compression_level = 9,
                          size_t max_input_bytes = kDefaultMaxCompressBytes,
                          const GraphOptions &graph = GraphOptions());

// Same as CompressParquetBytes(), but returns the compressed OpenZL frame
// instead of writing it -- for ChunkedArchiveWriter, which appends frames.
std::string CompressParquetBytesToString(const std::string &canonical_parquet_bytes,
                                         const std::string &trained_compressor_path = std::string(),
                                         int compression_level = 9, size_t max_input_bytes = kDefaultMaxCompressBytes,
                                         const GraphOptions &graph = GraphOptions());

} // namespace openzl_bridge
