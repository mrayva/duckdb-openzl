#include "openzl_bridge.hpp"

#include <cstdio>
#include <limits>
#include <cstring>
#include <memory>

#include "openzl/cpp/CCtx.hpp"
#include "openzl/cpp/CParam.hpp"
#include "openzl/cpp/Compressor.hpp"
#include "openzl/cpp/DCtx.hpp"
#include "openzl/cpp/Exception.hpp"
#include "openzl/zl_version.h"

#include "custom_parsers/dependency_registration.h"

#include "openzl_internal_io.hpp"

namespace openzl_bridge {

using internal::BuildParquetCompressor;
using internal::BuildSerialCompressor;
using internal::FileExists;
using internal::ReadFile;
using internal::WriteFile;

// ---- multi-chunk container ----

namespace {

constexpr char kContainerMagic[8] = {'O', 'Z', 'L', 'C', 'H', 'N', 'K', '1'};
constexpr uint64_t kMagicLen = 8;

void PutLE64(std::string &s, uint64_t v) {
	for (int i = 0; i < 8; i++) {
		s.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
	}
}

uint64_t GetLE64(const char *p) {
	uint64_t v = 0;
	for (int i = 0; i < 8; i++) {
		v |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
	}
	return v;
}

} // namespace

bool ReadContainerIndex(const std::string &path, std::vector<ChunkRange> &out) {
	out.clear();
	std::ifstream f(path, std::ios::binary | std::ios::ate);
	if (!f) {
		throw Error("openzl_bridge: could not open archive: " + path);
	}
	const uint64_t size = static_cast<uint64_t>(f.tellg());
	if (size < kMagicLen + 16) {
		return false;
	}
	char head[kMagicLen];
	f.seekg(0);
	f.read(head, kMagicLen);
	if (!f || std::memcmp(head, kContainerMagic, kMagicLen) != 0) {
		return false;
	}
	char tail[16];
	f.seekg(static_cast<std::streamoff>(size - 16));
	f.read(tail, 16);
	if (!f || std::memcmp(tail + 8, kContainerMagic, kMagicLen) != 0) {
		throw Error("openzl_bridge: corrupt chunked archive (missing trailer): " + path);
	}
	const uint64_t count = GetLE64(tail);
	if (count == 0 || count > (size - kMagicLen - 16) / 16) {
		throw Error("openzl_bridge: corrupt chunked archive (bad chunk count): " + path);
	}
	const uint64_t index_bytes = count * 16;
	const uint64_t index_start = size - 16 - index_bytes;
	std::string index(index_bytes, '\0');
	f.seekg(static_cast<std::streamoff>(index_start));
	f.read(&index[0], static_cast<std::streamsize>(index_bytes));
	if (!f) {
		throw Error("openzl_bridge: corrupt chunked archive (unreadable index): " + path);
	}
	for (uint64_t i = 0; i < count; i++) {
		ChunkRange r {GetLE64(index.data() + 16 * i), GetLE64(index.data() + 16 * i + 8)};
		if (r.offset < kMagicLen || r.offset > index_start || r.length > index_start - r.offset) {
			throw Error("openzl_bridge: corrupt chunked archive (chunk out of range): " + path);
		}
		out.push_back(r);
	}
	return true;
}

size_t ChunkCount(const std::string &path) {
	if (!FileExists(path)) {
		throw Error("openzl_bridge: input file not found: " + path);
	}
	std::vector<ChunkRange> ranges;
	return ReadContainerIndex(path, ranges) ? ranges.size() : 1;
}

std::string DecompressChunkToBuffer(const std::string &path, size_t index) {
	if (!FileExists(path)) {
		throw Error("openzl_bridge: input file not found: " + path);
	}
	std::vector<ChunkRange> ranges;
	if (!ReadContainerIndex(path, ranges)) {
		throw Error("openzl_bridge: not a chunked archive: " + path);
	}
	if (index >= ranges.size()) {
		throw Error("openzl_bridge: chunk " + std::to_string(index) + " out of range (archive has " +
		            std::to_string(ranges.size()) + ") : " + path);
	}
	std::string frame(ranges[index].length, '\0');
	{
		std::ifstream f(path, std::ios::binary);
		f.seekg(static_cast<std::streamoff>(ranges[index].offset));
		f.read(&frame[0], static_cast<std::streamsize>(frame.size()));
		if (!f) {
			throw Error("openzl_bridge: error reading chunk " + std::to_string(index) + " of " + path);
		}
	}
	try {
		openzl::DCtx dctx;
		return dctx.decompressSerial(frame);
	} catch (const std::exception &e) {
		throw Error("openzl_bridge: decompress failed for chunk " + std::to_string(index) + " of " + path + ": " +
		            e.what());
	}
}

ChunkedArchiveWriter::ChunkedArchiveWriter(std::string path) : path_(std::move(path)) {
}

ChunkedArchiveWriter::~ChunkedArchiveWriter() {
	if (!finished_) {
		Abort();
	}
}

void ChunkedArchiveWriter::Append(const std::string &compressed_frame) {
	num_chunks_++;
	if (num_chunks_ == 1) {
		first_ = compressed_frame;
		return;
	}
	if (num_chunks_ == 2) {
		out_.open(path_, std::ios::binary | std::ios::trunc);
		if (!out_) {
			throw Error("openzl_bridge: could not open file for writing: " + path_);
		}
		out_.write(kContainerMagic, kMagicLen);
		pos_ = kMagicLen;
		ranges_.push_back({pos_, first_.size()});
		out_.write(first_.data(), static_cast<std::streamsize>(first_.size()));
		pos_ += first_.size();
		first_.clear();
		first_.shrink_to_fit();
	}
	ranges_.push_back({pos_, compressed_frame.size()});
	out_.write(compressed_frame.data(), static_cast<std::streamsize>(compressed_frame.size()));
	pos_ += compressed_frame.size();
	if (!out_) {
		throw Error("openzl_bridge: error writing file: " + path_);
	}
}

void ChunkedArchiveWriter::Finish() {
	if (num_chunks_ == 0) {
		throw Error("openzl_bridge: cannot finish an archive with no chunks: " + path_);
	}
	if (num_chunks_ == 1) {
		WriteFile(path_, first_);
	} else {
		std::string trailer;
		for (const auto &r : ranges_) {
			PutLE64(trailer, r.offset);
			PutLE64(trailer, r.length);
		}
		PutLE64(trailer, ranges_.size());
		trailer.append(kContainerMagic, kMagicLen);
		out_.write(trailer.data(), static_cast<std::streamsize>(trailer.size()));
		out_.close();
		if (!out_) {
			throw Error("openzl_bridge: error writing file: " + path_);
		}
	}
	finished_ = true;
}

void ChunkedArchiveWriter::Abort() {
	finished_ = true;
	if (out_.is_open()) {
		out_.close();
		std::remove(path_.c_str());
	}
}

// ---- decompress ----

std::string DecompressToBuffer(const std::string &input_path) {
	if (!FileExists(input_path)) {
		throw Error("openzl_bridge: input file not found: " + input_path);
	}
	std::vector<ChunkRange> ranges;
	if (ReadContainerIndex(input_path, ranges)) {
		throw Error("openzl_bridge: " + input_path + " is a chunked archive (" + std::to_string(ranges.size()) +
		            " independent parquet chunks), which can't be a single parquet file. Read it with "
		            "read_openzl(), or write it out with COPY (SELECT * FROM read_openzl('...')) TO "
		            "'out.parquet'.");
	}
	try {
		std::string input = ReadFile(input_path);
		openzl::DCtx dctx;
		return dctx.decompressSerial(input);
	} catch (const Error &) {
		throw;
	} catch (const std::exception &e) {
		throw Error(std::string("openzl_bridge: decompress failed for ") + input_path + ": " + e.what());
	}
}

void Decompress(const std::string &input_path, const std::string &output_path) {
	WriteFile(output_path, DecompressToBuffer(input_path));
}

void ValidateGraphOptions(const GraphOptions &g) {
	if (g.profile != "parquet" && g.profile != "serial") {
		throw Error("openzl_bridge: unknown profile '" + g.profile + "' (expected 'parquet' or 'serial')");
	}
	if (g.format_version != 0 && (g.format_version < ZL_MIN_FORMAT_VERSION || g.format_version > ZL_MAX_FORMAT_VERSION)) {
		throw Error("openzl_bridge: format version " + std::to_string(g.format_version) + " is outside the supported range " +
		            std::to_string(ZL_MIN_FORMAT_VERSION) + ".." + std::to_string(ZL_MAX_FORMAT_VERSION) + " (0 = newest)");
	}
	const int effective = g.format_version == 0 ? ZL_MAX_FORMAT_VERSION : g.format_version;
	if (g.parquet_chunk_bytes > 0 && g.profile == "parquet" && effective < ZL_CHUNK_VERSION_MIN) {
		throw Error("openzl_bridge: parquet chunking needs format version >= " + std::to_string(ZL_CHUNK_VERSION_MIN) +
		            " (got " + std::to_string(effective) + "); raise the format version or set the chunk size to 0");
	}
	if (g.parquet_chunk_bytes > static_cast<size_t>(std::numeric_limits<int>::max())) {
		throw Error("openzl_bridge: parquet chunk size is too large (max " +
		            std::to_string(std::numeric_limits<int>::max()) + " bytes)");
	}
}

// ---- compress ----

namespace {

// Shared by every public Compress* entry point below: `input` is already the
// full canonical-parquet bytes in memory (a public wrapper either read it
// from a file or was handed it directly), and `compressor_bytes` empty means
// "use the generic graph", non-empty means "deserialize this trained
// compressor instead". `error_label` names the input in error messages (a
// path when there is one, else something like "<in-memory input>"). Returns
// the compressed OpenZL frame; callers decide where it goes.
[[noreturn]] void ThrowTooLarge(size_t size, size_t max_input_bytes) {
	throw Error("openzl_bridge: input parquet too large for one compress call (" + std::to_string(size) +
	            " bytes, limit " + std::to_string(max_input_bytes) +
	            "): compression needs roughly 4x its input in RAM. COPY ... (FORMAT OPENZL) splits large "
	            "tables into chunks automatically (see CHUNK_SIZE_BYTES / openzl_chunk_size_bytes); to "
	            "compress this one file whole, raise the limit with SET openzl_max_compress_bytes (note: OpenZL documents "
	            "payloads over 500MB as undefined behavior).");
}

std::string CompressBytesImpl(const std::string &input, const std::string &error_label,
                               const std::string &compressor_bytes, int compression_level, size_t max_input_bytes,
                               const GraphOptions &graph) {
	ValidateGraphOptions(graph);
	if (!compressor_bytes.empty() && graph.profile != "parquet") {
		throw Error("openzl_bridge: a trained compressor is built on the parquet profile; it can't be combined with "
		            "profile '" + graph.profile + "'");
	}
	if (input.size() > max_input_bytes) {
		ThrowTooLarge(input.size(), max_input_bytes);
	}
	try {
		// A trained compressor's serialized graph already encodes its own
		// column layout/clustering decisions; createCompressorFromSerialized
		// re-registers whatever custom dependencies it references (e.g. the
		// "Parquet Parser" graph) automatically, the same way it's built fresh
		// for the generic path below.
		std::unique_ptr<openzl::Compressor> trained_compressor;
		openzl::Compressor generic_compressor;
		openzl::Compressor *compressor;
		if (!compressor_bytes.empty()) {
			trained_compressor = openzl::custom_parsers::createCompressorFromSerialized(compressor_bytes, "");
			compressor = trained_compressor.get();
		} else {
			generic_compressor = graph.profile == "serial" ? BuildSerialCompressor()
			                                              : BuildParquetCompressor(graph.parquet_chunk_bytes);
			compressor = &generic_compressor;
		}

		openzl::CCtx cctx;
		// zli's CLI sets this implicitly to the newest format the linked
		// OpenZL build supports; we must set it explicitly when driving the
		// C++ API directly, or compression fails with "Format version is not
		// set" (ZL_CParam_formatVersion, gcparams.c).
		cctx.setParameter(openzl::CParam::FormatVersion,
		                  graph.format_version == 0 ? ZL_MAX_FORMAT_VERSION : graph.format_version);
		cctx.setParameter(openzl::CParam::CompressionLevel, compression_level);
		cctx.refCompressor(*compressor);
		return cctx.compressSerial(input);
	} catch (const Error &) {
		throw;
	} catch (const std::exception &e) {
		if (std::string(e.what()).find("incompatible with requested format version") != std::string::npos) {
			throw Error("openzl_bridge: format version " +
			            std::to_string(graph.format_version == 0 ? ZL_MAX_FORMAT_VERSION : graph.format_version) +
			            " is too old for this data with the '" + graph.profile + "' profile: the graph picked a codec that "
			            "needs a newer format (which codecs it needs depends on the data; e.g. the serial profile "
			            "needs roughly 24+). Raise openzl_format_version / FORMAT_VERSION.");
		}
		throw Error(std::string("openzl_bridge: compress failed for ") + error_label +
		            " (is it canonical parquet? uncompressed, plain-encoded, no dictionary): " + e.what());
	}
}

std::string LoadCompressorFile(const std::string &trained_compressor_path) {
	if (trained_compressor_path.empty()) {
		return std::string();
	}
	if (!FileExists(trained_compressor_path)) {
		throw Error("openzl_bridge: trained compressor file not found: " + trained_compressor_path);
	}
	return ReadFile(trained_compressor_path);
}

} // namespace

void CompressParquet(const std::string &input_parquet_path, const std::string &output_zl_path,
                      const std::string &trained_compressor_path, int compression_level, size_t max_input_bytes,
                      const GraphOptions &graph) {
	if (!FileExists(input_parquet_path)) {
		throw Error("openzl_bridge: input parquet file not found: " + input_parquet_path);
	}
	std::string compressor_bytes = LoadCompressorFile(trained_compressor_path);
	// Check the size before reading the file, so a too-large input is rejected
	// without first allocating a buffer the size of the input.
	if (internal::FileSize(input_parquet_path) > max_input_bytes) {
		ThrowTooLarge(internal::FileSize(input_parquet_path), max_input_bytes);
	}
	WriteFile(output_zl_path, CompressBytesImpl(ReadFile(input_parquet_path), input_parquet_path, compressor_bytes,
	                                              compression_level, max_input_bytes, graph));
}

void CompressParquetWithCompressorBytes(const std::string &input_parquet_path, const std::string &output_zl_path,
                                         const std::string &compressor_bytes, int compression_level,
                                         size_t max_input_bytes, const GraphOptions &graph) {
	if (!FileExists(input_parquet_path)) {
		throw Error("openzl_bridge: input parquet file not found: " + input_parquet_path);
	}
	if (compressor_bytes.empty()) {
		throw Error("openzl_bridge: compressor_bytes must be non-empty (use CompressParquet() for the generic graph)");
	}
	if (internal::FileSize(input_parquet_path) > max_input_bytes) {
		ThrowTooLarge(internal::FileSize(input_parquet_path), max_input_bytes);
	}
	WriteFile(output_zl_path, CompressBytesImpl(ReadFile(input_parquet_path), input_parquet_path, compressor_bytes,
	                                              compression_level, max_input_bytes, graph));
}

void CompressParquetBytes(const std::string &canonical_parquet_bytes, const std::string &output_zl_path,
                           const std::string &trained_compressor_path, int compression_level,
                           size_t max_input_bytes, const GraphOptions &graph) {
	WriteFile(output_zl_path, CompressParquetBytesToString(canonical_parquet_bytes, trained_compressor_path,
	                                                        compression_level, max_input_bytes, graph));
}

std::string CompressParquetBytesToString(const std::string &canonical_parquet_bytes,
                                          const std::string &trained_compressor_path, int compression_level,
                                          size_t max_input_bytes, const GraphOptions &graph) {
	return CompressBytesImpl(canonical_parquet_bytes, "<in-memory input>", LoadCompressorFile(trained_compressor_path),
	                          compression_level, max_input_bytes, graph);
}

} // namespace openzl_bridge
