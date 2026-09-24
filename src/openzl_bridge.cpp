#include "openzl_bridge.hpp"

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

using internal::FileExists;
using internal::FileSize;
using internal::ReadFile;
using internal::WriteFile;
using internal::BuildParquetCompressor;

std::string DecompressToBuffer(const std::string &input_path) {
	if (!FileExists(input_path)) {
		throw Error("openzl_bridge: input file not found: " + input_path);
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

// Empirically determined: compression succeeded at 1.94GB and segfaulted at
// 2.42GB of canonical parquet input on this OpenZL build, consistent with an
// internal 32-bit (2^31-1 byte) size limit somewhere in the parquet graph or
// its dependencies that isn't checked before use. This threshold is a
// conservative cutoff below the observed crash point, not the exact boundary
// (which wasn't worth pinning down further -- see README for how to work
// around it: split the source table into chunks below this size).
constexpr size_t kMaxCanonicalParquetBytes = 2'000'000'000;

namespace {

// Shared by every public Compress* entry point below: `input` is already the
// full canonical-parquet bytes in memory (a public wrapper either read it
// from a file or was handed it directly), and `compressor_bytes` empty means
// "use the generic graph", non-empty means "deserialize this trained
// compressor instead". `error_label` names the input in error messages
// (a path when there is one, else something like "<in-memory input>").
void CompressBytesImpl(const std::string &input, const std::string &error_label, const std::string &output_zl_path,
                        const std::string &compressor_bytes, int compression_level) {
	if (input.size() > kMaxCanonicalParquetBytes) {
		throw Error("openzl_bridge: input parquet too large to compress safely (" + std::to_string(input.size()) +
		            " bytes, limit " + std::to_string(kMaxCanonicalParquetBytes) +
		            "): OpenZL's parquet compression graph crashes above roughly this size. "
		            "Split the source table into smaller chunks (e.g. by row count) and "
		            "compress each chunk separately.");
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
			generic_compressor = BuildParquetCompressor();
			compressor = &generic_compressor;
		}

		openzl::CCtx cctx;
		// zli's CLI sets this implicitly to the newest format the linked
		// OpenZL build supports; we must set it explicitly when driving the
		// C++ API directly, or compression fails with "Format version is not
		// set" (ZL_CParam_formatVersion, gcparams.c).
		cctx.setParameter(openzl::CParam::FormatVersion, ZL_MAX_FORMAT_VERSION);
		cctx.setParameter(openzl::CParam::CompressionLevel, compression_level);
		cctx.refCompressor(*compressor);
		std::string output = cctx.compressSerial(input);
		WriteFile(output_zl_path, output);
	} catch (const Error &) {
		throw;
	} catch (const std::exception &e) {
		throw Error(std::string("openzl_bridge: compress failed for ") + error_label +
		            " (is it canonical parquet? uncompressed, plain-encoded, no dictionary): " + e.what());
	}
}

} // namespace

void CompressParquet(const std::string &input_parquet_path, const std::string &output_zl_path,
                      const std::string &trained_compressor_path, int compression_level) {
	if (!FileExists(input_parquet_path)) {
		throw Error("openzl_bridge: input parquet file not found: " + input_parquet_path);
	}
	std::string compressor_bytes;
	if (!trained_compressor_path.empty()) {
		if (!FileExists(trained_compressor_path)) {
			throw Error("openzl_bridge: trained compressor file not found: " + trained_compressor_path);
		}
		compressor_bytes = ReadFile(trained_compressor_path);
	}
	CompressBytesImpl(ReadFile(input_parquet_path), input_parquet_path, output_zl_path, compressor_bytes,
	                   compression_level);
}

void CompressParquetWithCompressorBytes(const std::string &input_parquet_path, const std::string &output_zl_path,
                                         const std::string &compressor_bytes, int compression_level) {
	if (!FileExists(input_parquet_path)) {
		throw Error("openzl_bridge: input parquet file not found: " + input_parquet_path);
	}
	if (compressor_bytes.empty()) {
		throw Error("openzl_bridge: compressor_bytes must be non-empty (use CompressParquet() for the generic graph)");
	}
	CompressBytesImpl(ReadFile(input_parquet_path), input_parquet_path, output_zl_path, compressor_bytes,
	                   compression_level);
}

void CompressParquetBytes(const std::string &canonical_parquet_bytes, const std::string &output_zl_path,
                           const std::string &trained_compressor_path, int compression_level) {
	std::string compressor_bytes;
	if (!trained_compressor_path.empty()) {
		if (!FileExists(trained_compressor_path)) {
			throw Error("openzl_bridge: trained compressor file not found: " + trained_compressor_path);
		}
		compressor_bytes = ReadFile(trained_compressor_path);
	}
	CompressBytesImpl(canonical_parquet_bytes, "<in-memory input>", output_zl_path, compressor_bytes,
	                   compression_level);
}

} // namespace openzl_bridge
