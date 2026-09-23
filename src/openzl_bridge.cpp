#include "openzl_bridge.hpp"

#include <fstream>
#include <sstream>

#include "openzl/cpp/CCtx.hpp"
#include "openzl/cpp/CParam.hpp"
#include "openzl/cpp/Compressor.hpp"
#include "openzl/cpp/DCtx.hpp"
#include "openzl/cpp/Exception.hpp"
#include "openzl/zl_version.h"

#include "custom_parsers/parquet/parquet_graph.h"
#include "custom_parsers/shared_components/clustering.h"

namespace openzl_bridge {

namespace {

bool FileExists(const std::string &path) {
	std::ifstream f(path, std::ios::binary);
	return f.good();
}

size_t FileSize(const std::string &path) {
	std::ifstream f(path, std::ios::binary | std::ios::ate);
	return static_cast<size_t>(f.tellg());
}

std::string ReadFile(const std::string &path) {
	std::ifstream f(path, std::ios::binary);
	if (!f) {
		throw Error("openzl_bridge: could not open file for reading: " + path);
	}
	std::ostringstream ss;
	ss << f.rdbuf();
	if (f.bad()) {
		throw Error("openzl_bridge: error reading file: " + path);
	}
	return ss.str();
}

void WriteFile(const std::string &path, const std::string &contents) {
	std::ofstream f(path, std::ios::binary | std::ios::trunc);
	if (!f) {
		throw Error("openzl_bridge: could not open file for writing: " + path);
	}
	f.write(contents.data(), static_cast<std::streamsize>(contents.size()));
	if (!f) {
		throw Error("openzl_bridge: error writing file: " + path);
	}
}

// Builds the "parquet" compression graph (canonical parquet bytes in,
// OpenZL-compressed bytes out): a generic clustering successor feeding the
// parquet-aware graph, matching what `zli --profile parquet` builds
// internally (see facebook/openzl cli/utils/compress_profiles.cpp). No
// chunking -- our files are already modestly sized per-table Parquet exports.
openzl::Compressor BuildParquetCompressor() {
	openzl::Compressor compressor;
	ZL_Compressor *comp = compressor.get();
	ZL_GraphID clustering = ZS2_createGraph_genericClustering(comp);
	ZL_GraphID parquet_graph = ZL_Parquet_registerGraph(comp, clustering);
	compressor.selectStartingGraph(parquet_graph);
	return compressor;
}

} // namespace

void Decompress(const std::string &input_path, const std::string &output_path) {
	if (!FileExists(input_path)) {
		throw Error("openzl_bridge: input file not found: " + input_path);
	}
	try {
		std::string input = ReadFile(input_path);
		openzl::DCtx dctx;
		std::string output = dctx.decompressSerial(input);
		WriteFile(output_path, output);
	} catch (const Error &) {
		throw;
	} catch (const std::exception &e) {
		throw Error(std::string("openzl_bridge: decompress failed for ") + input_path + ": " + e.what());
	}
}

// Empirically determined: compression succeeded at 1.94GB and segfaulted at
// 2.42GB of canonical parquet input on this OpenZL build, consistent with an
// internal 32-bit (2^31-1 byte) size limit somewhere in the parquet graph or
// its dependencies that isn't checked before use. This threshold is a
// conservative cutoff below the observed crash point, not the exact boundary
// (which wasn't worth pinning down further -- see README for how to work
// around it: split the source table into chunks below this size).
constexpr size_t kMaxCanonicalParquetBytes = 2'000'000'000;

void CompressParquet(const std::string &input_parquet_path, const std::string &output_zl_path) {
	if (!FileExists(input_parquet_path)) {
		throw Error("openzl_bridge: input parquet file not found: " + input_parquet_path);
	}
	try {
		size_t input_size = FileSize(input_parquet_path);
		if (input_size > kMaxCanonicalParquetBytes) {
			throw Error("openzl_bridge: input parquet file too large to compress safely (" +
			            std::to_string(input_size) + " bytes, limit " +
			            std::to_string(kMaxCanonicalParquetBytes) +
			            "): OpenZL's parquet compression graph crashes above roughly this size. "
			            "Split the source table into smaller chunks (e.g. by row count) and "
			            "compress each chunk separately.");
		}
		std::string input = ReadFile(input_parquet_path);
		openzl::Compressor compressor = BuildParquetCompressor();
		openzl::CCtx cctx;
		// zli's CLI sets this implicitly to the newest format the linked
		// OpenZL build supports; we must set it explicitly when driving the
		// C++ API directly, or compression fails with "Format version is not
		// set" (ZL_CParam_formatVersion, gcparams.c).
		cctx.setParameter(openzl::CParam::FormatVersion, ZL_MAX_FORMAT_VERSION);
		// Max level (default is 6, range 1-9): benchmarked against real mirror
		// tables, this is consistently smaller than the default for a modest,
		// well-worth-it CPU cost -- this is a compress-once/read-many archival
		// workload, so we bias fully toward ratio over compression speed.
		cctx.setParameter(openzl::CParam::CompressionLevel, 9);
		cctx.refCompressor(compressor);
		std::string output = cctx.compressSerial(input);
		WriteFile(output_zl_path, output);
	} catch (const Error &) {
		throw;
	} catch (const std::exception &e) {
		throw Error(std::string("openzl_bridge: compress failed for ") + input_parquet_path +
		            " (is it canonical parquet? uncompressed, plain-encoded, no dictionary): " + e.what());
	}
}

} // namespace openzl_bridge
