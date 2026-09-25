// Copyright (c) 2026.
//
// Tiny file I/O helpers shared by openzl_bridge.cpp and
// openzl_train_bridge.cpp. Not part of the public bridge API -- internal
// only, hence living outside openzl_bridge.hpp.
#pragma once

#include <fstream>
#include <sstream>
#include <string>

#include "openzl/cpp/Compressor.hpp"

#include "openzl/codecs/zl_ace.h"
#include "openzl/codecs/zl_lz.h"
#include "openzl/codecs/zl_segmenters.h"
#include "custom_parsers/parquet/parquet_graph.h"
#include "custom_parsers/shared_components/clustering.h"

#include "openzl_bridge.hpp"

namespace openzl_bridge {
namespace internal {

inline bool FileExists(const std::string &path) {
	std::ifstream f(path, std::ios::binary);
	return f.good();
}

inline size_t FileSize(const std::string &path) {
	std::ifstream f(path, std::ios::binary | std::ios::ate);
	return static_cast<size_t>(f.tellg());
}

inline std::string ReadFile(const std::string &path) {
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

inline void WriteFile(const std::string &path, const std::string &contents) {
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
// internally (see facebook/openzl cli/utils/compress_profiles.cpp). This is
// also the base graph Train() starts its search from.
//
// `chunk_bytes` > 0 makes the parquet graph split its input into independently
// compressed chunks of about that size inside the frame (upstream's own
// `zli --profile parquet` uses 20MB); 0 (the historical default here) compresses
// the whole input as one chunk. Chunking needs frame format version >= 21.
inline openzl::Compressor BuildParquetCompressor(size_t chunk_bytes = 0) {
	openzl::Compressor compressor;
	ZL_Compressor *comp = compressor.get();
	ZL_GraphID clustering = ZS2_createGraph_genericClustering(comp);
	ZL_GraphID parquet_graph = ZL_Parquet_registerGraph_withChunkSize(comp, clustering, static_cast<int>(chunk_bytes));
	compressor.selectStartingGraph(parquet_graph);
	return compressor;
}

// The "serial" profile: parquet bytes treated as opaque data (an LZ-backed
// ACE graph under a serial segmenter), like `zli --profile serial`. No parquet
// awareness, so it also accepts non-canonical input; useful as a baseline for
// what the parquet graph buys. `chunk_bytes` 0 = OpenZL's default segment size.
inline openzl::Compressor BuildSerialCompressor(size_t chunk_bytes = 0) {
	openzl::Compressor compressor;
	ZL_Compressor *comp = compressor.get();
	ZL_GraphID inner = ZL_Compressor_buildACEGraphWithDefault(comp, ZL_GRAPH_LZ);
	if (!ZL_GraphID_isValid(inner)) {
		throw Error("openzl_bridge: could not build the serial profile's inner graph");
	}
	ZL_GraphID graph = ZL_Compressor_buildSerialSegmenter(
	    comp, chunk_bytes > 0 ? chunk_bytes : ZL_DEFAULT_SEGMENTER_CHUNK_BYTE_SIZE, inner);
	if (!ZL_GraphID_isValid(graph)) {
		throw Error("openzl_bridge: could not build the serial profile graph");
	}
	compressor.selectStartingGraph(graph);
	return compressor;
}

} // namespace internal
} // namespace openzl_bridge
