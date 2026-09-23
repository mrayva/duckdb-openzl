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
inline openzl::Compressor BuildParquetCompressor() {
	openzl::Compressor compressor;
	ZL_Compressor *comp = compressor.get();
	ZL_GraphID clustering = ZS2_createGraph_genericClustering(comp);
	ZL_GraphID parquet_graph = ZL_Parquet_registerGraph(comp, clustering);
	compressor.selectStartingGraph(parquet_graph);
	return compressor;
}

} // namespace internal
} // namespace openzl_bridge
