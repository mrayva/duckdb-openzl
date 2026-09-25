#include "openzl_train_bridge.hpp"

#include <cstdio>

#include "openzl/cpp/CParam.hpp"
#include "openzl/cpp/Compressor.hpp"
#include "openzl/cpp/Exception.hpp"
#include "openzl/zl_version.h"

#include "custom_parsers/dependency_registration.h"

#include "tools/logger/Logger.h"
#include "tools/training/train.h"
#include "tools/training/train_params.h"
#include "tools/training/utils/benchmark.h"
#include "tools/training/utils/utils.h"

#include "openzl_internal_io.hpp"

namespace openzl_bridge {

namespace {

using internal::BuildParquetCompressor;
using internal::FileExists;
using internal::FileSize;
using internal::ReadFile;
using internal::WriteFile;

openzl::training::ClusteringTrainer ToOpenzlTrainer(ClusteringTrainer t) {
	switch (t) {
	case ClusteringTrainer::Greedy:
		return openzl::training::Greedy;
	case ClusteringTrainer::BottomUp:
		return openzl::training::BottomUp;
	case ClusteringTrainer::FullSplit:
		return openzl::training::FullSplit;
	}
	throw Error("openzl_bridge: unreachable clustering trainer value");
}

// Applies num_samples/max_file_size_mb/max_total_size_mb ourselves rather
// than via OpenZL's own SampleLimiter, which operates on its tools::io
// InputSet abstraction -- overkill here since we're already reading files
// directly. Takes samples in the given order (deterministic) up to the
// first limit hit; oversized individual files are skipped, not truncated.
std::vector<std::string> SelectSamplePaths(const std::vector<std::string> &sample_paths, const TrainOptions &opts) {
	size_t max_file_bytes = (opts.max_file_size_mb > 0 ? opts.max_file_size_mb : 150) * 1024 * 1024;
	size_t max_total_bytes = (opts.max_total_size_mb > 0 ? opts.max_total_size_mb : 300) * 1024 * 1024;

	std::vector<std::string> selected;
	size_t total = 0;
	for (const auto &path : sample_paths) {
		if (opts.num_samples > 0 && selected.size() >= opts.num_samples) {
			break;
		}
		size_t sz = FileSize(path);
		if (sz > max_file_bytes) {
			continue;
		}
		if (total + sz > max_total_bytes && !selected.empty()) {
			break;
		}
		selected.push_back(path);
		total += sz;
	}
	if (selected.empty()) {
		throw Error("openzl_bridge: no sample files survived the size limits (max_file_size_mb=" +
		            std::to_string(max_file_bytes / (1024 * 1024)) +
		            ", max_total_size_mb=" + std::to_string(max_total_bytes / (1024 * 1024)) + ")");
	}
	return selected;
}

std::string NumberedPath(const std::string &output_path, size_t i) {
	return output_path + "." + std::to_string(i);
}

} // namespace

std::vector<TrainedOutput> Train(const std::vector<std::string> &sample_paths, const std::string &output_path,
                                  const TrainOptions &opts) {
	if (sample_paths.empty()) {
		throw Error("openzl_bridge: Train() requires at least one sample file");
	}
	for (const auto &path : sample_paths) {
		if (!FileExists(path)) {
			throw Error("openzl_bridge: sample file not found: " + path);
		}
	}

	openzl::tools::logger::Logger::instance().setGlobalLoggerVerbosity(
	    opts.verbose ? openzl::tools::logger::VERBOSE1 : openzl::tools::logger::ERRORS);

	try {
		std::vector<std::string> selected_paths = SelectSamplePaths(sample_paths, opts);

		// Keep every sample's bytes alive for the whole call: MultiInput/Input
		// reference this memory, they don't own a copy of it.
		std::vector<std::string> sample_buffers;
		sample_buffers.reserve(selected_paths.size());
		std::vector<openzl::training::MultiInput> inputs;
		inputs.reserve(selected_paths.size());
		for (const auto &path : selected_paths) {
			sample_buffers.push_back(ReadFile(path));
		}
		for (const auto &buf : sample_buffers) {
			openzl::training::MultiInput mi;
			mi.add(openzl::Input::refSerial(buf));
			inputs.push_back(std::move(mi));
		}

		GraphOptions graph_opts;
		graph_opts.format_version = opts.format_version;
		graph_opts.parquet_chunk_bytes = opts.parquet_chunk_bytes;
		ValidateGraphOptions(graph_opts);
		openzl::Compressor compressor = BuildParquetCompressor(opts.parquet_chunk_bytes);
		// Same requirement as CompressParquet(): required when driving the C++
		// API directly, or training fails with "Compressor format version is
		// not set" the first time it tries to compress a candidate.
		compressor.setParameter(openzl::CParam::FormatVersion,
		                        opts.format_version == 0 ? ZL_MAX_FORMAT_VERSION : opts.format_version);
		compressor.setParameter(openzl::CParam::CompressionLevel, opts.compression_level);

		openzl::training::TrainParams params;
		params.compressorGenFunc = [](openzl::poly::string_view serialized,
		                               openzl::poly::string_view fatBundle) -> std::unique_ptr<openzl::Compressor> {
			return openzl::custom_parsers::createCompressorFromSerialized(serialized, fatBundle);
		};
		if (opts.threads > 0) {
			params.threads = opts.threads;
		}
		params.clusteringTrainer = ToOpenzlTrainer(opts.clustering_trainer);
		if (opts.num_samples > 0) {
			params.numSamples = opts.num_samples;
		}
		params.noAceSuccessors = opts.no_ace_successors;
		params.noClustering = opts.no_clustering;
		params.dictTraining = opts.dict_training;
		if (opts.max_time_secs > 0) {
			params.maxTimeSecs = opts.max_time_secs;
		}
		if (opts.max_file_size_mb > 0) {
			params.maxFileSizeMb = opts.max_file_size_mb;
		}
		if (opts.max_total_size_mb > 0) {
			params.maxTotalSizeMb = opts.max_total_size_mb;
		}
		params.paretoFrontier = opts.pareto_frontier;
		if (opts.max_num_candidates > 0) {
			params.maxNumCandidates = opts.max_num_candidates;
		}

		std::vector<openzl::training::TrainedCandidate> candidates = openzl::training::train(inputs, compressor, params);
		if (candidates.empty()) {
			throw Error("openzl_bridge: training produced no candidates");
		}

		// Optional per-candidate benchmark (ratio + speeds). Held-out files if
		// given, else the training samples themselves.
		const bool do_benchmark = opts.benchmark_set ? opts.benchmark : opts.pareto_frontier;
		std::vector<std::string> bench_buffers;
		std::vector<openzl::training::MultiInput> bench_inputs;
		if (do_benchmark && !opts.benchmark_paths.empty()) {
			for (const auto &path : opts.benchmark_paths) {
				if (!FileExists(path)) {
					throw Error("openzl_bridge: benchmark file not found: " + path);
				}
				bench_buffers.push_back(ReadFile(path));
			}
			for (const auto &buf : bench_buffers) {
				openzl::training::MultiInput mi;
				mi.add(openzl::Input::refSerial(buf));
				bench_inputs.push_back(std::move(mi));
			}
		}
		const auto &bench_set = bench_inputs.empty() ? inputs : bench_inputs;
		auto benchmark_candidate = [&](openzl::training::TrainedCandidate &cand, TrainedOutput &out) {
			if (!do_benchmark) {
				return;
			}
			std::string fat_bundle;
			if (!cand.dicts.empty()) {
				fat_bundle = cand.packFatBundle();
			}
			auto comp = params.compressorGenFunc(cand.serializedCompressor, fat_bundle);
			auto res = openzl::training::benchmark(*comp, bench_set, fat_bundle);
			if (!res) {
				throw Error("openzl_bridge: benchmarking a trained candidate failed");
			}
			out.has_benchmark = true;
			out.original_bytes = res->originalSize;
			out.compressed_bytes = res->compressedSize;
			out.compression_ratio = res->compressionRatio();
			out.compress_mb_s = res->compressionSpeedMBps();
			out.decompress_mb_s = res->decompressionSpeedMBps();
		};

		std::vector<TrainedOutput> outputs;
		if (!opts.pareto_frontier) {
			if (candidates.size() != 1) {
				throw Error("openzl_bridge: non-Pareto training produced " + std::to_string(candidates.size()) +
				            " candidates, expected 1");
			}
			TrainedOutput out;
			benchmark_candidate(candidates[0], out);
			out.compressor_bytes = std::move(candidates[0].serializedCompressor);
			if (!output_path.empty()) {
				WriteFile(output_path, out.compressor_bytes);
				out.path = output_path;
			}
			outputs.push_back(std::move(out));
		} else {
			for (size_t i = 0; i < candidates.size(); ++i) {
				TrainedOutput out;
				benchmark_candidate(candidates[i], out);
				out.compressor_bytes = std::move(candidates[i].serializedCompressor);
				if (!output_path.empty()) {
					std::string path = NumberedPath(output_path, i);
					WriteFile(path, out.compressor_bytes);
					out.path = path;
				}
				outputs.push_back(std::move(out));
			}
		}
		return outputs;
	} catch (const Error &) {
		throw;
	} catch (const std::exception &e) {
		throw Error(std::string("openzl_bridge: training failed: ") + e.what());
	}
}

} // namespace openzl_bridge
