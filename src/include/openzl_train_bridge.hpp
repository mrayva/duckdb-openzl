// Copyright (c) 2026.
//
// Wraps OpenZL's offline training pipeline (tools/training/): given a batch
// of representative canonical-parquet sample files, it searches for a better
// compressor than the generic "parquet" graph -- learning per-column
// clustering choices and optionally a shared dictionary -- and serializes
// the result to disk for reuse by CompressParquet()'s trained_compressor_path
// argument (see openzl_bridge.hpp).
//
// Like openzl_bridge.hpp, this header has no DuckDB dependency on purpose.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace openzl_bridge {

// Which search strategy OpenZL's clustering trainer uses to group columns.
// Mirrors openzl::training::ClusteringTrainer 1:1. Greedy is fastest;
// FullSplit explores the most candidates and is slowest.
enum class ClusteringTrainer {
	Greedy,
	BottomUp,
	FullSplit,
};

struct TrainOptions {
	// Parallelism for the training search itself (not for the eventual
	// compress calls, which are always single-threaded). 0 = OpenZL's own
	// default (hardware concurrency).
	unsigned int threads = 0;

	ClusteringTrainer clustering_trainer = ClusteringTrainer::Greedy;

	// How many of the sample files to actually train on. 0 = use all of them
	// (subject to the size caps below).
	size_t num_samples = 0;

	// Skips ACE (Automated Compressor Explorer) successor search -- faster
	// training, potentially worse compressors. ACE tries alternative codec
	// choices per cluster beyond the default backend.
	bool no_ace_successors = false;

	// Skips per-column clustering search entirely, training only ACE
	// successors (if enabled) on top of the generic graph's own clustering.
	bool no_clustering = false;

	// Also trains a shared zstd dictionary alongside the clustering graph.
	// Most useful when samples share repeated short strings across rows
	// (e.g. symbol codes, venue codes) that per-column clustering alone
	// doesn't fully exploit.
	bool dict_training = false;

	// Wall-clock budget for the training search. 0 = no limit (the search
	// still terminates on its own once each trainer converges).
	size_t max_time_secs = 0;

	// Per-sample and total sample-corpus size caps in MB, so training a huge
	// table doesn't require reading all of it. OpenZL's own defaults (150MB /
	// 300MB) are used when left at 0.
	size_t max_file_size_mb = 0;
	size_t max_total_size_mb = 0;

	// If true, returns a Pareto frontier of candidates trading off ratio vs.
	// speed instead of a single "best" compressor -- Train() then writes one
	// file per candidate. If false (default), returns exactly one candidate.
	bool pareto_frontier = false;

	// Caps how many candidates a single trainer keeps internally before
	// combining with other trainers' results. 0 = no cap.
	size_t max_num_candidates = 0;

	// Applied to the base "parquet" compressor before training explores
	// variations of it -- same knob CompressParquet() takes directly.
	int compression_level = 9;

	// OpenZL's training internals print progress bars/log lines (via a
	// process-global logger, defaulting to INFO) meant for interactive CLI
	// use; that's noisy for a function called from SQL, so Train() quiets it
	// to errors-only unless this is set. Since the logger is a global
	// singleton, this also affects any other OpenZL logging in the same
	// process for the duration of the call.
	bool verbose = false;

	// Frame format version and parquet internal chunk size baked into the
	// trained compressor (see GraphOptions in openzl_bridge.hpp; the parquet
	// profile is the only one that can be trained here). format_version 0 =
	// newest; parquet_chunk_bytes defaults to kAutoParquetChunkBytes (20MB when
	// the format version allows, else none), 0 = never.
	int format_version = 0;
	size_t parquet_chunk_bytes = static_cast<size_t>(-1);

	// Benchmarks each returned candidate (compression ratio, compress and
	// decompress MB/s -- the numbers `zli train --pareto-frontier` writes to
	// benchmark.csv) so the caller can pick a point on the frontier. Costs one
	// extra compress+decompress pass per candidate. Defaults to on for
	// pareto_frontier, off otherwise (see TrainOptions handling in Train()).
	// Set explicitly via `benchmark_set`.
	bool benchmark = false;
	bool benchmark_set = false;

	// Files to benchmark the candidates on. Empty = the training samples
	// themselves, which flatters the ratio (the compressor was fit to them);
	// pass held-out canonical parquet files (same schema) for honest numbers.
	std::vector<std::string> benchmark_paths;
};

// One trained candidate: the serialized compressor bytes (always populated),
// and the file path it was written to ("" if `output_path` was empty --
// i.e. the caller only wants the bytes, to store wherever it likes, e.g. as
// a BLOB column in a DuckDB table, rather than as a standalone file).
struct TrainedOutput {
	std::string path;
	std::string compressor_bytes;

	// Filled only when benchmarking ran (has_benchmark). Measured over the
	// benchmark files (or the training samples if none were given).
	bool has_benchmark = false;
	uint64_t original_bytes = 0;
	uint64_t compressed_bytes = 0;
	double compression_ratio = 0;
	double compress_mb_s = 0;
	double decompress_mb_s = 0;
};

// Trains a compressor against `sample_paths` (canonical parquet files sharing
// the target table's schema).
//
// `output_path` controls on-disk persistence ("outside the database"):
//   - non-empty, pareto_frontier=false (default): writes exactly `output_path`.
//   - non-empty, pareto_frontier=true: writes `output_path.0`, `output_path.1`,
//     ..., ordered best-ratio-first.
//   - empty: writes nothing -- every TrainedOutput.compressor_bytes is still
//     populated, for the caller to persist however it wants instead (e.g.
//     "inside the database", as a BLOB column alongside the data it
//     compresses -- see CompressParquetWithCompressorBytes() in
//     openzl_bridge.hpp for the matching read-back path).
// Returns one TrainedOutput per candidate (always exactly one unless
// pareto_frontier=true). Throws openzl_bridge::Error on failure (no samples,
// unreadable sample, non-canonical sample, training internal error, I/O
// error writing output).
std::vector<TrainedOutput> Train(const std::vector<std::string> &sample_paths, const std::string &output_path,
                                 const TrainOptions &opts);

} // namespace openzl_bridge
