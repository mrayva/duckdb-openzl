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
};

// Trains a compressor against `sample_paths` (canonical parquet files sharing
// the target table's schema) and writes the result(s) under `output_path`:
//   - pareto_frontier=false (default): writes exactly `output_path`.
//   - pareto_frontier=true: writes `output_path.0`, `output_path.1`, ...,
//     ordered best-ratio-first, and returns all of their paths.
// Returns the list of paths actually written (always non-empty on success).
// Throws openzl_bridge::Error on failure (no samples, unreadable sample,
// non-canonical sample, training internal error, I/O error writing output).
std::vector<std::string> Train(const std::vector<std::string> &sample_paths, const std::string &output_path,
                                const TrainOptions &opts);

} // namespace openzl_bridge
