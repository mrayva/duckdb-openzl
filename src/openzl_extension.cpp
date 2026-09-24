#define DUCKDB_EXTENSION_MAIN

#include "openzl_extension.hpp"
#include "openzl_bridge.hpp"
#include "openzl_file_system.hpp"
#include "openzl_train_bridge.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

#include <cstdio>

namespace duckdb {

// openzl_decompress(input_zl, output_parquet) -> output_parquet
//
// Decompresses an OpenZL archive back into a plain, directly queryable
// parquet file. Typical usage:
//   SELECT openzl_decompress('data.zl', 'data.parquet');
//   SELECT * FROM read_parquet('data.parquet');
inline void OpenzlDecompressFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vec = args.data[0];
	auto &output_vec = args.data[1];
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    input_vec, output_vec, result, args.size(), [&](string_t input_path, string_t output_path) {
		    try {
			    openzl_bridge::Decompress(input_path.GetString(), output_path.GetString());
		    } catch (const openzl_bridge::Error &e) {
			    throw IOException(e.what());
		    }
		    return StringVector::AddString(result, output_path);
	    });
}

// openzl_compress(input_parquet, output_zl) -> output_zl
//
// Canonicalizes and OpenZL-compresses a parquet file. Does not delete the
// source file. Typical usage:
//   COPY tbl TO 'staging.parquet' (FORMAT PARQUET);
//   SELECT openzl_compress('staging.parquet', 'data.zl');
inline void OpenzlCompressFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vec = args.data[0];
	auto &output_vec = args.data[1];
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    input_vec, output_vec, result, args.size(), [&](string_t input_path, string_t output_path) {
		    try {
			    openzl_bridge::CompressParquet(input_path.GetString(), output_path.GetString());
		    } catch (const openzl_bridge::Error &e) {
			    throw IOException(e.what());
		    }
		    return StringVector::AddString(result, output_path);
	    });
}

// openzl_compress(input_parquet, output_zl, trained_compressor[, compression_level]) -> output_zl
//
// Same as openzl_compress/2, but compresses with a compressor previously
// produced by openzl_train() instead of the generic "parquet" graph.
// `trained_compressor` may be '' or NULL to fall back to the generic graph
// (useful when a caller wants a single code path that conditionally trains).
// `compression_level` (1-9, default 9) applies either way.
//
// Implemented with plain per-row Value access rather than a vectorized
// executor: this function does real file I/O and OpenZL compression work per
// call, so the row-boxing overhead here is immaterial.
inline void OpenzlCompressWithOptionsFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	for (idx_t i = 0; i < count; i++) {
		auto input_path = args.data[0].GetValue(i).ToString();
		auto output_path = args.data[1].GetValue(i).ToString();
		auto trained_value = args.data[2].GetValue(i);
		string trained_path = trained_value.IsNull() ? string() : trained_value.ToString();
		int compression_level = 9;
		if (args.ColumnCount() > 3) {
			auto level_value = args.data[3].GetValue(i);
			if (!level_value.IsNull()) {
				compression_level = level_value.GetValue<int32_t>();
			}
		}
		try {
			openzl_bridge::CompressParquet(input_path, output_path, trained_path, compression_level);
		} catch (const openzl_bridge::Error &e) {
			throw IOException(e.what());
		}
		result.SetValue(i, Value(output_path));
	}
}

// openzl_compress(input_parquet, output_zl, trained_compressor_bytes[, compression_level]) -> output_zl
//
// Same as the VARCHAR trained_compressor overload above, except the trained
// compressor is passed as already-in-memory bytes -- e.g. a BLOB column read
// back from a table, for callers who keep trained compressors "inside the
// database" instead of as standalone files (see openzl_train's
// compressor_bytes output column). Unlike the path-based overload, an empty/
// NULL blob is an error, not a generic-graph fallback: use the VARCHAR
// overload with '' for that.
inline void OpenzlCompressWithBlobOptionsFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	for (idx_t i = 0; i < count; i++) {
		auto input_path = args.data[0].GetValue(i).ToString();
		auto output_path = args.data[1].GetValue(i).ToString();
		auto trained_value = args.data[2].GetValue(i);
		if (trained_value.IsNull()) {
			throw IOException("openzl_compress: trained_compressor_bytes must not be NULL");
		}
		string compressor_bytes = StringValue::Get(trained_value);
		int compression_level = 9;
		if (args.ColumnCount() > 3) {
			auto level_value = args.data[3].GetValue(i);
			if (!level_value.IsNull()) {
				compression_level = level_value.GetValue<int32_t>();
			}
		}
		try {
			openzl_bridge::CompressParquetWithCompressorBytes(input_path, output_path, compressor_bytes,
			                                                   compression_level);
		} catch (const openzl_bridge::Error &e) {
			throw IOException(e.what());
		}
		result.SetValue(i, Value(output_path));
	}
}

// read_openzl(path) -> table
//
// Single-call read: decompresses the archive straight into memory and reads
// it back through DuckDB's own parquet reader, with no intermediate
// .parquet file ever touching disk. Typical usage:
//   SELECT * FROM read_openzl('data.zl');
//
// Built on the "openzl://" virtual filesystem (openzl_file_system.hpp),
// registered below: read_parquet('openzl://data.zl') opens that scheme,
// which decompresses "data.zl" into an in-memory buffer
// (openzl_bridge::DecompressToBuffer) and serves reads directly from it --
// the same mechanism extensions like httpfs use for "s3://"/"https://".
// Each call re-decompresses (no cross-call caching), so repeated reads of
// the same archive cost repeated decompression, not repeated disk I/O.
static const DefaultTableMacro OpenzlReadMacro = {DEFAULT_SCHEMA, "read_openzl", {"path", nullptr},
                                                   {{nullptr, nullptr}},
                                                   R"(SELECT * FROM read_parquet('openzl://' || path))"};

// openzl_train(sample_paths, output_path[, named options...]) -> table
//
// Trains a compressor against a batch of representative canonical-parquet
// sample files (same schema as the table(s) it'll later compress) and writes
// the result to output_path, for reuse via openzl_compress's or
// COPY ... FORMAT OPENZL's trained-compressor argument. Typical usage:
//   SELECT * FROM openzl_train(
//     ['sample1.parquet', 'sample2.parquet'], 'nbbo.compressor',
//     clustering_trainer := 'bottom_up', dict_training := true);
//
// output_path may be NULL to skip writing a file entirely: every result row
// still carries the trained compressor's raw bytes in compressor_bytes, for
// the caller to persist "inside the database" instead -- e.g.
// CREATE TABLE compressors AS SELECT * FROM openzl_train([...], NULL);
// stores it in an ordinary table (a BLOB column), rather than as a
// standalone file. Both may be used together (write a file AND get the
// bytes back) -- output_path only controls on-disk persistence.
//
// Returns one row per candidate. Normally that's a single row --
// pareto_frontier := true instead returns one row per point on the
// ratio/speed trade-off curve, letting the caller pick one.
//
// All options below are optional; see openzl_train_bridge.hpp for what each
// one does to OpenZL's training search.
struct OpenzlTrainBindData : public TableFunctionData {
	vector<openzl_bridge::TrainedOutput> outputs;
};

static Value GetNamedParameter(TableFunctionBindInput &input, const string &name) {
	auto it = input.named_parameters.find(name);
	if (it == input.named_parameters.end()) {
		return Value();
	}
	return it->second;
}

static string GetNamedString(TableFunctionBindInput &input, const string &name, const string &default_value) {
	auto value = GetNamedParameter(input, name);
	return value.IsNull() ? default_value : value.ToString();
}

static int64_t GetNamedBigint(TableFunctionBindInput &input, const string &name, int64_t default_value) {
	auto value = GetNamedParameter(input, name);
	return value.IsNull() ? default_value : value.GetValue<int64_t>();
}

static bool GetNamedBool(TableFunctionBindInput &input, const string &name, bool default_value) {
	auto value = GetNamedParameter(input, name);
	return value.IsNull() ? default_value : value.GetValue<bool>();
}

static openzl_bridge::ClusteringTrainer ParseClusteringTrainer(const string &value) {
	if (value == "greedy") {
		return openzl_bridge::ClusteringTrainer::Greedy;
	}
	if (value == "bottom_up") {
		return openzl_bridge::ClusteringTrainer::BottomUp;
	}
	if (value == "full_split") {
		return openzl_bridge::ClusteringTrainer::FullSplit;
	}
	throw BinderException(
	    "openzl_train: clustering_trainer must be 'greedy', 'bottom_up', or 'full_split', got '" + value + "'");
}

static unique_ptr<FunctionData> OpenzlTrainBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[0].type().id() != LogicalTypeId::LIST) {
		throw BinderException(
		    "openzl_train: first argument must be a list of sample file paths, e.g. ['a.parquet', 'b.parquet']");
	}
	vector<string> sample_paths;
	for (auto &child : ListValue::GetChildren(input.inputs[0])) {
		sample_paths.push_back(StringValue::Get(child));
	}
	// NULL means "don't write a file, just return the bytes" -- see the
	// comment on OpenzlTrainBindData above.
	string output_path = input.inputs[1].IsNull() ? string() : StringValue::Get(input.inputs[1]);

	openzl_bridge::TrainOptions opts;
	opts.threads = static_cast<unsigned int>(GetNamedBigint(input, "threads", 0));
	opts.clustering_trainer = ParseClusteringTrainer(GetNamedString(input, "clustering_trainer", "greedy"));
	opts.num_samples = static_cast<size_t>(GetNamedBigint(input, "num_samples", 0));
	opts.no_ace_successors = GetNamedBool(input, "no_ace_successors", false);
	opts.no_clustering = GetNamedBool(input, "no_clustering", false);
	opts.dict_training = GetNamedBool(input, "dict_training", false);
	opts.max_time_secs = static_cast<size_t>(GetNamedBigint(input, "max_time_secs", 0));
	opts.max_file_size_mb = static_cast<size_t>(GetNamedBigint(input, "max_file_size_mb", 0));
	opts.max_total_size_mb = static_cast<size_t>(GetNamedBigint(input, "max_total_size_mb", 0));
	opts.pareto_frontier = GetNamedBool(input, "pareto_frontier", false);
	opts.max_num_candidates = static_cast<size_t>(GetNamedBigint(input, "max_num_candidates", 0));
	opts.compression_level = static_cast<int>(GetNamedBigint(input, "compression_level", 9));
	opts.verbose = GetNamedBool(input, "verbose", false);

	auto result = make_uniq<OpenzlTrainBindData>();
	try {
		result->outputs = openzl_bridge::Train(sample_paths, output_path, opts);
	} catch (const openzl_bridge::Error &e) {
		throw IOException(e.what());
	}

	return_types.push_back(LogicalType::BIGINT);
	names.push_back("candidate_index");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("output_path");
	return_types.push_back(LogicalType::BLOB);
	names.push_back("compressor_bytes");
	return std::move(result);
}

struct OpenzlTrainGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

static unique_ptr<GlobalTableFunctionState> OpenzlTrainInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	return make_uniq<OpenzlTrainGlobalState>();
}

// The actual training work already happened in Bind (it's a one-shot,
// non-streaming search over a fixed, already-in-hand sample set, not
// something that benefits from the scan-style init/execute split) -- this
// just emits the small number of already-computed result rows.
static void OpenzlTrainFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<OpenzlTrainBindData>();
	auto &gstate = data.global_state->Cast<OpenzlTrainGlobalState>();
	idx_t count = 0;
	while (gstate.offset < bind_data.outputs.size() && count < STANDARD_VECTOR_SIZE) {
		auto &out = bind_data.outputs[gstate.offset];
		output.SetValue(0, count, Value::BIGINT(static_cast<int64_t>(gstate.offset)));
		output.SetValue(1, count, out.path.empty() ? Value(LogicalType::VARCHAR) : Value(out.path));
		output.SetValue(2, count, Value::BLOB_RAW(out.compressor_bytes));
		gstate.offset++;
		count++;
	}
	output.SetCardinality(count);
}

static TableFunction GetOpenzlTrainFunction() {
	TableFunction function("openzl_train", {LogicalType::LIST(LogicalType::VARCHAR), LogicalType::VARCHAR},
	                        OpenzlTrainFunction, OpenzlTrainBind, OpenzlTrainInitGlobal);
	function.named_parameters["threads"] = LogicalType::BIGINT;
	function.named_parameters["clustering_trainer"] = LogicalType::VARCHAR;
	function.named_parameters["num_samples"] = LogicalType::BIGINT;
	function.named_parameters["no_ace_successors"] = LogicalType::BOOLEAN;
	function.named_parameters["no_clustering"] = LogicalType::BOOLEAN;
	function.named_parameters["dict_training"] = LogicalType::BOOLEAN;
	function.named_parameters["max_time_secs"] = LogicalType::BIGINT;
	function.named_parameters["max_file_size_mb"] = LogicalType::BIGINT;
	function.named_parameters["max_total_size_mb"] = LogicalType::BIGINT;
	function.named_parameters["pareto_frontier"] = LogicalType::BOOLEAN;
	function.named_parameters["max_num_candidates"] = LogicalType::BIGINT;
	function.named_parameters["compression_level"] = LogicalType::BIGINT;
	function.named_parameters["verbose"] = LogicalType::BOOLEAN;
	return function;
}

// COPY tbl TO 'data.zl' (FORMAT OPENZL)
//
// Delegates entirely to the catalog's registered "parquet" CopyFunction to do
// the actual writing -- staged into a canonical (uncompressed, plain-encoded,
// no dictionary) temp parquet file next to the real destination -- then
// OpenZL-compresses that temp file into the real destination on finalize and
// removes it. This avoids reimplementing a parquet writer: we only own the
// bind/global-state bookkeeping and forward every row batch straight through
// to parquet's own sink/combine/finalize.
struct OpenzlCopyBindData : public FunctionData {
	OpenzlCopyBindData() : parquet_copy_function("parquet") {
	}

	CopyFunction parquet_copy_function;
	unique_ptr<FunctionData> parquet_bind_data;
	// '' = use the generic "parquet" graph (default).
	string trained_compressor_path;
	int compression_level = 9;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<OpenzlCopyBindData>();
		result->parquet_copy_function = parquet_copy_function;
		result->parquet_bind_data = parquet_bind_data->Copy();
		result->trained_compressor_path = trained_compressor_path;
		result->compression_level = compression_level;
		return std::move(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<OpenzlCopyBindData>();
		return parquet_bind_data->Equals(*other.parquet_bind_data) &&
		       trained_compressor_path == other.trained_compressor_path &&
		       compression_level == other.compression_level;
	}
};

struct OpenzlCopyGlobalState : public GlobalFunctionData {
	string tmp_parquet_path;
	string final_zl_path;
	unique_ptr<GlobalFunctionData> parquet_global_state;
};

struct OpenzlCopyLocalState : public LocalFunctionData {
	unique_ptr<LocalFunctionData> parquet_local_state;
};

static unique_ptr<FunctionData> OpenzlCopyBind(ClientContext &context, CopyFunctionBindInput &input,
                                                const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto &parquet_entry =
	    Catalog::GetEntry<CopyFunctionCatalogEntry>(context, SYSTEM_CATALOG, DEFAULT_SCHEMA, "parquet");

	auto result = make_uniq<OpenzlCopyBindData>();
	result->parquet_copy_function = parquet_entry.function;

	// Force the canonical form OpenZL's parquet graph requires (uncompressed,
	// plain-encoded, no dictionary pages) regardless of what the user passed
	// -- there is no other valid way to write this format.
	CopyInfo parquet_info;
	parquet_info.format = "parquet";
	parquet_info.file_path = input.info.file_path;
	parquet_info.options["compression"] = {Value("uncompressed")};
	parquet_info.options["dictionary_size_limit"] = {Value::BIGINT(0)};

	CopyFunctionBindInput parquet_bind_input(parquet_info);
	result->parquet_bind_data =
	    result->parquet_copy_function.copy_to_bind(context, parquet_bind_input, names, sql_types);

	auto trained_it = input.info.options.find("trained_compressor");
	if (trained_it != input.info.options.end() && !trained_it->second.empty()) {
		result->trained_compressor_path = trained_it->second[0].ToString();
	}
	auto level_it = input.info.options.find("compression_level");
	if (level_it != input.info.options.end() && !level_it->second.empty()) {
		result->compression_level = level_it->second[0].GetValue<int32_t>();
	}
	return std::move(result);
}

static unique_ptr<GlobalFunctionData> OpenzlCopyInitGlobal(ClientContext &context, FunctionData &bind_data_p,
                                                            const string &file_path) {
	auto &bind_data = bind_data_p.Cast<OpenzlCopyBindData>();
	auto result = make_uniq<OpenzlCopyGlobalState>();
	// `file_path` here is whatever the framework decided the real output path
	// is for this run (it may be a "tmp_"-prefixed sibling if it plans to
	// rename over an existing file atomically after we return successfully).
	// Stage parquet's own output under a distinct suffix next to it, and
	// point our OpenZL output at `file_path` itself, so that path -- whatever
	// it is -- ends up holding a valid .zl archive exactly when the framework
	// expects one to be there.
	result->final_zl_path = file_path;
	result->tmp_parquet_path = file_path + ".openzl_staging.parquet";
	result->parquet_global_state = bind_data.parquet_copy_function.copy_to_initialize_global(
	    context, *bind_data.parquet_bind_data, result->tmp_parquet_path);
	return std::move(result);
}

static unique_ptr<LocalFunctionData> OpenzlCopyInitLocal(ExecutionContext &context, FunctionData &bind_data_p) {
	auto &bind_data = bind_data_p.Cast<OpenzlCopyBindData>();
	auto result = make_uniq<OpenzlCopyLocalState>();
	result->parquet_local_state =
	    bind_data.parquet_copy_function.copy_to_initialize_local(context, *bind_data.parquet_bind_data);
	return std::move(result);
}

static void OpenzlCopySink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p,
                            LocalFunctionData &lstate_p, DataChunk &input) {
	auto &bind_data = bind_data_p.Cast<OpenzlCopyBindData>();
	auto &gstate = gstate_p.Cast<OpenzlCopyGlobalState>();
	auto &lstate = lstate_p.Cast<OpenzlCopyLocalState>();
	bind_data.parquet_copy_function.copy_to_sink(context, *bind_data.parquet_bind_data, *gstate.parquet_global_state,
	                                              *lstate.parquet_local_state, input);
}

static void OpenzlCopyCombine(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p,
                               LocalFunctionData &lstate_p) {
	auto &bind_data = bind_data_p.Cast<OpenzlCopyBindData>();
	auto &gstate = gstate_p.Cast<OpenzlCopyGlobalState>();
	auto &lstate = lstate_p.Cast<OpenzlCopyLocalState>();
	bind_data.parquet_copy_function.copy_to_combine(context, *bind_data.parquet_bind_data,
	                                                 *gstate.parquet_global_state, *lstate.parquet_local_state);
}

static void OpenzlCopyFinalize(ClientContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p) {
	auto &bind_data = bind_data_p.Cast<OpenzlCopyBindData>();
	auto &gstate = gstate_p.Cast<OpenzlCopyGlobalState>();
	bind_data.parquet_copy_function.copy_to_finalize(context, *bind_data.parquet_bind_data,
	                                                  *gstate.parquet_global_state);
	try {
		openzl_bridge::CompressParquet(gstate.tmp_parquet_path, gstate.final_zl_path, bind_data.trained_compressor_path,
		                                bind_data.compression_level);
	} catch (const openzl_bridge::Error &e) {
		std::remove(gstate.tmp_parquet_path.c_str());
		throw IOException(e.what());
	}
	std::remove(gstate.tmp_parquet_path.c_str());
}

static CopyFunction GetOpenzlCopyFunction() {
	CopyFunction function("openzl");
	function.copy_to_bind = OpenzlCopyBind;
	function.copy_to_initialize_global = OpenzlCopyInitGlobal;
	function.copy_to_initialize_local = OpenzlCopyInitLocal;
	function.copy_to_sink = OpenzlCopySink;
	function.copy_to_combine = OpenzlCopyCombine;
	function.copy_to_finalize = OpenzlCopyFinalize;
	function.extension = "zl";
	return function;
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(ScalarFunction("openzl_decompress", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                        LogicalType::VARCHAR, OpenzlDecompressFun));

	ScalarFunctionSet openzl_compress_set("openzl_compress");
	openzl_compress_set.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, OpenzlCompressFun));

	// The 3/4-arg overloads below all take SPECIAL_HANDLING: DuckDB's default
	// null handling makes a scalar function itself a no-op returning NULL
	// (never invoking our code at all) if ANY argument is NULL, which would
	// silently skip compression entirely instead of falling back to the
	// generic graph ('' or NULL, per the doc comments below) or erroring
	// (a NULL trained_compressor_bytes blob) the way we actually want.
	ScalarFunction compress_trained_path({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                      LogicalType::VARCHAR, OpenzlCompressWithOptionsFun);
	compress_trained_path.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	openzl_compress_set.AddFunction(compress_trained_path);

	ScalarFunction compress_trained_path_level(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR,
	    OpenzlCompressWithOptionsFun);
	compress_trained_path_level.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	openzl_compress_set.AddFunction(compress_trained_path_level);

	// BLOB overloads: trained compressor as in-memory bytes (e.g. from a
	// table column) instead of a file path.
	ScalarFunction compress_trained_blob({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BLOB},
	                                      LogicalType::VARCHAR, OpenzlCompressWithBlobOptionsFun);
	compress_trained_blob.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	openzl_compress_set.AddFunction(compress_trained_blob);

	ScalarFunction compress_trained_blob_level(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BLOB, LogicalType::BIGINT}, LogicalType::VARCHAR,
	    OpenzlCompressWithBlobOptionsFun);
	compress_trained_blob_level.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	openzl_compress_set.AddFunction(compress_trained_blob_level);
	loader.RegisterFunction(openzl_compress_set);

	// Registers the "openzl://" scheme (see openzl_file_system.hpp) that
	// read_openzl's macro body opens -- same mechanism httpfs uses for
	// "s3://"/"https://".
	loader.GetDatabaseInstance().GetFileSystem().RegisterSubSystem(make_uniq<OpenzlFileSystem>());

	auto read_openzl_info = DefaultTableFunctionGenerator::CreateTableMacroInfo(OpenzlReadMacro);
	loader.RegisterFunction(*read_openzl_info);

	loader.RegisterFunction(GetOpenzlCopyFunction());
	loader.RegisterFunction(GetOpenzlTrainFunction());
}

void OpenzlExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string OpenzlExtension::Name() {
	return "openzl";
}

std::string OpenzlExtension::Version() const {
#ifdef EXT_VERSION_OPENZL
	return EXT_VERSION_OPENZL;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(openzl, loader) {
	duckdb::LoadInternal(loader);
}
}
