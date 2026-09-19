#define DUCKDB_EXTENSION_MAIN

#include "openzl_extension.hpp"
#include "openzl_bridge.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/scalar_function.hpp"
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

// read_openzl(path) -> table
//
// Single-call replacement for read_parquet(openzl_decompress(...)): decompresses
// the archive to a sibling .parquet file (stripping a trailing .zl, or just
// appending .parquet if there isn't one) and reads it back. Typical usage:
//   SELECT * FROM read_openzl('data.zl');
//
// The decompressed .parquet file is left on disk at a deterministic path (not
// cleaned up, and not uniquely named per call): re-running the same query
// reuses/overwrites it rather than accumulating temp files, which matters for
// this project given how often disk space has been the actual constraint.
// That means concurrent read_openzl() calls against the *same* archive path
// can race on the same sibling file -- fine for the single-user/analytical
// use this extension targets, but worth knowing.
static const DefaultTableMacro OpenzlReadMacro = {
    DEFAULT_SCHEMA, "read_openzl", {"path", nullptr}, {{nullptr, nullptr}},
    R"(SELECT * FROM read_parquet(openzl_decompress(path, regexp_replace(path, '\.zl$', '') || '.parquet')))"};

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

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<OpenzlCopyBindData>();
		result->parquet_copy_function = parquet_copy_function;
		result->parquet_bind_data = parquet_bind_data->Copy();
		return std::move(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<OpenzlCopyBindData>();
		return parquet_bind_data->Equals(*other.parquet_bind_data);
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
		openzl_bridge::CompressParquet(gstate.tmp_parquet_path, gstate.final_zl_path);
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

	loader.RegisterFunction(ScalarFunction("openzl_compress", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                        LogicalType::VARCHAR, OpenzlCompressFun));

	auto read_openzl_info = DefaultTableFunctionGenerator::CreateTableMacroInfo(OpenzlReadMacro);
	loader.RegisterFunction(*read_openzl_info);

	loader.RegisterFunction(GetOpenzlCopyFunction());
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
