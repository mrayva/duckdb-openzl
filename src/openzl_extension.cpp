#define DUCKDB_EXTENSION_MAIN

#include "openzl_extension.hpp"
#include "openzl_bridge.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"

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

static void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(ScalarFunction("openzl_decompress", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                        LogicalType::VARCHAR, OpenzlDecompressFun));

	loader.RegisterFunction(ScalarFunction("openzl_compress", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                        LogicalType::VARCHAR, OpenzlCompressFun));
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
