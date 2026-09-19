#include "openzl_bridge.hpp"

#include <array>
#include <cstdio>
#include <cstdlib>
#include <dirent.h>
#include <sstream>
#include <sys/stat.h>
#include <sys/wait.h>
#include <vector>

namespace openzl_bridge {

namespace {

std::string GetEnvOr(const char *name, const std::string &fallback) {
	const char *value = std::getenv(name);
	if (value != nullptr && value[0] != '\0') {
		return std::string(value);
	}
	return fallback;
}

std::string HomeDir() {
	return GetEnvOr("HOME", "/root");
}

bool FileExists(const std::string &path) {
	struct stat st{};
	return ::stat(path.c_str(), &st) == 0;
}

bool IsExecutable(const std::string &path) {
	struct stat st{};
	if (::stat(path.c_str(), &st) != 0) {
		return false;
	}
	return (st.st_mode & S_IXUSR) != 0;
}

// Runs `command`, capturing combined stdout+stderr for error reporting.
// Throws openzl_bridge::Error if the command exits non-zero.
void RunOrThrow(const std::string &command, const std::string &action_description) {
	std::string full_command = command + " 2>&1";
	std::array<char, 4096> buffer{};
	std::ostringstream output;

	FILE *pipe = popen(full_command.c_str(), "r");
	if (pipe == nullptr) {
		throw Error("Failed to launch subprocess for: " + action_description);
	}
	size_t bytes_read;
	while ((bytes_read = fread(buffer.data(), 1, buffer.size(), pipe)) > 0) {
		output.write(buffer.data(), static_cast<std::streamsize>(bytes_read));
	}
	int status = pclose(pipe);
	if (status != 0) {
		std::ostringstream msg;
		msg << "openzl_bridge: " << action_description << " failed (exit status " << status << "): " << command
		    << "\n"
		    << output.str();
		throw Error(msg.str());
	}
}

// Finds the single file directly inside `dir` (make_canonical_parquet writes
// exactly one output file per input). Throws if none is found.
std::string FindSingleFileIn(const std::string &dir) {
	DIR *handle = opendir(dir.c_str());
	if (handle == nullptr) {
		throw Error("openzl_bridge: could not open directory: " + dir);
	}
	std::string found;
	struct dirent *entry;
	while ((entry = readdir(handle)) != nullptr) {
		std::string name = entry->d_name;
		if (name == "." || name == "..") {
			continue;
		}
		std::string full_path = dir + "/" + name;
		struct stat st{};
		if (::stat(full_path.c_str(), &st) == 0 && S_ISREG(st.st_mode)) {
			found = full_path;
			break;
		}
	}
	closedir(handle);
	if (found.empty()) {
		throw Error("openzl_bridge: canonicalize produced no output file in: " + dir);
	}
	return found;
}

std::string MakeTempDir(const std::string &prefix) {
	std::string tmpl = prefix + "XXXXXX";
	std::vector<char> buf(tmpl.begin(), tmpl.end());
	buf.push_back('\0');
	if (mkdtemp(buf.data()) == nullptr) {
		throw Error("openzl_bridge: failed to create temp directory from template: " + tmpl);
	}
	return std::string(buf.data());
}

std::string ShellQuote(const std::string &s) {
	std::string out = "'";
	for (char c : s) {
		if (c == '\'') {
			out += "'\\''";
		} else {
			out += c;
		}
	}
	out += "'";
	return out;
}

} // namespace

std::string ZliBinPath() {
	return GetEnvOr("OPENZL_ZLI_BIN", HomeDir() + "/openzl/zli");
}

std::string MakeCanonicalParquetBinPath() {
	return GetEnvOr("OPENZL_MAKE_CANONICAL_PARQUET_BIN", HomeDir() + "/openzl/build_parquet/tools/parquet/make_canonical_parquet");
}

void Decompress(const std::string &input_path, const std::string &output_path) {
	if (!FileExists(input_path)) {
		throw Error("openzl_bridge: input file not found: " + input_path);
	}
	std::string zli = ZliBinPath();
	if (!IsExecutable(zli)) {
		throw Error("openzl_bridge: zli binary not found or not executable: " + zli +
		            " (set OPENZL_ZLI_BIN)");
	}

	std::string command = ShellQuote(zli) + " decompress " + ShellQuote(input_path) + " -o " +
	                       ShellQuote(output_path) + " -f";
	RunOrThrow(command, "decompress " + input_path);

	if (!FileExists(output_path)) {
		throw Error("openzl_bridge: decompress reported success but output is missing: " + output_path);
	}
}

void CompressParquet(const std::string &input_parquet_path, const std::string &output_zl_path) {
	if (!FileExists(input_parquet_path)) {
		throw Error("openzl_bridge: input parquet file not found: " + input_parquet_path);
	}
	std::string zli = ZliBinPath();
	std::string make_canonical = MakeCanonicalParquetBinPath();
	if (!IsExecutable(zli)) {
		throw Error("openzl_bridge: zli binary not found or not executable: " + zli +
		            " (set OPENZL_ZLI_BIN)");
	}
	if (!IsExecutable(make_canonical)) {
		throw Error("openzl_bridge: make_canonical_parquet binary not found or not executable: " + make_canonical +
		            " (set OPENZL_MAKE_CANONICAL_PARQUET_BIN)");
	}

	std::string canon_dir = MakeTempDir("/tmp/openzl_bridge_canon.");

	std::string canon_command = ShellQuote(make_canonical) + " --input " + ShellQuote(input_parquet_path) +
	                             " --output " + ShellQuote(canon_dir);
	try {
		RunOrThrow(canon_command, "canonicalize " + input_parquet_path);
		std::string canonical_file = FindSingleFileIn(canon_dir);

		std::string compress_command = ShellQuote(zli) + " compress " + ShellQuote(canonical_file) +
		                                " --profile parquet -o " + ShellQuote(output_zl_path) + " -f";
		RunOrThrow(compress_command, "compress " + input_parquet_path);
	} catch (...) {
		std::string cleanup = "rm -rf " + ShellQuote(canon_dir);
		std::system(cleanup.c_str());
		throw;
	}
	std::string cleanup = "rm -rf " + ShellQuote(canon_dir);
	std::system(cleanup.c_str());

	if (!FileExists(output_zl_path)) {
		throw Error("openzl_bridge: compress reported success but output is missing: " + output_zl_path);
	}
}

} // namespace openzl_bridge
