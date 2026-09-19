#include "openzl_bridge.hpp"

#include <array>
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <sys/stat.h>
#include <sys/wait.h>

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
	if (!IsExecutable(zli)) {
		throw Error("openzl_bridge: zli binary not found or not executable: " + zli +
		            " (set OPENZL_ZLI_BIN)");
	}

	std::string compress_command = ShellQuote(zli) + " compress " + ShellQuote(input_parquet_path) +
	                                " --profile parquet -o " + ShellQuote(output_zl_path) + " -f";
	RunOrThrow(compress_command, "compress " + input_parquet_path);

	if (!FileExists(output_zl_path)) {
		throw Error("openzl_bridge: compress reported success but output is missing: " + output_zl_path);
	}
}

} // namespace openzl_bridge
