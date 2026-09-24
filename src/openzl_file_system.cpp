#include "openzl_file_system.hpp"

#include <algorithm>
#include <cstring>
#include <fstream>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "openzl_bridge.hpp"

namespace duckdb {

namespace {
constexpr const char *SCHEME = "openzl://";
constexpr size_t SCHEME_LEN = 9; // strlen("openzl://")
} // namespace

OpenzlFileHandle::OpenzlFileHandle(FileSystem &file_system, string path, FileOpenFlags flags,
                                    string decompressed_bytes)
    : FileHandle(file_system, std::move(path), flags), data(std::move(decompressed_bytes)) {
}

string OpenzlFileSystem::StripScheme(const string &fpath) {
	return fpath.substr(SCHEME_LEN);
}

bool OpenzlFileSystem::CanHandleFile(const string &fpath) {
	return fpath.compare(0, SCHEME_LEN, SCHEME) == 0;
}

unique_ptr<FileHandle> OpenzlFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                   optional_ptr<FileOpener> opener) {
	if (flags.OpenForWriting()) {
		throw NotImplementedException("OpenzlFileSystem: \"%s\" is read-only (openzl:// archives can't be written to "
		                               "directly -- use COPY ... FORMAT OPENZL or openzl_compress instead)",
		                               path);
	}
	string real_path = StripScheme(path);
	string decompressed;
	try {
		decompressed = openzl_bridge::DecompressToBuffer(real_path);
	} catch (const openzl_bridge::Error &e) {
		throw IOException("OpenzlFileSystem: %s", e.what());
	}
	return make_uniq<OpenzlFileHandle>(*this, path, flags, std::move(decompressed));
}

void OpenzlFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &h = handle.Cast<OpenzlFileHandle>();
	if (location + nr_bytes > h.data.size()) {
		throw IOException("OpenzlFileSystem: attempted to read past the end of decompressed archive \"%s\"",
		                   handle.GetPath());
	}
	memcpy(buffer, h.data.data() + location, static_cast<size_t>(nr_bytes));
}

int64_t OpenzlFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &h = handle.Cast<OpenzlFileHandle>();
	idx_t remaining = h.data.size() > h.position ? h.data.size() - h.position : 0;
	idx_t to_read = std::min<idx_t>(remaining, static_cast<idx_t>(nr_bytes));
	if (to_read > 0) {
		memcpy(buffer, h.data.data() + h.position, to_read);
		h.position += to_read;
	}
	return static_cast<int64_t>(to_read);
}

int64_t OpenzlFileSystem::GetFileSize(FileHandle &handle) {
	return static_cast<int64_t>(handle.Cast<OpenzlFileHandle>().data.size());
}

timestamp_t OpenzlFileSystem::GetLastModifiedTime(FileHandle &handle) {
	return Timestamp::GetCurrentTimestamp();
}

void OpenzlFileSystem::Seek(FileHandle &handle, idx_t location) {
	handle.Cast<OpenzlFileHandle>().position = location;
}

void OpenzlFileSystem::Reset(FileHandle &handle) {
	handle.Cast<OpenzlFileHandle>().position = 0;
}

idx_t OpenzlFileSystem::SeekPosition(FileHandle &handle) {
	return handle.Cast<OpenzlFileHandle>().position;
}

bool OpenzlFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	if (!CanHandleFile(filename)) {
		return false;
	}
	// Cheap existence check on the real archive, without paying for a full
	// decompress -- OpenFile() (called for the real read) will surface a
	// clear decompression error separately if the archive is corrupt.
	std::ifstream f(StripScheme(filename), std::ios::binary);
	return f.good();
}

vector<OpenFileInfo> OpenzlFileSystem::Glob(const string &path, FileOpener *opener) {
	// openzl:// paths name one archive each -- no glob patterns supported,
	// same as there being exactly one .zl file per COPY ... FORMAT OPENZL
	// output. read_parquet() calls Glob() to expand its input before opening
	// files, so this just needs to hand the single path back.
	return {OpenFileInfo(path)};
}

} // namespace duckdb
