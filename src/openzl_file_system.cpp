#include "openzl_file_system.hpp"

#include <algorithm>
#include <cstring>
#include <fstream>

#include <atomic>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include "openzl_bridge.hpp"

namespace duckdb {

namespace {
constexpr const char *SCHEME = "openzl://";
constexpr size_t SCHEME_LEN = 9; // strlen("openzl://")

constexpr const char *BUFFER_SCHEME = "openzl-buffer://";
constexpr size_t BUFFER_SCHEME_LEN = 16; // strlen("openzl-buffer://")
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

// ---- OpenzlBufferFileSystem: writable in-memory staging buffers ----

OpenzlBufferFileHandle::OpenzlBufferFileHandle(FileSystem &file_system, string path, FileOpenFlags flags,
                                                std::shared_ptr<string> buffer_p)
    : FileHandle(file_system, std::move(path), flags), buffer(std::move(buffer_p)) {
}

std::mutex &OpenzlBufferFileSystem::Mutex() {
	static std::mutex mutex;
	return mutex;
}

std::unordered_map<string, std::shared_ptr<string>> &OpenzlBufferFileSystem::Buffers() {
	static std::unordered_map<string, std::shared_ptr<string>> buffers;
	return buffers;
}

string OpenzlBufferFileSystem::GenerateUniquePath() {
	static std::atomic<uint64_t> counter{0};
	return string(BUFFER_SCHEME) + std::to_string(counter.fetch_add(1, std::memory_order_relaxed));
}

string OpenzlBufferFileSystem::TakeBuffer(const string &path) {
	std::lock_guard<std::mutex> lock(Mutex());
	auto it = Buffers().find(path);
	if (it == Buffers().end()) {
		throw InternalException("OpenzlBufferFileSystem: no buffer registered for \"%s\"", path);
	}
	string result = std::move(*it->second);
	Buffers().erase(it);
	return result;
}

bool OpenzlBufferFileSystem::CanHandleFile(const string &fpath) {
	return fpath.compare(0, BUFFER_SCHEME_LEN, BUFFER_SCHEME) == 0;
}

unique_ptr<FileHandle> OpenzlBufferFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                         optional_ptr<FileOpener> opener) {
	std::shared_ptr<string> buffer;
	{
		std::lock_guard<std::mutex> lock(Mutex());
		auto &buffers = Buffers();
		auto it = buffers.find(path);
		if (it != buffers.end()) {
			buffer = it->second;
		} else {
			buffer = std::make_shared<string>();
			buffers.emplace(path, buffer);
		}
	}
	return make_uniq<OpenzlBufferFileHandle>(*this, path, flags, std::move(buffer));
}

void OpenzlBufferFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &h = handle.Cast<OpenzlBufferFileHandle>();
	if (location + nr_bytes > h.buffer->size()) {
		h.buffer->resize(location + nr_bytes);
	}
	memcpy(&(*h.buffer)[location], buffer, static_cast<size_t>(nr_bytes));
}

int64_t OpenzlBufferFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &h = handle.Cast<OpenzlBufferFileHandle>();
	Write(handle, buffer, nr_bytes, h.position);
	h.position += nr_bytes;
	return nr_bytes;
}

void OpenzlBufferFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &h = handle.Cast<OpenzlBufferFileHandle>();
	if (location + nr_bytes > h.buffer->size()) {
		throw IOException("OpenzlBufferFileSystem: attempted to read past the end of buffer \"%s\"",
		                   handle.GetPath());
	}
	memcpy(buffer, h.buffer->data() + location, static_cast<size_t>(nr_bytes));
}

int64_t OpenzlBufferFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &h = handle.Cast<OpenzlBufferFileHandle>();
	idx_t remaining = h.buffer->size() > h.position ? h.buffer->size() - h.position : 0;
	idx_t to_read = std::min<idx_t>(remaining, static_cast<idx_t>(nr_bytes));
	if (to_read > 0) {
		memcpy(buffer, h.buffer->data() + h.position, to_read);
		h.position += to_read;
	}
	return static_cast<int64_t>(to_read);
}

int64_t OpenzlBufferFileSystem::GetFileSize(FileHandle &handle) {
	return static_cast<int64_t>(handle.Cast<OpenzlBufferFileHandle>().buffer->size());
}

timestamp_t OpenzlBufferFileSystem::GetLastModifiedTime(FileHandle &handle) {
	return Timestamp::GetCurrentTimestamp();
}

void OpenzlBufferFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	handle.Cast<OpenzlBufferFileHandle>().buffer->resize(static_cast<size_t>(new_size));
}

void OpenzlBufferFileSystem::Seek(FileHandle &handle, idx_t location) {
	handle.Cast<OpenzlBufferFileHandle>().position = location;
}

void OpenzlBufferFileSystem::Reset(FileHandle &handle) {
	handle.Cast<OpenzlBufferFileHandle>().position = 0;
}

idx_t OpenzlBufferFileSystem::SeekPosition(FileHandle &handle) {
	return handle.Cast<OpenzlBufferFileHandle>().position;
}

bool OpenzlBufferFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	if (!CanHandleFile(filename)) {
		return false;
	}
	std::lock_guard<std::mutex> lock(Mutex());
	return Buffers().find(filename) != Buffers().end();
}

} // namespace duckdb
