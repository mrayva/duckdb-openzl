#include "openzl_file_system.hpp"

#include <algorithm>
#include <cstring>
#include <fstream>

#include <atomic>
#include <mutex>
#include <unordered_map>
#ifdef _WIN32
#include <sys/stat.h>
#else
#include <sys/stat.h>
#endif

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
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
                                   std::shared_ptr<const string> decompressed_bytes)
    : FileHandle(file_system, std::move(path), flags), data(std::move(decompressed_bytes)) {
}

namespace {
// One decompressed buffer per archive, shared by every concurrently-open
// handle on it. Entries are weak: the buffer lives exactly as long as some
// handle (or scan) holds it, then is freed -- there's no cross-query cache.
// Keyed by the archive's size+mtime too, so a rewritten archive is never
// served from a stale buffer that an old handle happens to still hold.
struct ArchiveCacheEntry {
	std::weak_ptr<const string> data;
	int64_t size = -1;
	int64_t mtime = -1;
};
std::mutex &ArchiveCacheMutex() {
	static std::mutex m;
	return m;
}
std::unordered_map<string, ArchiveCacheEntry> &ArchiveCache() {
	static std::unordered_map<string, ArchiveCacheEntry> cache;
	return cache;
}
bool StatArchive(const string &path, int64_t &size, int64_t &mtime) {
#ifdef _WIN32
	struct _stat64 st;
	if (_stat64(path.c_str(), &st) != 0) {
		return false;
	}
#else
	struct stat st;
	if (stat(path.c_str(), &st) != 0) {
		return false;
	}
#endif
	size = static_cast<int64_t>(st.st_size);
	mtime = static_cast<int64_t>(st.st_mtime);
	return true;
}
} // namespace

string OpenzlFileSystem::StripScheme(const string &fpath) {
	return fpath.substr(SCHEME_LEN);
}

namespace {
// "<archive>#chunk=N" addresses chunk N of a multi-chunk archive. Glob()
// expands a chunked archive into one such path per chunk, so read_parquet
// scans the chunks as ordinary separate files. Returns false (and leaves the
// outputs untouched) if `real_path` has no such suffix.
bool SplitChunkSuffix(const string &real_path, string &archive, idx_t &chunk) {
	static const string marker = "#chunk=";
	auto pos = real_path.rfind(marker);
	if (pos == string::npos) {
		return false;
	}
	auto digits = real_path.substr(pos + marker.size());
	if (digits.empty() || digits.size() > 15 || digits.find_first_not_of("0123456789") != string::npos) {
		return false;
	}
	archive = real_path.substr(0, pos);
	chunk = static_cast<idx_t>(std::stoull(digits));
	return true;
}
} // namespace

bool OpenzlFileSystem::CanHandleFile(const string &fpath) {
	return fpath.compare(0, SCHEME_LEN, SCHEME) == 0;
}

unique_ptr<FileHandle> OpenzlFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                  optional_ptr<FileOpener> opener) {
	string real_path = StripScheme(path);
	string archive_path = real_path;
	idx_t chunk = 0;
	bool is_chunk = SplitChunkSuffix(real_path, archive_path, chunk);
	if (flags.OpenForWriting()) {
		if (is_chunk || flags.OpenForAppending() || flags.OpenForReading()) {
			throw NotImplementedException("OpenzlFileSystem: \"%s\" can only be opened write-only to create a new "
			                              "archive (no append, no read+write, no single-chunk paths)",
			                              path);
		}
		auto handle = make_uniq<OpenzlFileHandle>(*this, path, flags, nullptr);
		handle->write_buffer = std::make_shared<string>();
		handle->real_path = real_path;
		auto &s = handle->write_settings;
		Value v;
		if (FileOpener::TryGetCurrentSetting(opener, "openzl_compression_level", v) && !v.IsNull()) {
			s.compression_level = static_cast<int>(v.GetValue<int64_t>());
		}
		if (FileOpener::TryGetCurrentSetting(opener, "openzl_max_compress_bytes", v) && !v.IsNull()) {
			s.max_compress_bytes = static_cast<size_t>(v.GetValue<uint64_t>());
		}
		if (FileOpener::TryGetCurrentSetting(opener, "openzl_format_version", v) && !v.IsNull()) {
			s.graph.format_version = static_cast<int>(v.GetValue<int64_t>());
		}
		if (FileOpener::TryGetCurrentSetting(opener, "openzl_profile", v) && !v.IsNull()) {
			s.graph.profile = v.ToString();
		}
		if (FileOpener::TryGetCurrentSetting(opener, "openzl_permissive_compression", v) && !v.IsNull()) {
			s.graph.permissive = v.GetValue<bool>() ? 1 : 0;
		}
		if (FileOpener::TryGetCurrentSetting(opener, "openzl_parquet_chunk_bytes", v) && !v.IsNull()) {
			auto bytes = v.GetValue<int64_t>();
			s.graph.parquet_chunk_bytes = bytes < 0 ? openzl_bridge::kAutoParquetChunkBytes : static_cast<size_t>(bytes);
		}
		return std::move(handle);
	}
	int64_t size = -1, mtime = -1;
	StatArchive(archive_path, size, mtime);

	// Held across the decompression on purpose: when DuckDB opens one handle
	// per scan thread at once, the first decompresses and the rest wait and
	// then share its buffer, instead of every thread decompressing its own.
	std::lock_guard<std::mutex> lock(ArchiveCacheMutex());
	auto &entry = ArchiveCache()[real_path];
	std::shared_ptr<const string> data = entry.data.lock();
	if (!data || entry.size != size || entry.mtime != mtime) {
		try {
			data = std::make_shared<const string>(is_chunk ? openzl_bridge::DecompressChunkToBuffer(archive_path, chunk)
			                                               : openzl_bridge::DecompressToBuffer(archive_path));
		} catch (const openzl_bridge::Error &e) {
			throw IOException("OpenzlFileSystem: %s", e.what());
		}
		entry.data = data;
		entry.size = size;
		entry.mtime = mtime;
	}
	return make_uniq<OpenzlFileHandle>(*this, path, flags, std::move(data));
}

void OpenzlFileHandle::Close() {
	if (!write_buffer || closed) {
		return;
	}
	closed = true;
	// Compress next to the destination and rename into place, so a reader never sees a half-written archive.
	const string tmp_path = real_path + ".openzl_tmp";
	try {
		try {
			openzl_bridge::CompressParquetBytes(*write_buffer, tmp_path, string(), write_settings.compression_level,
			                                    write_settings.max_compress_bytes, write_settings.graph);
		} catch (const openzl_bridge::Error &) {
			// Hosts write some small side files that are not canonical parquet (DuckLake's delete files): store those
			// with the generic graph rather than failing the write. A LARGE non-canonical file is still an error --
			// silently storing it generically would cost most of OpenZL's gain and hide a misconfiguration.
			constexpr size_t kSmallFallbackBytes = 64000000ULL;
			if (write_settings.graph.profile == "serial" || write_buffer->size() > kSmallFallbackBytes) {
				throw;
			}
			auto generic = write_settings.graph;
			generic.profile = "serial";
			generic.parquet_chunk_bytes = 0;
			openzl_bridge::CompressParquetBytes(*write_buffer, tmp_path, string(), write_settings.compression_level,
			                                    write_settings.max_compress_bytes, generic);
		}
	} catch (const openzl_bridge::Error &e) {
		std::remove(tmp_path.c_str());
		throw IOException("OpenzlFileSystem: cannot write \"%s\": %s (files written through openzl:// must be canonical "
		                  "parquet -- uncompressed, plain-encoded, no dictionary -- unless SET openzl_profile = 'serial')",
		                  GetPath(), e.what());
	}
	write_buffer.reset();
	if (std::rename(tmp_path.c_str(), real_path.c_str()) != 0) {
		std::remove(tmp_path.c_str());
		throw IOException("OpenzlFileSystem: cannot move the finished archive into place at \"%s\"", real_path);
	}
}

void OpenzlFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &h = handle.Cast<OpenzlFileHandle>();
	auto &bytes = h.Bytes();
	if (location + nr_bytes > bytes.size()) {
		throw IOException("OpenzlFileSystem: attempted to read past the end of decompressed archive \"%s\"",
		                  handle.GetPath());
	}
	memcpy(buffer, bytes.data() + location, static_cast<size_t>(nr_bytes));
}

int64_t OpenzlFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &h = handle.Cast<OpenzlFileHandle>();
	auto &bytes = h.Bytes();
	idx_t remaining = bytes.size() > h.position ? bytes.size() - h.position : 0;
	idx_t to_read = std::min<idx_t>(remaining, static_cast<idx_t>(nr_bytes));
	if (to_read > 0) {
		memcpy(buffer, bytes.data() + h.position, to_read);
		h.position += to_read;
	}
	return static_cast<int64_t>(to_read);
}

void OpenzlFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &h = handle.Cast<OpenzlFileHandle>();
	if (!h.write_buffer) {
		throw IOException("OpenzlFileSystem: \"%s\" was not opened for writing", handle.GetPath());
	}
	if (location + nr_bytes > h.write_buffer->size()) {
		h.write_buffer->resize(location + static_cast<idx_t>(nr_bytes));
	}
	memcpy(&(*h.write_buffer)[location], buffer, static_cast<size_t>(nr_bytes));
}

int64_t OpenzlFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &h = handle.Cast<OpenzlFileHandle>();
	Write(handle, buffer, nr_bytes, h.position);
	h.position += static_cast<idx_t>(nr_bytes);
	return nr_bytes;
}

void OpenzlFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	auto &h = handle.Cast<OpenzlFileHandle>();
	if (!h.write_buffer) {
		throw IOException("OpenzlFileSystem: \"%s\" was not opened for writing", handle.GetPath());
	}
	h.write_buffer->resize(static_cast<size_t>(new_size));
}

namespace {
FileSystem &LocalFs() {
	static auto fs = FileSystem::CreateLocal();
	return *fs;
}
} // namespace

void OpenzlFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	LocalFs().CreateDirectory(StripScheme(directory), opener);
}

bool OpenzlFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	return CanHandleFile(directory) && LocalFs().DirectoryExists(StripScheme(directory), opener);
}

void OpenzlFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	LocalFs().RemoveDirectory(StripScheme(directory), opener);
}

void OpenzlFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	LocalFs().MoveFile(CanHandleFile(source) ? StripScheme(source) : source,
	                   CanHandleFile(target) ? StripScheme(target) : target, opener);
}

int64_t OpenzlFileSystem::GetFileSize(FileHandle &handle) {
	return static_cast<int64_t>(handle.Cast<OpenzlFileHandle>().Bytes().size());
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
	string archive_path = StripScheme(filename);
	idx_t chunk = 0;
	SplitChunkSuffix(StripScheme(filename), archive_path, chunk);
	// A directory also opens fine as an ifstream on Linux, so check for a regular file explicitly.
	return LocalFs().FileExists(archive_path, opener);
}

bool OpenzlFileSystem::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	if (!CanHandleFile(filename)) {
		return false;
	}
	string archive_path = StripScheme(filename);
	idx_t chunk = 0;
	if (SplitChunkSuffix(archive_path, archive_path, chunk)) {
		throw IOException("OpenzlFileSystem: cannot remove \"%s\": it is one chunk of a multi-chunk archive; remove the "
		                  "whole archive instead",
		                  filename);
	}
	return std::remove(archive_path.c_str()) == 0;
}

void OpenzlFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	if (!TryRemoveFile(filename, opener)) {
		throw IOException("OpenzlFileSystem: could not remove \"%s\"", filename);
	}
}

vector<OpenFileInfo> OpenzlFileSystem::Glob(const string &path, FileOpener *opener) {
	// openzl:// paths name one archive each -- no glob patterns supported.
	// read_parquet() calls Glob() to expand its input before opening files: a
	// plain archive expands to itself, a multi-chunk archive to one
	// "<path>#chunk=N" per chunk (each decompressed independently on open, so
	// memory is bounded by the chunk size, not the table size).
	string archive_path = StripScheme(path);
	idx_t chunk = 0;
	if (SplitChunkSuffix(archive_path, archive_path, chunk)) {
		return {OpenFileInfo(path)};
	}
	try {
		std::vector<openzl_bridge::ChunkRange> ranges;
		if (std::ifstream(archive_path, std::ios::binary).good() &&
		    openzl_bridge::ReadContainerIndex(archive_path, ranges)) {
			vector<OpenFileInfo> result;
			for (idx_t i = 0; i < ranges.size(); i++) {
				result.emplace_back(path + "#chunk=" + std::to_string(i));
			}
			return result;
		}
	} catch (const openzl_bridge::Error &e) {
		throw IOException("OpenzlFileSystem: %s", e.what());
	}
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
	static std::atomic<uint64_t> counter {0};
	return string(BUFFER_SCHEME) + std::to_string(counter.fetch_add(1, std::memory_order_relaxed));
}

size_t OpenzlBufferFileSystem::PeekSize(const string &path) {
	std::lock_guard<std::mutex> lock(Mutex());
	auto it = Buffers().find(path);
	return it == Buffers().end() ? 0 : it->second->size();
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
		throw IOException("OpenzlBufferFileSystem: attempted to read past the end of buffer \"%s\"", handle.GetPath());
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
