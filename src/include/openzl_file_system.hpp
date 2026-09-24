// Copyright (c) 2026.
//
// A virtual DuckDB filesystem for the "openzl://" scheme: opening
// "openzl://<path-to-archive>.zl" decompresses that archive into memory
// (openzl_bridge::DecompressToBuffer) and serves reads directly from that
// buffer -- no intermediate .parquet file ever touches disk. This is what
// read_openzl() is built on; see openzl_extension.cpp.
//
// Registered the same way extensions like httpfs register "s3://"/"https://":
// FileSystem::RegisterSubSystem on the database's top-level (virtual)
// filesystem, which dispatches to us by CanHandleFile/prefix match.
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "duckdb/common/file_system.hpp"

namespace duckdb {

class OpenzlFileHandle : public FileHandle {
public:
	OpenzlFileHandle(FileSystem &file_system, string path, FileOpenFlags flags,
	                 std::shared_ptr<const string> decompressed_bytes);

	void Close() override {
	}

	// Shared with every other open handle on the same archive (see
	// OpenzlFileSystem::OpenFile): DuckDB opens one handle per scan thread,
	// and a private copy per handle multiplied RAM by the thread count.
	std::shared_ptr<const string> data;
	idx_t position = 0;
};

class OpenzlFileSystem : public FileSystem {
public:
	std::string GetName() const override {
		return "OpenzlFileSystem";
	}

	bool CanHandleFile(const string &fpath) override;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                 optional_ptr<FileOpener> opener = nullptr) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	int64_t GetFileSize(FileHandle &handle) override;
	// The buffer is decompressed fresh whenever no handle is holding one, so
	// there's no on-disk staleness to report: this (and GetVersionTag() below)
	// just report "always current" rather than tracking the archive's mtime.
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	string GetVersionTag(FileHandle &handle) override {
		return string();
	}
	void Seek(FileHandle &handle, idx_t location) override;
	void Reset(FileHandle &handle) override;
	idx_t SeekPosition(FileHandle &handle) override;
	bool CanSeek() override {
		return true;
	}
	// A decompressed-in-memory buffer has no seek penalty -- at least as
	// cheap as a real on-disk file, unlike e.g. a network file.
	bool OnDiskFile(FileHandle &handle) override {
		return true;
	}

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

	// Strips the "openzl://" prefix to recover the real path to the .zl
	// archive on the actual filesystem.
	static string StripScheme(const string &fpath);
};

// A writable, in-memory-only filesystem for the "openzl-buffer://" scheme:
// used as the staging target for COPY ... FORMAT OPENZL's IN_MEMORY option,
// so DuckDB's own parquet writer (which opens its output via
// FileSystem::GetFileSystem(context) -- the database's normal dispatching
// filesystem -- same as any real file) writes the canonical parquet bytes
// straight into a std::string instead of a temp file on disk. Nothing about
// the parquet writer needs to change: it just thinks it opened a file.
//
// Buffers are held in a process-wide registry keyed by path (there's no
// FileHandle-level way for the caller that chose the path to get the bytes
// back later -- copy_to_finalize only has the path string, not our
// FileHandle object) until explicitly reclaimed via TakeBuffer(), which the
// OPENZL copy function's finalize step calls once parquet's own writer has
// closed the file. GenerateUniquePath() ensures concurrent COPYs (even
// across connections) never collide on the same key.
class OpenzlBufferFileHandle : public FileHandle {
public:
	OpenzlBufferFileHandle(FileSystem &file_system, string path, FileOpenFlags flags,
	                        std::shared_ptr<string> buffer);

	void Close() override {
	}

	// Points at the same object held in OpenzlBufferFileSystem's registry
	// (keyed by path), so writes here are what TakeBuffer() sees after this
	// handle is closed and destroyed.
	std::shared_ptr<string> buffer;
	idx_t position = 0;
};

class OpenzlBufferFileSystem : public FileSystem {
public:
	std::string GetName() const override {
		return "OpenzlBufferFileSystem";
	}

	bool CanHandleFile(const string &fpath) override;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                 optional_ptr<FileOpener> opener = nullptr) override;

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	void Seek(FileHandle &handle, idx_t location) override;
	void Reset(FileHandle &handle) override;
	idx_t SeekPosition(FileHandle &handle) override;
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return true;
	}
	void FileSync(FileHandle &handle) override {
	}

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

	// A fresh, never-before-used "openzl-buffer://..." path, for a caller
	// about to open+write a new staging buffer.
	static string GenerateUniquePath();
	// Removes and returns the accumulated bytes for `path`. Throws if no
	// such buffer exists (e.g. called twice, or before OpenFile was ever
	// called for it).
	static string TakeBuffer(const string &path);
	// Current size in bytes of the buffer at `path` (0 if none).
	static size_t PeekSize(const string &path);

private:
	static std::mutex &Mutex();
	static std::unordered_map<string, std::shared_ptr<string>> &Buffers();
};

} // namespace duckdb
