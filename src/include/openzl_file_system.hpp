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

#include "duckdb/common/file_system.hpp"

namespace duckdb {

class OpenzlFileHandle : public FileHandle {
public:
	OpenzlFileHandle(FileSystem &file_system, string path, FileOpenFlags flags, string decompressed_bytes);

	void Close() override {
	}

	string data;
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
	// Each OpenFile() call decompresses fresh -- there's no on-disk staleness
	// to report, so this (and GetVersionTag() below) just report "always
	// current" rather than tracking the real archive file's mtime.
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

} // namespace duckdb
