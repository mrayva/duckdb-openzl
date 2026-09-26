#!/usr/bin/env python3
"""Route A: repack a DuckLake catalog's parquet data files as OpenZL archives, in place.

DuckLake only ever writes Parquet. This maintenance step takes the data files it wrote, re-encodes each one as canonical
parquet (keeping the parquet column field_ids DuckLake identifies columns by, and the row order, which DuckLake's row ids and
delete files depend on), compresses it with COPY ... (FORMAT OPENZL), proves the archive returns exactly the same rows, and
then repoints the catalog row (ducklake_data_file: path -> openzl://<abs>.zl, file_size_bytes/footer_size -> the logical,
decompressed parquet's) and deletes the original parquet file. Compaction (merge_adjacent_files) writes ordinary parquet again;
run this again afterwards to repack whatever is new. Reads of the packed files go through the extension's openzl:// filesystem,
so every DuckDB that queries the catalog must LOAD openzl.

  ducklake_pack.py --meta meta.ducklake [--ext .../openzl.duckdb_extension] [--workers 4] [--dry-run] [--limit N]

--meta is the DuckLake catalog when it is a DuckDB file (for other catalog backends pass --meta-attach 'ATTACH ...' text that
defines the schema name `meta`). Run it with nothing else writing to the DuckLake. Resumable: files already packed are skipped,
and a file only changes in the catalog after its archive was verified.
"""
import argparse, concurrent.futures as cf, csv, io, os, re, subprocess, sys, tempfile, threading, time

DUCK = os.environ.get("DUCKDB", "/home/mrayva/duckdb")
DEFAULT_EXT = "/home/mrayva/duckdb-openzl/build/release/extension/openzl/openzl.duckdb_extension"


def sq(s):
    return "'" + str(s).replace("'", "''") + "'"


class Duck:
    def __init__(self, ext, memory, threads):
        self.pre = f"LOAD {sq(ext)}; SET memory_limit={sq(memory)}; SET threads={threads}; SET openzl_max_compress_bytes=8000000000; "

    def run(self, sql, csv_out=True):
        p = subprocess.run([DUCK, "-unsigned", "-csv", "-c", self.pre + sql], capture_output=True, text=True)
        if p.returncode != 0:
            raise RuntimeError((p.stderr or p.stdout).strip().split("\n")[0][:400])
        if not csv_out:
            return p.stdout
        return list(csv.reader(io.StringIO(p.stdout)))[1:]    # drop header


def resolve(base, path, is_rel):
    return (base.rstrip("/") + "/" + path) if is_rel else path


def list_files(duck, meta_attach):
    rows = duck.run(meta_attach + """
      SELECT value FROM meta.ducklake_metadata WHERE key = 'data_path';
    """)
    data_path = rows[0][0]
    rows = duck.run(meta_attach + """
      SELECT DISTINCT ON (f.data_file_id) f.data_file_id, f.path, f.path_is_relative, s.path, s.path_is_relative, t.path,
             t.path_is_relative, f.file_format, f.file_size_bytes, f.record_count
      FROM meta.ducklake_data_file f JOIN meta.ducklake_table t USING (table_id)
      JOIN meta.ducklake_schema s ON s.schema_id = t.schema_id
      ORDER BY f.data_file_id;
    """)
    out = []
    for fid, fp, frel, sp, srel, tp, trel, fmt, size, rc in rows:
        frel, srel, trel = frel == "true", srel == "true", trel == "true"
        if frel:
            base = resolve(data_path, sp, srel)
            base = resolve(base, tp, trel)
            full = resolve(base, fp, True)
        else:
            full = fp
        if "://" not in full:
            full = os.path.abspath(full)              # the catalog must not depend on the packer's working directory
        out.append(dict(id=int(fid), path=full, fmt=fmt, size=int(size), rows=int(rc)))
    return out


def field_ids(duck, path):
    """Top-level field_ids of a flat parquet file, or None if the schema has nested columns / missing ids."""
    rows = duck.run(f"SELECT name, field_id, num_children FROM parquet_schema({sq(path)}) WHERE name <> 'duckdb_schema';")
    if not rows:
        return None
    ids = {}
    for name, fid, nchild in rows:
        if fid in ("", "NULL") or nchild not in ("", "NULL", "0"):
            return None
        ids[name] = int(fid)
    return ids


def fingerprint(duck, path):
    r = duck.run(f"SELECT count(*), coalesce(sum(hash(t)::HUGEINT), 0) FROM "
                 f"(SELECT * FROM read_parquet({sq(path)}, file_row_number = true)) t;")
    return tuple(r[0])


def pack_one(duck, f, keep_tmp=False):
    src = f["path"]
    zl = re.sub(r"\.parquet$", "", src) + ".zl"
    ids = field_ids(duck, src)
    if ids is None:
        return f, None, "skipped: nested columns or missing field_ids"
    fid_sql = "{" + ", ".join(f"{sq(k)}: {v}" for k, v in ids.items()) + "}"
    duck.run(f"COPY (SELECT * FROM read_parquet({sq(src)}, file_row_number = false)) TO {sq(zl)} "
             f"(FORMAT OPENZL, FIELD_IDS {fid_sql}, CHUNK_SIZE_BYTES 0);", csv_out=False)
    a, b = fingerprint(duck, src), fingerprint(duck, "openzl://" + zl)
    if a != b:
        os.remove(zl)
        return f, None, f"verification FAILED (source {a} vs archive {b})"
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        duck.run(f"SELECT openzl_decompress({sq(zl)}, {sq(tmp_path)});", csv_out=False)
        logical = os.path.getsize(tmp_path)
        with open(tmp_path, "rb") as h:
            h.seek(-8, 2)
            footer = int.from_bytes(h.read(4), "little")
    finally:
        os.remove(tmp_path)
    return f, dict(zl=zl, logical=logical, footer=footer, physical=os.path.getsize(zl)), "ok"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--meta", help="DuckLake catalog DuckDB file")
    ap.add_argument("--meta-attach", help="SQL that attaches the catalog as schema `meta` (instead of --meta)")
    ap.add_argument("--ext", default=DEFAULT_EXT)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--memory", default="8GB", help="memory_limit per worker")
    ap.add_argument("--threads", type=int, default=2, help="threads per worker")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if not (a.meta or a.meta_attach):
        ap.error("--meta or --meta-attach required")
    meta_attach = (a.meta_attach or f"ATTACH {sq(a.meta)} AS meta;") + " "
    duck = Duck(a.ext, a.memory, a.threads)

    files = [f for f in list_files(duck, meta_attach) if f["fmt"] == "parquet" and not f["path"].startswith("openzl://")]
    if a.limit:
        files = files[:a.limit]
    print(f"{len(files)} parquet data files to pack", flush=True)
    if a.dry_run:
        for f in files:
            print(f["id"], f["size"], f["path"])
        return
    before = after = done = failed = 0
    lock = threading.Lock()
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = [ex.submit(pack_one, duck, f) for f in files]
        for fut in cf.as_completed(futs):
            try:
                f, res, msg = fut.result()
            except Exception as e:
                failed += 1
                print(f"FAILED: {e}", flush=True)
                continue
            if res is None:
                failed += 1
                print(f"file {f['id']}: {msg}", flush=True)
                continue
            new_path = "openzl://" + res["zl"]
            duck.run(meta_attach + f"UPDATE meta.ducklake_data_file SET path = {sq(new_path)}, path_is_relative = false, "
                     f"file_size_bytes = {res['logical']}, footer_size = {res['footer']} WHERE data_file_id = {f['id']};",
                     csv_out=False)
            os.remove(f["path"])
            done += 1
            before += f["size"]
            after += res["physical"]
            print(f"[{done}/{len(files)}] file {f['id']}: {f['size']:,} -> {res['physical']:,} bytes "
                  f"({100.0 * res['physical'] / max(1, f['size']):.1f}%) | total {before:,} -> {after:,} | {time.time() - t0:.0f}s",
                  flush=True)
    print(f"DONE packed={done} failed={failed} bytes {before:,} -> {after:,}")


if __name__ == "__main__":
    main()
