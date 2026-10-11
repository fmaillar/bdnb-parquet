# bdnb-parquet

Streaming converter for the BDNB `2026-02-a` PostgreSQL SQL dump distributed as a gzip-compressed tar archive.

The goal is a controlled, typed Parquet/GeoParquet warehouse that can be consumed directly by PyArrow, DuckDB, pandas and GeoPandas without restoring the 157 GiB SQL dump into PostgreSQL.

## Design

- one sequential pass through `pgdump/bdnb.sql`;
- GNU `tar` selects the SQL member from the decompressed stream (no seek required);
- **parallel gzip decompression** via `rapidgzip`;
- **parallel multi-core conversion/writing** via `ProcessPoolExecutor`;
- bounded number of in-flight Parquet parts to control RAM usage;
- parses `ALTER TABLE ... ADD COLUMN`, `AddGeometryColumn`, and `COPY ... FROM STDIN`;
- explicit PostgreSQL -> Arrow typing;
- `numeric(p,s)` -> Arrow decimal, unbounded `numeric` -> float64;
- PostgreSQL arrays -> Arrow lists;
- PostGIS 2D EWKB hex -> standard WKB (SRID removed from payload) with GeoParquet 1.1 metadata;
- ZSTD Parquet;
- one table = one Parquet dataset directory;
- crash-safe staging and `_SUCCESS.json` markers;
- optional row-count validation against the surviving legacy `.done.json` files;
- resumable at table granularity.

The SQL stream itself is sequential, but each Parquet part is independent. The main process reads COPY rows and dispatches bounded chunks to worker processes. This avoids pretending that Python threads would accelerate the CPU-bound parser despite the GIL. `rapidgzip` remains genuinely multithreaded.

## Install

```bash
cd ~/projets/bdnb-parquet
uv venv .venv
uv pip install --python .venv/bin/python -e .
```

## Parallelism

Default on an 8-logical-CPU machine:

```text
conversion workers : 4 processes
rapidgzip threads   : 4 threads
in-flight parts     : 4 maximum
```

The gzip thread count is automatically `CPU count - workers` unless explicitly set.

Examples:

```bash
# Balanced default
.venv/bin/bdnb-parquet --workers 4

# More conservative / lower RAM usage
.venv/bin/bdnb-parquet --workers 2 --max-in-flight 2

# Explicit CPU split on an 8-thread CPU
.venv/bin/bdnb-parquet --workers 5 --gzip-threads 3
```

`--file-rows` controls the number of source rows sent to one worker and written as one `part-*.parquet`. The default is 250,000. `--batch-rows` controls the Parquet row-group size and defaults to 100,000.

Increasing `--file-rows` reduces the number of files but increases RAM use because each worker receives a complete raw COPY chunk. For wide BDNB tables, keep the default unless memory usage has been measured.

## Smoke test

Convert only the first table and validate its row count against the legacy output:

```bash
.venv/bin/bdnb-parquet \
  --tables adresse \
  --workers 4 \
  > ~/bdnb-parquet-adresse.log 2>&1
```

Inspect:

```bash
sed -n '1,200p' ~/bdnb-parquet-adresse.log
```

## Full conversion

```bash
.venv/bin/bdnb-parquet \
  --workers 4 \
  > ~/bdnb-parquet-convert.log 2>&1
```

Re-running the command keeps completed tables and skips conversion for tables whose `_SUCCESS.json` matches the current source identity. Because the source SQL is sequential, their COPY data still has to be decompressed/traversed to reach later tables.

## Output

Default root:

```text
/mnt/data/datasets/parquet/territoire-geospatial/cadastre/batiments/
  base-donnees-nationale/bdnb_2026_02_a_open_data/
```

Structure:

```text
bdnb_2026_02_a_open_data/
├── dataset.json
├── manifest.json
├── manifest.parquet
├── schemas/
│   └── <table>.json
└── tables/
    └── <table>/
        ├── part-00000.parquet
        ├── part-00001.parquet
        └── _SUCCESS.json
```

The legacy `/mnt/data/datasets/numpy/...` tree is read only as a row-count reference and is never modified.

## Python examples

PyArrow:

```python
import pyarrow.dataset as ds

table = ds.dataset(
    "/mnt/data/datasets/parquet/territoire-geospatial/cadastre/batiments/"
    "base-donnees-nationale/bdnb_2026_02_a_open_data/tables/adresse",
    format="parquet",
)

df = table.to_table(
    filter=ds.field("code_departement_insee") == "67"
).to_pandas()
```

DuckDB:

```python
import duckdb

result = duckdb.sql("""
    SELECT code_departement_insee, count(*) AS n
    FROM read_parquet('/mnt/data/datasets/parquet/territoire-geospatial/cadastre/batiments/base-donnees-nationale/bdnb_2026_02_a_open_data/tables/adresse/*.parquet')
    GROUP BY 1
    ORDER BY 1
""").df()
```

GeoPandas:

```python
import geopandas as gpd

gdf = gpd.read_parquet(
    "/mnt/data/datasets/parquet/territoire-geospatial/cadastre/batiments/"
    "base-donnees-nationale/bdnb_2026_02_a_open_data/tables/adresse"
)
```

## Safety properties

- the source archive is opened read-only;
- the legacy Parquet tree is never modified;
- data is first written under `tables/.staging/<table>`;
- `_SUCCESS.json` is created only after Parquet footer row counts have been re-read and validated;
- unknown PostgreSQL types fail closed unless `--allow-type-fallback` is explicitly supplied.


## Dataset migration audit

The repository also provides a read-only migration inventory for the wider
dataset warehouse. It scans dataset **leaf directories** under `raw/`,
legacy `numpy/`, and canonical `parquet/`, profiles their file formats and
allocated size, and proposes conservative cross-layer matches.

```bash
bdnb-datasets-audit \
  --root /mnt/data/datasets \
  --output /tmp/datasets-migration-audit.tsv
```

The TSV reports source format profiles, sizes, proposed `raw -> numpy` and
`raw -> parquet` matches, match reasons/scores, and an action:

- `convert-raw-to-parquet`: no plausible canonical Parquet dataset found;
- `validate-parquet`: a plausible Parquet equivalent exists and must be
  validated before considering the legacy `numpy/` derivative redundant;
- `manual-review`: a derived dataset has no unambiguous raw match.

Matching is deliberately conservative. Exact relative paths score highest,
followed by same-parent/same-name and unique normalized-name matches. A match
is a migration candidate, not proof of semantic equivalence: deletion of
`numpy/` is only appropriate after format-specific validation.

The intended warehouse lifecycle is:

```text
raw/  --controlled conversion-->  parquet/
numpy/                          ->  legacy layer to retire after validation
```

The audit never modifies or deletes dataset content.

## Parquet compaction planning (read-only)

The `bdnb-parquet-compact` command inventories Parquet footers and proposes
physical file-compaction candidates. **This first phase does not rewrite,
move, or delete any source file.** It can be run against a BDNB dataset or a
larger Parquet corpus.

Run on the m710s (Python 3.13, with the project installed):

```bash
bdnb-parquet-compact \
  --root /mnt/data/torrents/download/france-open-data-parquet-2026-10-10/parquet \
  --output-dir /tmp/parquet-compaction-plan \
  --target-mib 256
```

The output directory must be outside the scanned source tree and must be
empty or not yet exist. Three files are produced:

- `summary.json`: source counts, rows, logical bytes, estimated output file
  count and potential file reduction;
- `batches.tsv`: one row per proposed output batch, with status, schema
  fingerprint, and input row count;
- `files.tsv`: one row per source file, mapping each input to a batch.

The grouping criterion is **same parent directory and identical serialized
Arrow schema**, including schema/field metadata such as GeoParquet attributes.
Source filenames are sorted deterministically. Original compressed sizes are
used as a heuristic to pack batches towards the requested target size;
the real size after recompression cannot be predicted from this plan.

Only groups with more than one file receive `review-required` status.
`keep` marks single files, `keep-oversize` marks files already above the
target, and `skip-partial` prevents interrupted outputs from entering
candidates. **Even a review-required candidate is not automatically safe to
merge:** schema equality does not establish dataset, temporal, geographic
partition, ordering or source-provenance equivalence. Review the TSV
manifests and accompanying dataset-specific metadata before authorizing an
actual compaction.

This phase does not estimate storage savings and does not validate row values.
The next phase will implement streamed writing and integrity verification
in a separate output tree after an explicit plan has been approved.

## Experimental streaming compaction: single batch

After reviewing a `bdnb-parquet-compact` plan, compact a single batch into a
**separate output tree**. The input files are never moved, overwritten or
deleted. All Arrow values are compared in source order with the result.

Start with the first ten parts of the BDNB `batiment_groupe_hthd` batch:

```bash
bdnb-parquet-compact-run \
  --plan-dir /mnt/data/parquet-compaction-plan \
  --source-root /mnt/data/torrents/download/france-open-data-parquet-2026-10-10/parquet \
  --output-root /mnt/data/parquet-compaction-pilot \
  --batch-id d1d1ac9f87edca61f1e292eff7d247413af32fb09e5f8b43b89ab5c443f50af7-0001 \
  --max-files 10 \
  --batch-rows 16384 \
  --row-group-rows 100000 \
  --max-buffer-mib 128 \
  --threads 4 \
  --zstd-level 3
```

`--max-files 10` is a **pilot**: the output filename includes `pilot-10`,
so it cannot be confused with the full 263-part batch. Without
`--max-files`, all source files listed for that batch are processed.

Before writing, each source's size, footer row count, and serialized Arrow
schema are compared with the saved plan. Parquet is read in small RecordBatches
(`--batch-rows 16384`), but several batches are buffered to form larger output
row groups (`--row-group-rows 100000`). The pending uncompressed Arrow buffer
is additionally limited to approximately `--max-buffer-mib 128` MiB: when the
limit is reached, a shorter row group is written. Actual process memory also
includes active input batches, writer compression buffers, and Arrow overhead.
The default compression is ZSTD level 3.

`--threads 4` enables PyArrow's multithreaded **column decoding** during
both conversion and value verification, using a temporary four-thread Arrow
CPU pool. One batch still writes a single Parquet output sequentially. This
can improve CPU-limited wide tables, but may not accelerate a mechanical-HDD
I/O bottleneck; do not assume linear speedups. Compare timings on the m710s.
Before promoting the temporary output, the runner reopens it, checks its
schema and row count, and compares **every logical Arrow value in order**
against all selected source files. Only a verified output is atomically
renamed into place, followed by a JSON audit report alongside it. A new
output undergoes exactly **one full-value verification** on its staging file:
the atomic rename does not modify its contents. Existing outputs are always
reverified completely on restart or with `--verify-only`.

On restart, if the output already exists, it is reverified instead of being
rewritten. **For row-group or compression benchmarks, use a fresh output root**
(e.g. `/mnt/data/parquet-compaction-pilot-v2`), since rerunning with different
flags does not overwrite an existing validated output. The JSON report includes
output row-group counts and minimum/maximum group sizes, plus the requested
thread count, write/verify/total wall times and logical-source-byte MiB/s
for the write and verify phases (not physical disk bandwidth).
Use `--verify-only` to check an existing output without creating one.
A malformed plan, changed source, or mismatching data stops the run.

Limitations: this experimental command processes a **single manually reviewed
batch**. It cannot establish dataset semantic equivalence from schemas alone;
date or provenance encoded solely in source filenames may be lost in the
merged Parquet data, although all input paths are retained in the sidecar
JSON report. It does not update BDNB `_SUCCESS.json` or release manifests,
and it never removes original Parquet files. No automatic corpus-wide
compaction or publication is performed.

For the **full 263-file** BDNB batch, omit `--max-files 10` and choose a fresh
output directory, for example `/mnt/data/parquet-compaction-full-test-threads4`.
On an HDD, start with `--threads 4`; benchmark against `--threads 1` on
separate output trees before tuning or launching corpus-wide work.

## BDNB multi-batch compaction scheduler (experimental)

The `bdnb-parquet-compact-schedule` command reuses the read-only planner.
It **never modifies the source Parquet corpus** and requires `--execute`
before writing compacted outputs. First inventory and review BDNB tables.

Set your corpus-specific table directory:

```bash
BDNB_TABLES='territoire-geospatial/cadastre/batiments/base-donnees-nationale/bdnb_2026_02_a_open_data/tables'
```

### 1. Inventory BDNB tables (no compaction)

```bash
bdnb-parquet-compact-schedule \
  --plan-dir /mnt/data/parquet-compaction-plan \
  --source-root /mnt/data/torrents/download/france-open-data-parquet-2026-10-10/parquet \
  --output-root /mnt/data/parquet-compaction-scheduled \
  --tables-root "$BDNB_TABLES" \
  --inventory > /tmp/bdnb-table-review.tsv
```

The inventory TSV includes candidate batch/file/row totals, compressed MiB,
schema variants and blocked candidate batches per table. The `approved`
and `reason` columns are initially blank. A table with zero blocked batches
is *technically eligible* but NOT automatically semantically approved.

Review each table's actual data semantics, source provenance, part naming,
row ordering and partitions. For each accepted table, set `approved` to
`yes` and supply a nonempty `reason` (no tabs or newlines). Leave all other
tables blank or `no`. Check the `other_batches` field for noncompactable
files in that same table directory.

### 2. Preview only approved tables

```bash
bdnb-parquet-compact-schedule \
  --plan-dir /mnt/data/parquet-compaction-plan \
  --source-root /mnt/data/torrents/download/france-open-data-parquet-2026-10-10/parquet \
  --output-root /mnt/data/parquet-compaction-scheduled \
  --tables-root "$BDNB_TABLES" \
  --approved-tables /tmp/bdnb-table-review.tsv \
  --max-batches 4 \
  --workers 4 \
  --threads 4
```

This only displays candidate batch IDs and paths. Add `--execute` only
after reviewing the selection.

Defaults: `--workers 1`, `--threads 4`, `--max-batches 1`,
`--min-free-gib 20`. Workers 2, 3 and 4 use separate processes and Arrow
CPU thread pools. Four processes × four threads requires HDD/CPU/RAM
monitoring and is not guaranteed faster than two processes.

Repeatable `--table NAME` is also an explicit selector. `--all-tables` is
**preview-only** and cannot be combined with `--execute`. Review TSVs
cannot approve blocked candidates or omit a review reason. Plans and real
source metadata are validated again before any newly compacted output.

### 3. Journal and recovery

The fsync'ed JSONL journal is
`<output-root>/_meta/compaction-journal.jsonl`; the latest summary is
`<output-root>/_meta/last-run.json`. Restart with the same command:
previous outputs are reverified in full, never blindly trusted or overwritten.
Per-output JSON audit reports retain source paths and verification results.

The source files are never deleted. Disk checks reserve the free-space
floor and an estimated staging budget (twice the largest concurrent
compressed input batches); this is a heuristic, not a hard quota.
Source-level semantic/provenance review remains required before publishing
any derivative corpus. `_SUCCESS.json`, release manifests and torrents
are not modified.
