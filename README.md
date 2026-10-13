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
  --threads 2
```

This only displays candidate batch IDs and paths. Add `--execute` only
after reviewing the selection.

Defaults: `--workers 1`, `--threads 4`, `--max-batches 1`,
`--min-free-gib 20`. Workers 2, 3 and 4 use separate processes and Arrow
CPU thread pools. Four processes × two threads requires HDD/CPU/RAM
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

## Publish a fully verified compacted BDNB in place

`bdnb-parquet-compact-publish` is the **destructive finalization command**
for the BDNB subtree of the larger French open-data snapshot. Do not run it
before `bdnb-parquet-compact-schedule` has successfully compacted **all**
eligible BDNB batches. It fails closed when any compacted batch is missing.

On the m710s, use the existing plan, corpus and compacted output:

```bash
BDNB_TABLES='territoire-geospatial/cadastre/batiments/base-donnees-nationale/bdnb_2026_02_a_open_data/tables'

bdnb-parquet-compact-publish \
  --plan-dir /mnt/data/parquet-compaction-plan \
  --source-root /mnt/data/torrents/download/france-open-data-parquet-2026-10-10/parquet \
  --output-root /mnt/data/parquet-compaction-scheduled \
  --tables-root "$BDNB_TABLES" \
  --execute \
  --prune-backup
```

### Publication contract

- Requires the original BDNB `_SUCCESS.json` markers to match every
  table's planned source parts and total rows, with zero gaps in numbering.
- Requires all candidate compaction outputs and provenance JSON reports.
  Rechecks **all logical Arrow values** against the sources before staging.
- Builds a complete sibling BDNB dataset tree by **hardlinking** existing
  compacted outputs and singleton/non-compacted source parts; no bulk 32 GiB
  duplicate copy. Source and compacted outputs must be on one filesystem.
- Renumbers the new Parquet parts `part-00000.parquet`, etc.; updates
  `_SUCCESS.json`, `schemas/*.json`, `manifest.json`, `manifest.parquet`,
  and records compaction provenance in `dataset.json` and `_COMPACTION.json`.
- Validates all final part row counts and rebuilt manifest; then renames
  the original BDNB dataset to `*.precompact-backup` and activates the
  replacement. Without `--prune-backup`, the old dataset is retained.
- With `--prune-backup`, **permanently deletes obsolete originals** only
  after validating the activated replacement. Non-compactable singleton
  Parquet data is preserved via hardlinks, not deleted.
- On interruption during directory rename, the backup is preserved and
  subsequent runs refuse to proceed until it has been inspected/recovered.

**Scope:** Only this BDNB dataset subtree is replaced. The remaining
datasets in the 322-GiB corpus are untouched. Existing snapshot-level
manifest/checksum/torrent metadata outside BDNB may now be stale and must
be rebuilt **before** creating or seeding the replacement torrent.

## One-command full BDNB compaction (recommended for the verified 2026-02-a dataset)

After reviewing the original BDNB converter provenance, use one command instead
of multiple pilot approvals. This validates the original `_SUCCESS.json` and
planner, selects **every** eligible BDNB candidate batch, runs 4 independent
worker processes × 2 PyArrow threads, fully verifies each batch, stages a
hardlinked replacement dataset, regenerates BDNB manifests and then deletes
redundant original Parquet fragments **only after published validation**.

```bash
bdnb-parquet-compact-all \
  --plan-dir /mnt/data/parquet-compaction-plan \
  --source-root /mnt/data/torrents/download/france-open-data-parquet-2026-10-10/parquet \
  --output-root /mnt/data/parquet-compaction-scheduled \
  --tables-root "territoire-geospatial/cadastre/batiments/base-donnees-nationale/bdnb_2026_02_a_open_data/tables" \
  --workers 4 \
  --threads 2 \
  --execute \
  --prune-originals
```

This one call deliberately replaces the BDNB dataset subtree in the existing
snapshot. The old directory is renamed to a backup during promotion and its
obsolete part files are **permanently removed** once the new tree passes
verification. Uncompacted singleton Parquet files remain available (with
new part filenames as appropriate), and the output staging tree is kept for
audit. If the machine stops unexpectedly during promotion, the backup is
left intact and the command refuses a further attempt until recovery.

Omit `--execute --prune-originals` to preview the original table counts and
candidate batches without modifying any data. Use `--execute` without
`--prune-originals` to preserve the complete pre-compaction BDNB backup.

**Only the BDNB dataset is compacted by this command** (not the full 322 GiB
mixed-source corpus). Snapshot-level indexes, file hashes and torrent metadata
outside the BDNB dataset must be regenerated after publication.

### BDNB snapshot mounted through a read-only Transmission bind mount

A snapshot mounted as
`/mnt/data/torrents/download/france-open-data-parquet-2026-10-10`
from `/mnt/data/datasets/france-open-data-parquet-2026-10-10`
may have a **different mount ID** from the writable `/mnt/data` root.
Linux returns EXDEV for hardlinks across separate mount IDs even if the
underlying ext4 filesystem is identical. The physical /mnt/data/datasets
path can itself still traverse a separate mount, so using it alone does
not guarantee that hardlinks will work.

For the final **publication-only** run, use the native physical mount alias.
The planner stays anchored to its original --source-root and the tool requires
both source roots to identify the same physical directory (same inode). It
probes both the ability to create a staging directory and a real cross-path
hardlink **before** verifying any of the candidate batches. If the cross-path
link fails with EXDEV, the publisher falls back to staging copies; it verifies
each copied file's byte length and SHA-256 checksum before promotion.
The preflight reserves the estimated total size of compacted Parquet outputs
plus `--min-free-gib` (default 2 GiB) for the worst-case copy requirement.
This is a *space estimate*, not a filesystem quota.

```bash
bdnb-parquet-compact-publish \
  --plan-dir /mnt/data/parquet-compaction-plan \
  --source-root /mnt/data/torrents/download/france-open-data-parquet-2026-10-10/parquet \
  --writable-source-root /mnt/data/datasets/france-open-data-parquet-2026-10-10/parquet \
  --output-root /mnt/data/parquet-compaction-scheduled \
  --tables-root "territoire-geospatial/cadastre/batiments/base-donnees-nationale/bdnb_2026_02_a_open_data/tables" \
  --execute \
  --prune-backup
```

The native directory must be writable by the invoking user, including the
BDNB dataset's parent directory. Do not run the Parquet publisher as root.
The same `--writable-source-root` option is accepted by
`bdnb-parquet-compact-all`. The command emits an `INFO` message when
the byte-copy fallback is activated. The published replacement is immediately visible
through both native and bind-mount paths since the snapshot directory itself
is not renamed. Original redundant BDNB parts are removed only after staging
and publication validation. Subsequent global snapshot indexes or torrents
must be rebuilt.

## Five selected datasets: verified staging before destructive publication

The next five source trees are:

- `administration/associations/repertoire-national/waldec`
- `tourisme/offre-nationale/datatourisme`
- `territoire-geospatial/cadastre/batiments/imope`
- `administration/associations/repertoire-national/import`
- `climat-environnement/secheresse/vigieau`

The existing planner groups by parent directory and serialized Arrow schema.
That alone is NOT proof that files may replace one another: DataTourisme
`ntriples/<snapshot>/part-00000.parquet` is a consecutive sequence, but
IMoPE departmental files, Vigieau GeoJSON members and independent
association sources can encode identity in their filenames and manifests.

First inspect the actual candidate inventory without changing the snapshot:

```bash
bdnb-parquet-compact-five \
  --plan-dir /mnt/data/parquet-compaction-plan \
  --source-root /mnt/data/torrents/download/france-open-data-parquet-2026-10-10/parquet
```

Stage all five datasets' candidate batches without changing or deleting
any source. Exact Arrow schemas, metadata and logical row values are
verified by the same bounded-memory runner used for BDNB:

```bash
bdnb-parquet-compact-five \
  --plan-dir /mnt/data/parquet-compaction-plan \
  --source-root /mnt/data/torrents/download/france-open-data-parquet-2026-10-10/parquet \
  --output-root /mnt/data/parquet-compaction-other5 \
  --workers 4 --threads 2 \
  --execute --include-independent-files
```

The explicit `--include-independent-files` flag acknowledges that some
candidates combine separately named inputs. It allows **staging only**,
never source deletion. A durable
`_meta/five-datasets-provenance.json` maps each output to all its
original input paths, row ranges, schema and plan fingerprint. The
existing scheduler journal records verified, resumable output batches.

**Do not delete the original Parquet files or replace their manifest files
from these staged results**. A dataset-specific publisher must update
filename-based source references and validate source partition semantics
before pruning them. The global snapshot/torrent manifest must be
regenerated after all datasets have been safely published.

## Five datasets: controlled publication and original cleanup

The separate `bdnb-parquet-compact-five-publish` command consumes the 497
fully verified staged batches. It never recompresses Parquet data. It
publishes *exactly* WALDEC, DataTourisme, IMoPE, RNA import and Vigieau.

### Preflight without changes (always run first)

```bash
bdnb-parquet-compact-five-publish \
  --plan-dir /mnt/data/parquet-compaction-plan \
  --source-root /mnt/data/torrents/download/france-open-data-parquet-2026-10-10/parquet \
  --writable-source-root /mnt/data/datasets/france-open-data-parquet-2026-10-10/parquet \
  --output-root /mnt/data/parquet-compaction-other5 \
  --duplicates-catalog /mnt/data/datasets/release-parquet-plan/_meta/duplicates.tsv
```

IMoPE has 187 SHA-256 deduplicated logical source paths absent from
the physical release. Use the original `duplicates.tsv` catalog to prove
the byte-identical canonical source for **every** missing path. Preflight
verifies exact duplicate/canonical paths, original byte counts, real
canonical SHA-256, row count, row groups, columns and GeoParquet status;
no missing entry may be guessed from filename or schema. Canonicals
outside IMoPE remain release-scoped logical references if their physical
paths stay unchanged; cross-dataset canonical files themselves scheduled
for compaction are refused for manual audit. If an internal canonical
file is compacted, its alias resolves to the new physical file and
exact row range. Published metadata separates physical
`rows` from `original_logical_rows` and preserves all aliases in
`deduplicated_aliases` without recreating physical duplicates.

Vigieau's legacy converter manifest has **4,721 records**, but the release
contains **2,338 physical Parquet files**. The release-generation pipeline
moved 2,322 surviving GeoJSON files from archive-named folders to
`geojson/YYYY`, left 16 paths unchanged, and deduplicated 2,383
additional logical records by SHA-256. Preflight now checks **every**
original record against one of these three mechanisms and refuses any
unlisted physical files, duplicate original paths, unresolvable canonicals,
size differences, Parquet row mismatches, or SHA-256 mismatches. The
published `dataset.json` distinguishes the physical **1,330,462**
rows from the legacy **1,571,878** logical rows, including **241,416**
rows represented by aliases. Every alias retains its original name,
canonical SHA-256, release path, and final published physical row range
under `_COMPACTION_SOURCES.json`; the new manifest lists physical files
only. No duplicate Parquet files are recreated.

The preflight checks the old plan against the actual dataset file counts,
inspects converter manifests and rejects missing, changed or unrecognized
source references (including unexpectedly stale JSON sidecars). A narrow
DataTourisme exception accepts an absent CSV Parquet output only when its
legacy manifest entry declares **exactly zero rows and zero row groups**,
and no physical CSV output is unlisted. The staged manifest omits that
nonexistent output, records its full original entry under
`compaction_omitted_empty_csv`, and recalculates file counts and byte
sizes; the original manifest remains unchanged until the final directory
swap. Any nonempty missing source or ambiguous manifest still blocks
publication. If a
dataset has been normalized, the Vigieau converter's known
`geojson/<archive-year>/...` to `geojson/YYYY/...` move is reconciled
using the exact rule in `normalize_vigieau.py`; no fuzzy matching.

### Explicit publication

Only after the preflight succeeds, stop the old Transmission torrent and
grant the invoking user write permission on the four **direct parent**
directories of these datasets on the *native writable mount*. Then run:

```bash
bdnb-parquet-compact-five-publish \
  --plan-dir /mnt/data/parquet-compaction-plan \
  --source-root /mnt/data/torrents/download/france-open-data-parquet-2026-10-10/parquet \
  --writable-source-root /mnt/data/datasets/france-open-data-parquet-2026-10-10/parquet \
  --output-root /mnt/data/parquet-compaction-other5 \
  --duplicates-catalog /mnt/data/datasets/release-parquet-plan/_meta/duplicates.tsv \
  --workers 4 --threads 2 \
  --execute
```

The verification phase can reuse the proven **4 worker processes × 2
Arrow threads** configuration via `--workers 4 --threads 2`.
The worker pool performs only read-only output-audit and complete
ordered Arrow equality checks. At most four batches are in flight;
if any fails, no staging or directory swaps take place. The source
tree staging, manifest rewrites, five directory swaps and rollback
remain sequential. On a mechanical HDD this configuration is not
guaranteed faster than fewer workers; measure actual progress.

Keep `--prune-backup` **off for the first real publication** so the five
`.precompact-backup` directories are preserved until post-publication
verification. `--workers` is limited to 1–4; `--threads` must be
positive. Dry-run remains read-only and does not run bulk Arrow
comparisons.

The publisher verifies all 497 compressed outputs against every original
Arrow value and schema, stages **all five** dataset replacement trees with
verified hardlinks or SHA-256-checked cross-mount copies, rebuilds the
native DataTourisme NTriples `_SUCCESS.json` and numbered part files,
DataTourisme CSV mappings, IMoPE departmental inventories, and Vigieau
source records, and writes `_COMPACTION_SOURCES.json` per dataset.
The index maps every obsolete original filename to its new physical
Parquet path, zero-based `first_row` offset and `rows` length. The
original identity and date are preserved in that mapping rather than
as a one-file-per-date physical layout.

After stage validation, the publisher swaps all five directories while
retaining `.precompact-backup` originals. Ordinary swap exceptions trigger
rollback of already-swapped datasets. Only after all five new datasets
pass validation are the backups pruned when `--prune-backup` was given.
A power outage/SIGKILL mid-swap can still require manual inspection;
the tool refuses pre-existing stages or backups. A failure during backup
pruning also requires inspection; new published data already passed
validation. There is no filesystem-wide atomic transaction spanning five
independent directory names.

Peak staging headroom is conservatively budgeted from all source and
compacted file sizes plus `--min-free-gib` (default 20). Leave the old
snapshot's bind-mount protections in place; the physical native alias
is used for directory renames. Do not run as root.

**The global corpus manifest, checksums, and torrent metadata are NOT
regenerated by this publisher**. They must be rebuilt after publication
before seeding the new torrent. Do not delete the 74 GiB compaction output
work directory until the entire corpus and source mappings have been
audited.

## IMoPE v2: read-only cross-department compaction proposal

**This is an inventory and planning tool only.** It does not execute
compaction, rewrite GeoParquet, modify the existing publication or backups,
update departmental manifests, or change Transmission/torrent metadata.
It must not be treated as an executable publication plan.

Following the first five-dataset publication, IMoPE contains **3,360**
physical Parquet files, of which **94** already have a `compact-` prefix.
The preliminary external diagnosis counted **3,266** uncompacted files,
**1,687** full Arrow schema variants, and **76** structural variants.
The bbox-only cohort simulation suggested ~**402** total files from
**79** compatibility cohorts. This remains an estimate until reproduced
by the plan command on the real published tree.

The planner groups files across departments **only** when the table name,
Arrow field structures (including per-field metadata), and all schema-level
metadata except GeoParquet `bbox` agree. It does **not** mix different
CRSs, geometry encodings, pandas metadata or structural variants. It reads
Parquet footers/schemas but not column data; it never drops or overwrites a
source. Proposals include exact original department paths and ordered
`first_row`/`rows` provenance, plus proposed merged bounding boxes.
Missing bbox coverage or incompatible bbox dimensions are flagged for manual
geometry validation. A proposed bbox union is not itself proof of GeoParquet
validity. Previously compacted physical files are never candidates.

Inspect the actual published snapshot **without writing any files**:

```bash
bdnb-imope-v2-plan \
  --root /mnt/data/datasets/france-open-data-parquet-2026-10-10/parquet/territoire-geospatial/cadastre/batiments/imope \
  --target-mib 256
```

To keep an **external** reviewable proposal, use `--output-dir` pointing
to a *new* path (its parent must exist). The command refuses to overwrite
previous plans or write within the IMoPE directory:

```bash
bdnb-imope-v2-plan \
  --root /mnt/data/datasets/france-open-data-parquet-2026-10-10/parquet/territoire-geospatial/cadastre/batiments/imope \
  --target-mib 256 \
  --output-dir /mnt/data/imope-v2-plan
```

Artifacts: `summary.json` (counts and uncertainty),
`batches.json` (proposed cohorts, output names, bbox unions and explicit
metadata-review warnings), `sources.tsv` (one original source per row,
including department and source-to-proposed-output row offsets),
`untouched.json` (originals not selected for a batch), and
`pending_alias_rewrites.json` (exact future physical paths and row offsets
for existing IMoPE logical aliases whose canonicals would be regrouped).
The planner reads the current `_COMPACTION_SOURCES.json` when present,
counts all recorded logical aliases, and refuses malformed internal alias
references; it does not modify or repair those references. The proposal uses
source file sizes, which cannot predict compressed output sizes exactly.

**Do not run a second compaction or remove the five
`.precompact-backup` directories on the basis of this plan.** The next
implementation will require a separate reviewed pilot, geometry-aware
metadata regeneration, full Arrow equality checks, an updated IMoPE
department/alias index, and transactional publication with preserved
backups. The existing 187 SHA-256 aliases must continue resolving after
any future physical reorganization.


## IMoPE v2: isolated 2048 MiB compaction pilot

**Chosen planning target: 2048 MiB (2 GiB).** The metadata-only
plan projected **213 physical IMoPE Parquet files**, down from 3,360:
92 proposed new outputs, 27 unchanged un-compacted physical files and
94 already-compacted outputs. This is a file-count *projection*, not a
promise of exact compressed output sizes.

Before any second publication, an isolated pilot can test the *actual*
Arrow streaming and GeoParquet metadata regeneration. The pilot does not
replace the IMoPE directory, modify source Parquet, alter source indices
or aliases, touch any of the five `.precompact-backup` directories,
or change torrents.

First, produce a new **external** 2048 MiB plan (not under IMoPE):

```bash
PYTHONPATH=src .venv/bin/python -m bdnb_parquet.imope_v2_plan \
  --root /mnt/data/datasets/france-open-data-parquet-2026-10-10/parquet/territoire-geospatial/cadastre/batiments/imope \
  --target-mib 2048 \
  --output-dir /mnt/data/imope-v2-plan-2048
```

Preview the **smallest GeoParquet batch** without writing:

```bash
PYTHONPATH=src .venv/bin/python -m bdnb_parquet.imope_v2_pilot \
  --plan-dir /mnt/data/imope-v2-plan-2048 \
  --source-root /mnt/data/datasets/france-open-data-parquet-2026-10-10/parquet/territoire-geospatial/cadastre/batiments/imope \
  --pick smallest-geo
```

Only after reviewing the selected sources, perform the isolated pilot
with an unused output directory:

```bash
PYTHONPATH=src .venv/bin/python -m bdnb_parquet.imope_v2_pilot \
  --plan-dir /mnt/data/imope-v2-plan-2048 \
  --source-root /mnt/data/datasets/france-open-data-parquet-2026-10-10/parquet/territoire-geospatial/cadastre/batiments/imope \
  --output-root /mnt/data/imope-v2-pilot-small-2048 \
  --pick smallest-geo --threads 2 --execute
```

The command reads source footers again, regenerates the union of recorded
GeoParquet bboxes, writes a new Zstandard Parquet file via bounded Arrow
batches and verifies **all ordered Arrow values and output metadata** before
the pilot output becomes visible. It writes a JSON report with input
provenance and timings. A failure never replaces original files. Existing
output directories are refused, not overwritten.

For a more demanding trial, rerun with `--pick largest-geo` and a
**different new output directory**. The two trials serve different goals:
first functional correctness, then throughput and memory measurements.

Caveats: the bbox union is derived from source metadata rather than
recalculated from geometries themselves. The pilot report explicitly notes
that geometry-level validation remains outstanding. Neither pilot authorizes
replacing the existing 3,360-file IMoPE publication, resolving the 90
potential alias redirects, or deleting backups. A full multi-worker
publisher and independent geometry/manifest audits require separate review.


## IMoPE v2: four-worker staging of all 2048 MiB batches

Following the successful ~2 GiB nine-file `parcelle.parquet` pilot
(8,797,706 original rows compared exhaustively, no original touched),
`bdnb-imope-v2-schedule` stages the complete plan in a **separate
output directory**. It does **not** publish, overwrite source files,
remove backups, regenerate global checksums or modify torrents.

Selected release target: **2048 MiB (2 GiB)**, with **4 independent
process workers and 2 Arrow threads per worker**. The real read-only
plan proposed 92 compacted output files from 3,239 existing physical
sources, leaving 27 singleton source files and retaining 94 previously
published compacted files, for 213 projected physical files after a
future, separately reviewed publication. The 187 logical aliases (90
requiring retargeting) are not rewritten by the scheduler.

Use the existing `/mnt/data/imope-v2-plan-2048` **only if its original
SHA-256-locked planner artifacts are still present**. A new plan is
required if the sources have changed, but an existing staging directory
cannot be reused for a different plan.

Preflight all 92 source inventories, schemas and bounding-box profiles,
without creating files:

```bash
PYTHONPATH=src .venv/bin/python -m bdnb_parquet.imope_v2_schedule \
  --plan-dir /mnt/data/imope-v2-plan-2048 \
  --source-root /mnt/data/datasets/france-open-data-parquet-2026-10-10/parquet/territoire-geospatial/cadastre/batiments/imope \
  --output-root /mnt/data/imope-v2-stage-2048 \
  --workers 4 --threads 2
```

Once the preflight succeeds and free disk capacity is confirmed, opt in
to writing only the **new external** staging directory:

```bash
PYTHONPATH=src .venv/bin/python -m bdnb_parquet.imope_v2_schedule \
  --plan-dir /mnt/data/imope-v2-plan-2048 \
  --source-root /mnt/data/datasets/france-open-data-parquet-2026-10-10/parquet/territoire-geospatial/cadastre/batiments/imope \
  --output-root /mnt/data/imope-v2-stage-2048 \
  --workers 4 --threads 2 --execute
```

All selected sources are re-inspected. Each worker writes one temporary
Parquet and compares every ordered Arrow value plus output schema,
GeoParquet metadata and row count *before* it makes the validated
file visible. Only workers owning their output path write into the stage.
The new root is guarded by an immutable plan fingerprint, a
nonblocking exclusive `schedule.lock` and an append-only durable journal
under `_meta/`. At most four batches are in flight. On the first
failure, the scheduler starts no new jobs, finishes the in-flight jobs
and records an unsuccessful run.

**Resume** by repeating the identical command and output root. For
every existing completed batch it validates its JSON audit, file size,
schema, bounding box and **all Arrow values again**; it does not blindly
trust the old success journal. Existing outputs without their audit
files, incomplete `.partial` files, divergent plan fingerprints and
pre-existing unrelated output roots fail closed for manual inspection.
Do not manually delete or replace a `.partial` without inspecting it
and confirming that it belongs to this staging attempt.

The final `_meta/last-run.json` should show
`successful_batches == selected_batches`, `failed_batches == 0`,
and the planned source row total. This still does **not** authorize
publishing IMoPE: a separate audit must independently validate the
WKB/GeoParquet coordinate extents, source-to-batch row mappings, the
departmental manifests, and all 187 aliases before any directory
swaps. Keep the existing five `.precompact-backup` copies.


## IMoPE v2: independent geometry and provenance validation

The 92 batches generated at `/mnt/data/imope-v2-stage-2048` must
pass an **independent** audit before any source is removed or replaced.
`bdnb-imope-v2-audit` repeats complete Arrow-value comparisons with
originals, rereads each output footer and source schema, decodes all
WKB geometries with Shapely/GEOS, recomputes real geometry bounding
boxes and compares them to the GeoParquet metadata. It also verifies
physical file coverage (candidate, singleton, first-published compact),
old per-source row ranges, all existing SHA-256 aliases, and the 90
required re-targeting proposals.

The audit may be CPU- and I/O-intensive: it processes the complete
365-million-row staging inventory, not a sample. Four spawned workers
are supported, with at most four concurrent batches.

**Full audit without any writes:**

```bash
PYTHONPATH=src .venv/bin/python -m bdnb_parquet.imope_v2_audit \
  --plan-dir /mnt/data/imope-v2-plan-2048 \
  --source-root /mnt/data/datasets/france-open-data-parquet-2026-10-10/parquet/territoire-geospatial/cadastre/batiments/imope \
  --stage-root /mnt/data/imope-v2-stage-2048 \
  --workers 4
```

**After successful dry validation**, to persist a complete SHA-256
attestation in the *external stage metadata* (not inside the published
IMoPE dataset), rerun with `--attest`. This repeats every check,
and writes a new `_meta/imope-v2-validation.json` only when all
92 outputs, all physical sources and all logical mappings pass. The
command refuses to overwrite an existing attestation. It never runs
publication, deletes backups or alters originals:

```bash
PYTHONPATH=src .venv/bin/python -m bdnb_parquet.imope_v2_audit \
  --plan-dir /mnt/data/imope-v2-plan-2048 \
  --source-root /mnt/data/datasets/france-open-data-parquet-2026-10-10/parquet/territoire-geospatial/cadastre/batiments/imope \
  --stage-root /mnt/data/imope-v2-stage-2048 \
  --workers 4 --attest
```

A failed audit must be investigated, **not bypassed**. The 3,360
current files remain published, and all five old
`.precompact-backup` directories remain untouched. Even a successful
audit is not yet permission to delete anything: the department manifest
format, historical provenance and alias consumers need a separately
tested publisher and a post-swap verification. The intended future
physical layout contains 92 new compacted outputs, 27 indispensable
singleton files, and 94 first-pass compacted outputs: those 121
non-candidate files must **not** be deleted.


## IMoPE v2: attested publication and redundant-source cleanup

After the **independent geometry and Arrow audit passed for all 92
batches** (3,239 sources, 365,270,873 verified rows, 64 WKB GeoParquet
batches and all 187 aliases, including 90 to retarget), the new
`bdnb-imope-v2-publish` publishes **only the IMoPE dataset**. The
expected layout has **213** physical Parquet files: 92 newly grouped,
94 preserved compact files from the first publication, and 27 preserved
singleton files. All source data is preserved through *logical* department
row ranges and the versioned source index.

This command does **not** modify other dataset roots, torrent data,
external corpus release hashes or the five first-pass
`.precompact-backup` directories. It does not automatically seed a
new torrent. The legacy IMoPE department folders still hold
`_SUCCESS.json` and any indispensable old files; they are not all
deleted because department-level metadata must stay accessible. No
redundant old Parquet remains in the replacement dataset.

It first verifies the immutable plan and independent attestation
fingerprints, hashes each staged output Parquet against the 92 original
attested SHA-256 values, confirms the existing departmental Parquet
manifest and all 187 aliases, and refuses any extra files.

**Dry-run / preflight (default, no changes to published files):**

```bash
PYTHONPATH=src .venv/bin/python -m bdnb_parquet.imope_v2_publish \
  --plan-dir /mnt/data/imope-v2-plan-2048 \
  --source-root /mnt/data/datasets/france-open-data-parquet-2026-10-10/parquet/territoire-geospatial/cadastre/batiments/imope \
  --stage-root /mnt/data/imope-v2-stage-2048
```

**Publish the audited replacement with a distinct v2 rollback backup:**

```bash
PYTHONPATH=src .venv/bin/python -m bdnb_parquet.imope_v2_publish \
  --plan-dir /mnt/data/imope-v2-plan-2048 \
  --source-root /mnt/data/datasets/france-open-data-parquet-2026-10-10/parquet/territoire-geospatial/cadastre/batiments/imope \
  --stage-root /mnt/data/imope-v2-stage-2048 \
  --execute
```

The publisher builds a complete sibling replacement tree through
hardlinks or verified byte-for-byte copies; it removes only the planned
3,239 source paths *inside that replacement*, links the 92 attested
outputs, constructs `tables/<table>/compact-v2-*.parquet`, rebuilds
departmental `manifest.json` and `_SUCCESS.json` files, migrates
90 alias row offsets, and extends `_COMPACTION_SOURCES.json` with
`v2_sources` **without discarding the 194 historical first-pass
source records**. It independently checks all Parquet file counts,
rows, source ranges and aliases *before* swapping the IMoPE directory.
After the swap, it rechecks every new output SHA-256 plus all metadata
references. An ordinary post-swap error attempts to restore the original
directory; a power failure may require manual recovery.

The original pre-v2 published directory is retained by default at the
sibling path `imope.imope-v2-prepublish-backup`. This backup is distinct
from the five previous `.precompact-backup` directories and must not
be mistaken for them. The one-pass option `--execute --prune-backup`
irreversibly deletes that v2 backup **only after the new published
dataset has passed its post-swap checks**. Use only when willing to
forfeit the immediate v2 rollback. There is deliberately no automatic
pruning of older first-pass backups.

Any existing v2 replacement stage or v2 backup blocks another
publication attempt, avoiding unsafe overwrites. For old torrents and
release metadata, regenerate the global manifests and torrent *after*
this publication has been audited and the release tree is stable.
