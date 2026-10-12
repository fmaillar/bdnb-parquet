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
  --output-root /mnt/data/parquet-compaction-other5
```

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
  --execute --prune-backup
```

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
