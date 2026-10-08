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
