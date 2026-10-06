# Changelog

## 0.3.0

- add persistent rapidgzip random-access index and COPY-block offset catalog;
- resume directly at incomplete tables without reparsing completed COPY data;
- store PostgreSQL column and geometry metadata in the resume catalog;
- add `--rebuild-resume-index`, `--resume-index`, `--resume-catalog`, and `--no-indexed-resume`;
- accept slash- and dot-separated BDNB dates and timestamps;
- add regression tests for indexed COPY ranges and alternate date separators.

## 0.2.0

- add bounded multi-core Parquet conversion with `ProcessPoolExecutor`;
- keep `rapidgzip` multithreaded while splitting CPU budget between decompression and conversion;
- add `--workers`, `--gzip-threads`, and `--max-in-flight`;
- write independent typed Parquet parts in parallel;
- reduce default part size to 250,000 rows to bound per-worker memory;
- serialize the Arrow schema once in the parent and reuse it in workers;
- add GitHub Actions tests for Python 3.11, 3.12, and 3.13;
- document PyArrow, DuckDB, and GeoPandas consumption.

## 0.1.1

- stream the tar member through GNU `tar` instead of Python `tarfile`;
- normalize PostGIS EWKB to standard WKB for GeoParquet.

## 0.1.0

- initial streaming BDNB SQL -> typed Parquet/GeoParquet converter.
