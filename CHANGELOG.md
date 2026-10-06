# Changelog

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
