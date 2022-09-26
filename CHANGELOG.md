# Change Log

## [0.1.48] - 2022-09-13

### Changed
- **BREAKING**: `load-csv` argument order, meanings, and usage.
  - Only `csv-file` is required.
  - Database `cfg` instead of `conn`.
  - Ref- and tuple-related options separated out from the rest, with a map of schema-related options `:schema`, `:ref-map`, `:tuple-map` `:composite-tuple-map` replacing `col-schema`.

## [0.1.50] - 2022-09-13

### Added
- Full user specification of schema possible using alternative form for `:schema` to that for partial specification.

## [0.1.61] - 2022-09-22

### Changed
- Library name: from "datahike-csv-loader" to "tablehike"

## [0.1.66] - 2022-09-26

### Added
- Support for chunking, for files too large to fit within memory
