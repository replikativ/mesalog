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

## [0.2.211] - 2024-02-07

### Changed
- **BREAKING**: `load-csv` argument order, semantics, and usage.
  - Database `cfg` has been supplanted by `conn`.
  - `filename` and `conn` are required.
  - `schema-opts` -> `schema-desc`, with different keys and semantics, as described in the docstring and `README.md`.
  - Optional `parsers-desc` argument added for specification of  column parsers / types.
- Type / Parser inference implemented within repo, instead of relying on `scicloj/tablecloth`.

### Added
- `tablehike.parse.parser` namespace added as replacement for `scicloj/tablecloth` dependency (see last item in the list of changes for this release).
- `tablehike.schema` namespace added for schema inference, in conjunction with parser / type inference implementation.

## [0.2.220] - 2024-02-11

### Changed
- Library name: from "tablehike" to "mesalog"
