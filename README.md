# datahike-csv-loader

Loads CSV data into [Datahike](https://datahike.io) (or [on GitHub](https://github.com/replikativ/datahike)) with a single function call.

## Quickstart

[![Clojars Project](https://img.shields.io/clojars/v/io.replikativ/datahike-csv-loader.svg)](https://clojars.org/io.replikativ/datahike-csv-loader) [![cljdoc badge](https://cljdoc.org/badge/io.replikativ/datahike-csv-loader)](https://cljdoc.org/d/io.replikativ/datahike-csv-loader)

``` clojure
(require '[datahike.api :as d]
         '[datahike-csv-loader.core :as dcsv])

(dcsv/load-csv "data.csv")

;; or
(def cfg {:store ...})
(dcsv/load-csv "data.csv" cfg)

;; or (map contents elided here and described below)
(dcsv/load-csv "data.csv" cfg {:schema [{:db/ident :name
                                         ...}
                                        ...]
                               :ref-map {...}
                               :tuple-map {...}
                               :composite-tuple-map {...}})

;; or (ditto)
(dcsv/load-csv "data.csv" cfg {:schema {:unique-id #{...}
                                        ...}
                               :ref-map {...}
                               :tuple-map {...}
                               :composite-tuple-map {...}})
```

Reads, parses, and loads data from data.csv into the Datahike database having (optionally specified) config `cfg`, with likewise optional schema-related options for the corresponding attributes. Each column represents an attribute, with keywordized column name as attribute ident, or otherwise, an element in a heterogeneous or homogeneous tuple (more on tuples below).

## Datahike config

If `cfg` is omitted, and the last argument:
1. is also absent, or has empty `:schema`, `:ref-map`, and `:composite-tuple-map`, `cfg` is inferred to be `{:schema-flexibility :read}`.
2. has a non-empty value for one or more of `:schema`, `:ref-map`, and `:composite-tuple-map`, `cfg` is inferred to be `{}`, i.e. the default.

## Schema options

Each file is assumed to represent attributes for one entity "type", whether new or existing: e.g. a student with columns _student/name_, _student/id_. This also means that attribute data for a single "type" can be loaded from multiple files: for example, another file with columns _student/id_ and _student/course_ can be loaded later.

`:schema` in the last argument can be specified in one of two ways:
1. Full specification via the usual Datahike transaction data format, i.e. a vector of maps, each corresponding to an attribute.
2. Partial specification via a concise format like the map returned by `datahike.api/reverse-schema`, albeit with slightly different keys, each having a set of attribute idents as the corresponding value. Available options:

| Key                 | Description   |
|---------------------|---------------|
| `:unique-id`        | `:db/unique` value `:db.unique/identity`
| `:unique-val`       | `:db/unique` value `:db.unique/value`
| `:index`            | `:db/index` value `true`
| `:cardinality-many` | `:db/cardinality` value `:db.cardinality/many`

For example, a value of `#{:user/email :user/account-id}` for the key `:unique-id` indicates that the attributes in the set are unique identifiers.

Ref- and tuple-valued attributes, i.e. those with `:db/valueType` `:db.type/ref` or `:db.type/tuple`, are however specified separately, via `:ref-map`, `:tuple-map`, or `:composite-tuple-map`, each a map as follows:

| Key                     | Description   |
|-------------------------|---------------|
| `:ref-map`              | `:db.type/ref` attribute idents to referenced attribute idents
| `:composite-tuple-map`  | Composite `:db.type/tuple` attribute idents to constituent attribute idents
| `:tuple-map`            | Other (homogeneous, heterogeneous) `:db.type/tuple` attribute idents to constituent attribute idents

Unspecified schema attribute values are defaults or inferred from the data given: for instance, except with `:db.type/ref` and `:db.type/tuple`, `:db/valueType` is inferred. Note also that only one cardinality-many attribute is allowed per file for semantic reasons. Examples in the rest of this document use the partial `:schema` specification style for brevity.

### Attributes already in schema

`load-csv` also handles data for attributes already present in the schema, e.g. if a file with identical or overlapping column names was loaded earlier, in which case the corresponding columns should be left out of `:schema`, although they would be excluded anyway from any schema transaction before the data proper is loaded. That said, this behaviour hasn't yet been tested, so caution is advised.

### `:ref-map`

Data in a reference-valued attribute column must consist of domain identifier (i.e. an attribute with `:db.unique/identity`) values for entities already present in the database; these are automatically converted into entity IDs. For example:

``` clojure
(d/transact conn [{:db/ident :course/id
                   :db/unique :db.unique/identity
                   ...}])
(d/transact conn [{:course/id "CMSC101"
                   :course/name "Intro. to CS"}
                   ...])
(dcsv/load-csv "students.csv" cfg {:schema {:unique-id #{:student/id}
                                            :cardinality-many #{:student/course}}
                                   :ref-map {:student/course :course/id}})
;; values for :student/course will consist of their corresponding course entity IDs 
```
With CSV contents such as:

| student/id | student/course |
|------------|----------------|
| 1          | CMSC101        |
| 1          | MATH101        |
| 1          | MUSI101        |
| 2          | PHYS101        |
| 2          | ...            |

Support for loading entity IDs directly can be added if observations of such use cases in the wild are reported.

### `:tuple-map` and `:composite-tuple-map`

First: an [introduction](https://docs.datomic.com/on-prem/schema/schema.html#tuples) to tuples for the uninitiated.

`load-csv` can load data from multiple columns into any of the three kinds of tuples available in Datahike (as in Datomic, for which the documentation just linked to is written): composite, heterogeneous, and homogeneous. Composite tuples are automatically created and transacted by the database when their constituent attributes are transacted (and retained independent of the tuple attribute); heterogeneous and homogeneous tuples consist instead of user-created vectors, with no independent constituent attributes. 

In addition to `:schema` specification as necessary, the ident of and columns belonging to each tuple need to be specified. This should be done via `:composite-tuple-map` or `:tuple-map` as appropriate, with key-value pairs each consisting of a tuple attribute ident and a vector of corresponding attribute idents (for composite tuples) or keywordized column names (for other tuple types); whether each entry in `:tuple-map` is homogeneous or heterogeneous is inferred from its column data value types.

For example, roughly working off [this schema definition](https://docs.datomic.com/on-prem/schema/schema.html#composite-tuples), to create a composite tuple attribute from attributes (columns) _student/id_, _course/id_, and _semester/year+season_:
``` clojure
(load-csv "data.csv" cfg {:composite-tuple-map
                          {:reg/semester+course+student [:student/id :course/id :semester/year+season]}
                          ...})
```
This results in four separate attributes; a full `:schema` specification should include all of them (whereas a partial specification should of course only feature attributes as needed).

Another example, of creating a homogeneous tuple using columns _station/lat_ and _station/lon_:
``` clojure
(load-csv "data.csv" cfg  {:tuple-map {:station/coordinates [:station/lat :station/lon]}
                           ...})
})
```
Here, the data in these columns are merged and transacted only as `:station/coordinates`; a full `:schema` specification need only include the tuple attribute.

As implied above, the `db/valueType` of tuple elements is inferred unless specified in `:schema`.

## Current limitations

datahike-csv-loader currently doesn't support:
1. Loading CSV files that don't fit into memory.
2. Variable-length homogeneous tuples.
3. Excluding columns from the import.

We are currently working on #1 and #3. We will address #2 and any other limitations that arise as necessary.

## License

Copyright © 2022 Yee Fay Lim.

Distributed under the Eclipse Public License version 1.0.
