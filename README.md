# datahike-csv-loader

Loads CSV data into [Datahike](https://datahike.io) with a single function call.

A summary of the information below is also available: [![cljdoc badge](https://cljdoc.org/badge/io.replikativ/datahike-csv-loader)](https://cljdoc.org/d/io.replikativ/datahike-csv-loader)

## Usage

``` clojure
(require '[datahike.api :as d]
               '[datahike-csv-loader.core :as dcsv])
(d/create-database)
(def conn (d/connect))
(dcsv/load-csv conn "data.csv")
;; or (map contents elided here and described below)
(def col-schema {...})
(dcsv/load-csv conn "data.csv" col-schema)
```

Reads, parses, and loads data from `data.csv` into the database pointed to by `conn`, with schema for the corresponding attributes optionally specified in map `col-schema`. Each column represents an attribute, with keywordized column name as attribute ident, or an element in a heterogeneous or homogeneous tuple (more on tuples below).

`col-schema` expects a set of attribute idents as the value of each key, except `:ref` and `:tuple`. The available options are:

  | Key                 | Description   |
  |---------------------|---------------|
  | `:unique-id`        | `:db/unique` value `:db.unique/identity`
  | `:unique-val`       | `:db/unique` value `:db.unique/value`
  | `:index`            | `:db/index` value `true`
  | `:cardinality-many` | `:db/cardinality` value `:db.cardinality/many`
  | `:ref`              | Map of `:db/valueType` `:db.type/ref` attributes to referenced attribute idents
  | `:tuple`            | Map of `:db/valueType` `:db.type/tuple` attributes to constituent attribute idents

Each file is assumed to represent attributes for one entity "type", whether new or existing: e.g. a student with columns _student/name_, _student/id_. This also means that attribute data for a single "type" can be loaded from multiple files: for example, another file with columns _student/id_ and _student/course_ can be loaded later. Attribute schema can be partially specified via `col-schema`: for example, a value of `#{:user/email :user/acct-id}` for the key `:unique-id` indicates that the attributes in the set are unique identifiers. That said, except with `:db.type/ref` and `:db.type/tuple`, `:db/valueType` is inferred. Note also that only one cardinality-many attribute is allowed per file for semantic reasons.

### Ref-valued attributes

Data in a reference-valued attribute column must consist of domain identifier (i.e. an attribute with `:db.unique/identity`) values for entities already present in the database; these are automatically converted into entity IDs. For example:

``` clojure
(d/transact conn [{:db/ident :course/id
                         :db/unique :db.unique/identity
                         ...}])
(d/transact conn [{:course/id "CMSC101"
                   :course/name "Intro. to CS"}
                   ...])
(dcsv/load-csv conn "students.csv" {:unique-id #{:student/id}
                                          :cardinality-many #{:student/course}
                                          :ref {:student/course :course/id}})
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

### Tuple attributes

First: an [introduction](https://docs.datomic.com/on-prem/schema/schema.html#tuples) to tuples for the uninitiated.

datahike-csv-loader supports the three kinds of tuples available in Dathaike (as in Datomic, for which the documentation just linked to is written): composite, heterogeneous, and homogeneous. They should be specified in a map, with each tuple attribute ident as a key with a vector of constituent attribute idents. For example, roughly working off [this schema definition](https://docs.datomic.com/on-prem/schema/schema.html#composite-tuples) and supposing a CSV file with columns _student/id_, _course/id_, and _semester/year+season_:
``` clojure
(def col-schema {:tuple {:reg/semester+course+student
                         [:student/id :course/id :semester/year+season]}
                 ...})
```

And another, supposing a CSV file including columns _station/lat_ and _station/lon_:
``` clojure
(def col-schema {:tuple {:station/coordinates [:station/lat :station/lon]}
                 ...})
})
```

As with attribute `db/valueType` in general, the `db/valueType` of tuple elements is inferred. The kind of each tuple itself is also inferred, using rules illustrated by the following clojure-ish pseudocode:
``` clojure
(if (tuple-ident (:unique-id col-schema))
  :composite
  (if (->> (tuple-ident (:tuple col-schema))
           (map valtype)
           (apply = ))
    :homogeneous
    :heterogeneous))
```

Note that the schema definitions of these tuple types imply that columns belonging to a composite tuple will be individually retained as attributes (with the tuple being automatically transacted by the database), while those belonging to other kinds will be subsumed into their respective tuples--for example, the data in columns _station/lat_ and _station/lon_ would be merged into the tuple attribute _:station/coordinates_, but the database would not contain the attributes _:station/lat_ and _:station/lon_.

## Current limitations

datahike-csv-loader currently:
1. Assumes that the columns of any given CSV file represent attributes that do not yet exist in the database, i.e. it isn't possible to add data for existing attributes.
2. Apart from any specification passed in `col-schema` to `load-csv`, automatically infers attribute schema, i.e. complete user specification of the schema isn't supported.

We hope to address these shortcomings if they prove to be substantial.

## License

Copyright © 2022 Yee Fay Lim.

Distributed under the Eclipse Public License version 1.0.
