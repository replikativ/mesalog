
# Table of Contents

1.  [TL;DR](#org85d77cc)
    1.  [Presentations](#orgd12de98)
2.  [Acknowledgements](#orgb62687c)
3.  [Quickstart](#org8158cc7)
4.  [Columns and attributes](#org7d0496c)
5.  [Column identifiers](#orgbbfcdd3)
6.  [Including and excluding columns](#org350d346)
7.  [Supported column data types](#orgdb4c5b3)
8.  [Parsers vs. schema](#orga39dbe3)
9.  [Parser descriptions](#org43359b6)
10. [Schema description](#org48833d3)
11. [Schema-on-read](#orgb12e02f)
12. [Cardinality inference](#orgce16e08)
13. [Attributes already in schema](#org9bb8c3f)
14. [Reference-type attributes (with `:db/valueType` `:db.type/ref`)](#orgf305146)
15. [Vector-valued columns](#org4c040c4)
16. [Tuples](#org878de7d)
17. [Options](#org8951e89)
18. [More examples](#orgd3dd8ae)
19. [Current limitations](#org61f1dd3)
20. [License](#orgb6941dd)


<a id="org85d77cc"></a>

# TL;DR

Loads CSV data into Datalog databases with (for now) a single function call.

-   Handles arbitrarily large files by default
-   Offers both automatic inference and user specification of
    -   Parsers (types)
    -   Schema
    -   Cardinality
-   Automatic type inference and parsing of relatively simple datetime values
-   Automatic vector/tuple value detection and parsing
    -   E.g. `"[1,2,3]"` -> `[1 2 3]`
-   Not too slow (improvements soon with any luck): ~45s per million rows to parse and infer schema.
    -   This is mostly for the record; it likely still leaves database transactions of the data as the performance bottleneck for most backends (though only Datahike is currently supported).
    -   See `tablehike.demo` namespace for details.


<a id="orgd12de98"></a>

## Presentations

Note: substantial overlap in content, though the earlier talk is somewhat more conceptually comprehensive
[Clojure Berlin lightning talk, December 2023](https://docs.google.com/presentation/d/10mCViOX9Lkmxi8t0V7vnTLNIdoToPfVMZIUxW48onUQ/edit?usp=sharing)
[Austin Clojure Meetup, November 2023](https://docs.google.com/presentation/d/1LotuOmUVs5bVAhMiCt8xHyQoI-CfsB2gCaYkPmvZx4k/edit?usp=sharing)


<a id="orgb62687c"></a>

# Acknowledgements

Much of the code in `src/tablehike/parse` is adapted from the library [tech.ml.dataset](https://github.com/techascent/tech.ml.dataset).


<a id="org8158cc7"></a>

# Quickstart

[![Clojars Project](https://img.shields.io/clojars/v/io.replikativ/tablehike.svg)](https://clojars.org/io.replikativ/tablehike) [![cljdoc badge](https://cljdoc.org/badge/io.replikativ/tablehike)](https://cljdoc.org/d/io.replikativ/tablehike)


Reads, parses, and loads data from `filename` into a Datahike database via connection `conn`. The remaining arguments are optional: parser descriptions, schema description, and options for other relevant specifications.

    (require '[datahike.api :as d]
             '[tablehike.core :as t])
    
    (def cfg (d/create-database))
    (def conn (d/connect cfg))
    
    ;; 2-ary (basic)
    (th/load-csv filename conn)
    ;; 3-ary
    (th/load-csv filename conn parser-descriptions)
    ;; 4-ary
    (th/load-csv filename conn parser-descriptions schema-description)
    ;; 5-ary
    (th/load-csv filename conn parser-descriptions schema-description options)

Where `parser-descriptions` can be:

    {}
    ;; or
    []
    ;; or a map of valid column identifiers (nonnegative indices, strings, or keywords) to
    ;; default parser function identifiers (generally though not always database type identifiers)
    ;; or two-element tuples of database type identifier and parser function.
    ;; Example, referring to `data/stops-sample.csv`:
    {0 :db.type/string
     4 :db.type/float ; 1. Would default to `:db.type/double` otherwise. 2. Maps to default parser for floats.
     5 :db.type/float
     8 [:db.type/boolean #(identical? % 0)]}
    ;; or equivalently
    {"stop/id" :db.type/string
     "stop/lat" :db.type/float
     "stop/lon" :db.type/float
     "stop/wheelchair-boarding" [:db.type/boolean #(identical? % 0)]}
    ;; or equivalently (since each column corresponds to an attribute in this case)
    {:stop/id :db.type/string
     :stop/lat :db.type/float
     :stop/lon :db.type/float
     :stop/wheelchair-boarding [:db.type/boolean #(identical? % 0)]}
    ;; or a vector specifying parsers for consecutive columns, starting from the 1st
    ;; (though not necessarily ending at the last)
    ;; E.g. based on `data/shapes.csv` with header and first row as follows:
    ;; shape/id,shape/pt-lat,shape/pt-lon,shape/pt-sequence
    ;; 185,52.296719,13.631534,0
    (let [parse-fn #(-> (.setScale (bigdec %) 3 java.math.RoundingMode/HALF_EVEN)
                        float)
          dtype-parser [:db.type/float parse-fn]]
      [:db.type/long dtype-parser dtype-parser :db.type/bigint])

And `schema-description` can be (again referring to the same sample data):

    {}
    ;; or
    []
    ;; or
    {:db/index #{:stop/name} ; `:stop/name` has a schema `:db/index` value of `true`
     :db.type/float #{:stop/lat :stop/lon} ; note: redundant / synonymous with parser specifications above
     :db.unique/identity #{:stop/id}
     :db.type/ref {:stop/parent-station :stop/id}} ; parent-station attr references id attr

And `options` can be:

    {}
    ;; or (context-free example)
    {:batch-size 50000
     :num-rows 1000000
     :separator \tab
     :parser-sample-size 20000
     :include-cols #{0 2 3} ; can also be (valid, column-referencing) strings or keywords
     :header-row? false}


<a id="org7d0496c"></a>

# Columns and attributes

Each column represents either of the following:

1.  An attribute, with keywordized column name as default attribute ident.
2.  An element in a heterogeneous or homogeneous tuple.


<a id="orgbbfcdd3"></a>

# Column identifiers

Columns can be identified by (nonnegative, 0-based) index, name (string-valued), or keyword (&ldquo;ident&rdquo;).

-   String-valued name: Defaults to the value at the same index of the column header if present, otherwise `(str "column-" index)`. A custom index-to-name function can be specified via the option `:idx->colname`.
-   Keyword: Based on the convention of each column representing an attribute, and keywordized column name as default attribute ident. Defaults to the keywordized column name, with consecutive spaces replaced by a single hyphen.
    A custom name-to-keyword function can be specified via the option `:colname->ident`.

All three forms of identifier are supported in parser descriptions and the `:include-cols` option. Unfortunately, that isn&rsquo;t yet the case for the schema description; apologies.


<a id="org350d346"></a>

# Including and excluding columns

By default, data from all columns are loaded. If not, whether a column should be included or excluded can be specified via a predicate in the `:include-cols` option.


<a id="orgdb4c5b3"></a>

# Supported column data types

    tablehike.parse.parser/supported-dtypes
    ;; i.e.
    #{:db.type/number
      :db.type/instant
      :db.type/tuple
      :db.type/boolean
      :db.type/uuid
      :db.type/string
      :db.type/keyword
      :db.type/ref
      :db.type/bigdec
      :db.type/float
      :db.type/bigint
      :db.type/double
      :db.type/long
      :db.type/symbol
      :local-date-time
      :zoned-date-time
      :instant
      :offset-date-time
      :local-date}


<a id="orga39dbe3"></a>

# Parsers vs. schema

**Parser**: Interprets the values in a CSV column (field). Each included column has a parser, whether specified or inferred.
**Schema** (on write): Explicitly defines data model.

Note that some databases (including Datahike) support both *schema-on-read* (no explicitly defined data model) and *schema-on-write* (the default, described above). The schema description (4th) argument to `load-csv` is only relevant with schema-on-write, and irrelevant to schema-on-read.


<a id="org43359b6"></a>

# Parser descriptions

Column data types (and their corresponding parsers) can be automatically inferred, except where the column:

-   Is not self-contained, and corresponds to an attribute with `:db/valueType` being one of these:
    -   `:db.type/ref`: column values belong to another attribute
        -   E.g. each value in column `"station/parent-station"` references another (parent) station via the latter&rsquo;s `:station/id` attribute value
    -   `:db.type/tuple`: column values belong to a tuple
        -   E.g. attribute `:abc` is tuple-valued, with the elements of each tuple coming from columns `"a"`, `"b"`, and `"c"`
-   Has values that are otherwise too non-standard for automatic type inference.

`load-csv` accepts parser descriptions as its 3rd argument, with the description for each column containing its data type(s) as well as parser function(s). For a scalar-valued column, this takes the form `[dtype fn]`, which can (currently) be specified in one of these two ways:

-   A default data type, say `d`, as shorthand for `[d (d tablehike.parse.parser/default-coercers)]`, with the 2nd element being its corresponding default parser function. The value of `d` must come from:
    
        (set (keys tablehike.parse.parser/default-coercers))
        ;; i.e.
        #{:db.type/number
          :db.type/instant
          :db.type/boolean
          :db.type/uuid
          :db.type/string
          :db.type/keyword
          :db.type/float
          :db.type/bigint
          :db.type/double
          :db.type/long
          :db.type/symbol
          :local-date-time
          :zoned-date-time
          :instant
          :offset-date-time
          :local-date}
-   In full, as a two-element tuple of type and (custom) parser, e.g. `[:db.type/long #(long (Float/parseFloat %))]`.

Parser descriptions can be specified as:

-   A map with each element consisting of the following:
    -   Key: a valid column identifier (see above)
    -   Value: a parser description taking the form described above.
-   A vector specifying parsers for consecutive columns, starting from the 1st (though not necessarily ending at the last), with each element again being a parser description taking the form above, just like one given as a map value.

See the section [Vector-valued columns](#vector-valued-columns) for details on specifying parser descriptions for vector-valued columns.


<a id="org48833d3"></a>

# Schema description

Schema can be fully or partially specified for attributes introduced by the input CSV, via the 4th argument to `load-csv`. (It can also be specified for existing attributes, but any conflict with the existing schema, whether specified or inferred, will currently result in an error, even if the connected database supports the corresponding updates.)

The primary form currently supported for providing a schema description is a map, with each key-value pair having the following possible forms:

1.  **Key:** Schema attribute, e.g. `:db/index`
    **Value:** Set of attribute idents
    **E.g.:** `{:db/index #{:name}}`
2.  **Key:** Schema attribute value, e.g. `:db.type/keyword`, `:db.cardinality/many`
    **Value:** Set of attribute idents
    **E.g.:** `{:db.cardinality/many #{:orders}}`
3.  **Key:** `:db.type/ref`
    **Value:** Map of ref-type attribute idents to referenced attribute idents
    **E.g.**: `{:db.type/ref {:station/parent-station :station/id}}`
4.  **Key:** `:db.type/tuple`
    **Value:** Map of tuple attribute ident to sequence of keywordized column names
    **E.g.:** `{:db.type/tuple {:abc [:a :b :c]}}`
5.  **Key:** `:db.type/compositeTuple` (a keyword not used in Datahike, but that serves here as a shorthand to distinguish composite and ordinary tuples)
    **Value:** Map of composite tuple attribute ident to constituent attribute idents (keywordized column names)
    **E.g.:** `{:db.type/compositeTuple {:abc [:a :b :c]}}`

(3), (4), and (5) are specifically type-related, but seem more easily specified as part of the schema description instead of parser descriptions.

Please see `load-csv` docstring for further detail.


<a id="orgb12e02f"></a>

# Schema-on-read

Tablehike supports schema-on-read databases, though not thoroughly, as noted in [Current limitations](#current-limitations) below.


<a id="orgce16e08"></a>

# Cardinality inference

Note that cardinality many can only be inferred in the presence of a separate attribute marked as unique (`:db.unique/identity` or `:db.unique/value`).


<a id="org9bb8c3f"></a>

# Attributes already in schema

Tablehike currently supports loading data for existing attributes, as long as their schema remains the same; unfortunately, it doesn&rsquo;t yet support schema updates even where allowed by the connected database. As stated above, any conflict with the existing schema, whether specified or inferred, will currently result in an error.


<a id="orgf305146"></a>

# Reference-type attributes (with `:db/valueType` `:db.type/ref`)

Examples above illustrate one way reference-type attributes can be represented in CSV. Another way is possible, via a tuple-valued field (column), e.g. the column `"station/parent-station"` could have values like `[:station/id 12345]` instead of `12345`. In this case, the column would be self-contained, and assuming valid tuple-valued references throughout the parser inference row sample:

-   `:db.type/ref` would be inferred as its `:db/valueType`.
-   Type specification is unnecessary: `{:db.type/ref {:station/parent-station :station/id}}` can be dropped.


<a id="org4c040c4"></a>

# Vector-valued columns

The parser description for a vector-valued column (whatever the `:db/valueType` of its corresponding attribute, if any) can be specified in one of a few ways:

-   `[dtype parse-fn]` (not supported for tuples)
-   `[[dt1 dt2 ...]]`, if `dt1` etc. are all data types having default parsers
-   `[[dt1 dt2 ...] [pfn1 pfn2 ...]]`, to specify custom parser functions.

A shorthand form for homogeneous vectors, e.g. `[[dt] [pfn]]`, `[[dt]]`, or maybe even `[dt]`, isn&rsquo;t yet supported.


<a id="org878de7d"></a>

# Tuples

For the uninitiated: an [introduction](https://docs.datomic.com/on-prem/schema/schema.html#tuples) to tuples.

Instead of being represented across columns as illustrated above, (homogeneous and heterogeneous, but not composite) tuples can also be represented by vector values. For example, a value of `[1 2 3]` for tuple `:abc` can be represented as such within a single column, say `"abc"`, instead of across 3 columns, 1 for each element. In this case:

1.  Its specification as tuple, e.g. `{:db.type/tuple {:abc [:a :b :c]}}`, can be dropped from the schema description.
2.  Its type and parser may be inferred or specified:
    -   If `:abc` is a homogeneous tuple of uniform length, its type and parser can be automatically inferred.
    -   The parser description for `"abc"` can take one of the forms described above for [Vector-valued columns](#vector-valued-columns), except `[dtype parse-fn]` as noted.

Note: Type and parser can also be inferred for heterogeneous tuples, but they must have uniform length (regardless of type inference needs).


<a id="org8951e89"></a>

# Options

Supported options: `:batch-size`, `:num-rows`, `:separator`, `:parser-sample-size`, `:include-cols`, and `:header-row?`. See `load-csv` docstring for more, including `:idx->colname`, `:colname->ident`, and vector-related options.


<a id="orgd3dd8ae"></a>

# More examples

See test namespaces and the `tablehike.demo` namespace for more examples.


<a id="org61f1dd3"></a>

# Current limitations

Many if not most of the remaining major limitations of Tablehike are due to the continuing (even if much decreased) presence of coupling between parsers and schema, and current lack of a clean separation and coherent interface between them. For example:

-   The parser descriptions argument to `load-csv` still requires column type specification, even when it is irrelevant because the connected database has schema-on-read.
-   More importantly:
    -   *Consistency between the parsers and schema ultimately used for data load and transaction is not checked*.
    -   The current API only supports a single-step workflow, without a multi-step option as well, that would allow verification of inferred parsers and schema before data transaction.

However, at least one such limitation not attributable to the lacking parser-schema interface exists: currently, only [Datahike](https://datahike.io) (see also [GitHub](https://github.com/replikativ/datahike)) is supported, though that shall be extended to other databases once the API and implementation have matured.


<a id="orgb6941dd"></a>

# License

Copyright © 2022-2023 Yee Fay Lim

Distributed under the Eclipse Public License version 1.0.

