(ns tablehike.core
  (:require [clojure.set :as clj-set]
            [datahike.api :as d]
            [tablehike.parse.parser :as parser]
            [tablehike.parse.utils :as parse-utils]
            [tablehike.parse.datetime :as dt]
            [tablehike.read :as csv-read]
            [tablehike.schema :as schema])
  (:import [clojure.lang Indexed PersistentVector]
           [tablehike.read TakeReducer]))


(defn- csv-row->entity-map-parser [idents
                                   parsers
                                   {tuples :db.type/tuple
                                    composite-tuples :db.type/compositeTuple
                                    refs :db.type/ref}
                                   options]
  (let [string->vector (parse-utils/vector-str->elmt-strs-fn options)
        parse-fns (-> #(if (= :vector (:parser-dtype %))
                         (let [field-parsers (:field-parser-data %)
                               field-dtypes (mapv :parser-dtype field-parsers)]
                           (if (parse-utils/homogeneous-sequence? field-dtypes)
                             (let [parse-fn (:parse-fn (nth field-parsers 0))]
                               (fn [v]
                                 (->> (string->vector v)
                                      (into [] (map parse-fn)))))
                             (let [v-parse-fns (mapv :parse-fn field-parsers)]
                               (fn [v]
                                 (->> (string->vector v)
                                      (into [] (map-indexed
                                                (fn [i s]
                                                  ((nth v-parse-fns i) s)))))))))
                         (:parse-fn %))
                      (mapv parsers))
        ref-attr->ref-ident (when (map? refs)
                              (let [vector-val-cols (into #{}
                                                          (comp (remove #(= (:parser-dtype %)
                                                                            :vector))
                                                                (map :column-ident))
                                                          parsers)]
                                (into {}
                                      (comp (map (fn [[a rid]]
                                                   (when (contains? vector-val-cols a)
                                                     [a rid])))
                                            (filter some?))
                                      refs)))
        idents (remove #(-> (set (keys composite-tuples))
                            (contains? %))
                       idents)
        ident->indices (parse-utils/map-idents->indices idents parsers tuples)]
    (fn [row]
      (let [parsed-vals (-> (fn [v i av]
                              (conj! v (if (csv-read/missing-value? av)
                                         nil
                                         ((.nth ^Indexed parse-fns i) av))))
                            (reduce-kv (transient []) row)
                            persistent!)]
        (into {}
              (-> (map (fn [ident]
                         (let [^Indexed vals (->> (get ident->indices ident)
                                                  (into [] (map #(nth parsed-vals %))))]
                           (when (every? some? vals)
                             (if (= (.length ^PersistentVector vals) 1)
                               [ident (if-some [ref-ident (get ref-attr->ref-ident ident)]
                                        [ref-ident (.nth vals 0)]
                                        (.nth vals 0))]
                               [ident vals])))))
                  (comp (filter some?)))
              idents)))))


(defn load-csv
    "Reads, parses, and loads data from CSV file named `filename` into a Datahike database via
  the connection `conn`, with optional specifications in `parsers-desc`, `schema-desc` and `options`.

  *Please note that the functionality (API and implementation) documented here, in particular
  aspects related to schema specification/inference and its interface with parser specification/inference,
  is still evolving and will undergo changes, possibly breaking, in the future.*

  Each column represents an attribute, with keywordized column name as default attribute ident, or
  otherwise, an element in a tuple. Type and cardinality are automatically inferred, though they
  sometimes require specification; in particular, cardinality many is well-defined and can only
  be inferred in the presence of a separate attribute marked as unique (`:db.unique/identity` or
  `:db.unique/value`).

  Please see the docstring for `infer-parsers` for detailed information on `parsers-desc`.

  `schema-desc` can be used to specify schema fully or partially for attributes introduced by
  `filename`. It may be:

  1. A map, for partial specification: using schema attributes or schema attribute values as keys,
  each with a collection of attribute idents or keywordised column names as its corresponding value,
  in the following forms:

  *Key:* Any of `:db/isComponent`, `:db/noHistory`, and `:db/index`
  *Value:* Set of attribute idents
  *Description:* Denotes a schema attribute value of `true`
  *Example:* `{:db/index #{:name}}` denotes a `:db/index` value of `true` for attribute `:name`

  *Key:* Any element of the sets `:db.type/value`, `:db.type/cardinality`, and `:db.type/unique`
  from namespace `datahike.schema`, except `:db.type.install/attribute`
  *Value:* Set of attribute idents
  *Description:* The key denotes the corresponding schema attribute value for the attributes named
  in the value. `:db.type/tuple` and `:db.type/ref` attributes have two possible forms of specification.
  In this form, each attribute must correspond to a self-contained column, i.e. consist of sequences
  for tuples, and lookup refs or entity IDs for refs. The other form is described below.
  *Examples:*
  `{:db.type/keyword #{:kw}}` denotes `:db/valueType` `:db.type/keyword` for attribute `:kw`.
  `{:db.cardinality/many #{:orders}}` denotes `:db/cardinality` `:db.cardinality/many` for `:orders`.
  `{:db.unique/identity #{:email}}` denotes `:db/unique` `:db.unique/identity` for `:email`.

  *Key:* `:db.type/ref`
  *Value:* Map of ref-type attribute idents to referenced attribute idents
  *Description:* Each key-value pair maps a ref-type attribute to an attribute which uniquely
  identifies referenced entities
  *Example:* `{:db.type/ref {:parent-station :station-id}}` denotes that the ref-type attribute
  `:parent-station` references entities with the unique identifier attribute `:station-id`

  *Key:* `:db.type/tuple`
  *Value:* Map of tuple attribute ident to sequence of keywordized column names
  *Description:* Each key-value pair denotes a tuple attribute and the columns representing its elements
  *Example:* `{:db.type/tuple {:abc [:a :b :c]}}` denotes that the tuple attribute `:abc` consists of
  elements with values represented in columns `:a`, `:b`, and `:c`

  *Key:* `:db.type/compositeTuple` (a keyword not used in Datahike, but that serves here as a
  shorthand to distinguish composite and ordinary tuples)
  *Value:* Map of composite tuple attribute ident to constituent attribute idents (keywordized
  column names)
  *Description:* Each key-value pair denotes a composite tuple attribute and its constituent
  attributes (each corresponding to a column)
  *Example:* `{:db.type/compositeTuple {:abc [:a :b :c]}}`: the composite tuple attribute `:abc`
  consists of attributes (with corresponding columns) `:a`, `:b`, and `:c`

  2. A vector of maps, of the form used for schema specification in Datahike. Still not well supported:
  besides `:db/ident`, `:db/cardinality` (which is required) for each attribute must be specified, though
  type is inferred if omitted.

  Lastly, `options` supports the following keys:
  - `:batch-size`: The number of rows to read and transact per batch (default `128,000`).
  - `:num-rows`: The number of rows in the CSV file.
  - `:separator`: Separator character for CSV row entries. Defaults to `,`.
  - `:parser-sample-size`: Number of rows to sample for type (parser) inference. Defaults to `Long/MAX_VALUE`.
  - `:vector-delims-use`: Whether vector-valued entries are delimited, e.g. by square brackets (`[]`).
  Defaults to `true`.
  - `:vector-open-char`: Left delimiter for vector values, only applicable if `:vector-delims-use` is `true`.
  Default: `[`.
  - `:vector-close-char`: Right delimiter for vector values, only applicable if `:vector-delims-use` is `true`.
  Default: `]`.
  - `:vector-separator`: Separator character for elements in vector-valued entries, analogous to `:separator`
  (default `,`) for CSV row entries. Defaults to the same value as that of `:separator`.
  - `:include-cols`: Predicate for whether a column should be included in the data load. Columns can be
  specified using valid index values, strings, or keywords. Example: `#{1 2 3}`.
  - `:idx->colname`: Function taking the 0-based index of a column and returning name. Defaults to the
  value at the same index of the column header if present, otherwise `(str \"column-\" idx)`.
  - `:colname->ident`: Function taking the name of a column and returning a keyword, based on the convention of each
  column representing an attribute, and keywordized column name as default attribute ident. The returned value is assumed
  to be the corresponding ident for each column representing an attribute, though it can also apply to columns for which
  that is not the case. Defaults to the keywordized column name, with consecutive spaces replaced by a single hyphen."
  ([filename conn parsers-desc schema-desc options]
   (let [colname->ident (parser/colname->ident-fn options)
         parsers (mapv (fn [p]
                         ;; TODO revisit: stopgap in lieu of proper interface
                         ;; between DB attr idents and col names
                         (update (->> (colname->ident (:column-name p))
                                      (assoc p :column-ident))
                                 :parser-dtype
                                 ;; TODO revisit when developing better parser-schema iface
                                 #(if (contains? dt/datetime-datatypes %)
                                    :db.type/instant
                                    %)))
                       (parser/infer-parsers filename parsers-desc options))
         ;; TODO take into account in `parser` namespace (drop dtype requirement from parser descriptions)
         schema-on-read (= (:schema-flexibility (:config @conn))
                           :read)
         existing-schema (d/schema @conn)
         schema (when (not schema-on-read)
                  (schema/build-schema
                   parsers
                   schema-desc
                   existing-schema
                   (d/reverse-schema @conn)
                   (csv-read/csv->header-skipped-row-iter filename options)
                   options))
         shared-keys #(clj-set/intersection (set (keys %1))
                                            (set (keys %2)))
         schema-map (when (some? schema)
                      (reduce (fn [m s]
                                (assoc m (:db/ident s) s))
                              {}
                              schema))
         shared-attrs (when (some? schema)
                        (shared-keys existing-schema schema-map))
         _ (doseq [k1 shared-attrs]
             (let [existing-k1 (get existing-schema k1)
                   schema-k1 (get schema-map k1)]
               (doseq [k2 (shared-keys existing-k1 schema-k1)]
                 (let [old (get existing-k1 k2)
                       new (get schema-k1 k2)]
                   (when (not= old new)
                     (-> "Inconsistency between existing and specified/inferred schema: %s %s"
                         (format old new)
                         IllegalArgumentException.
                         throw))))))
         csv-row->entity-map (csv-row->entity-map-parser (if schema-on-read
                                                           (map :column-ident parsers)
                                                           (map :db/ident schema))
                                                         parsers
                                                         (when (not schema-on-read)
                                                           schema-desc)
                                                         options)
         row-iter (csv-read/csv->header-skipped-row-iter filename options)
         num-rows (when-some [num-rows (get options :num-rows)]
                    (when (and (number? num-rows)
                               (>= num-rows 0))
                      (long num-rows)))
         batch-size (let [batch-size (long (get options :batch-size 128000))]
                      (if num-rows
                        (min batch-size num-rows)
                        batch-size))]
     (when (not schema-on-read)
       (let [keep-attrs (clj-set/difference (set (keys schema-map))
                                            shared-attrs)]
         (->> (filter #(contains? keep-attrs (:db/ident %))
                      schema)
              (d/transact conn))))
     (loop [continue? (.hasNext row-iter)
            rows-loaded 0
            rows-left num-rows]
       (if (and continue?
                (or (nil? rows-left)
                    (> rows-left 0)))
         (do (->> {:tx-data (-> (fn [v row]
                                  (conj! v (csv-row->entity-map row)))
                                (reduce (transient [])
                                        (TakeReducer. row-iter batch-size))
                               persistent!)}
                  (d/transact conn))
             (recur (.hasNext row-iter)
                    (unchecked-add rows-loaded batch-size)
                    (when rows-left
                      (unchecked-subtract rows-left batch-size))))
         @conn))))
  ([filename conn parsers-desc schema-desc]
   (load-csv filename conn parsers-desc schema-desc {}))
  ([filename conn parsers-desc]
   (load-csv filename conn parsers-desc {} {}))
  ([filename conn]
   (load-csv filename conn {} {} {})))


(defn infer-parsers
  "`parsers-desc` can be used to specify parsers, with the description for each column containing its
  data type(s) as well as parser function(s).

  For a scalar-valued column, this takes the form ~[dtype fn]~, which can (currently) be specified in
  one of these two ways:
  - A default data type, say ~d~, as shorthand for ~[d (d tablehike.parse.parser/default-coercers)]~,
  with the 2nd element being its corresponding default parser function. The value of ~d~ must come from
  `(keys tablehike.parse.parser/default-coercers)`.
  - In full, as a two-element tuple of type and (custom) parser, e.g.
  `[:db.type/long #(long (Float/parseFloat %))]`.

  For a vector-valued column (whatever the ~:db/valueType~ of its corresponding attribute, if any), the
  following forms are possible:
  - ~[dtype parse-fn]~ (not supported for tuples)
  - ~[[dt1 dt2 ...]]~, if ~dt1~ etc. are all data types having default parsers
  - ~[[dt1 dt2 ...] [pfn1 pfn2 ...]]~, to specify custom parser functions.

  `parsers-desc` can be specified as:
  - A map with each element consisting of the following:
    - Key: a valid column identifier (see above)
    - Value: a parser description taking the form described above.
  - A vector specifying parsers for consecutive columns, starting from the 1st (though not necessarily ending
  at the last), with each element again being a parser description taking the form above, just like one given
  as a map value.

  Please see test namespace `tablehike.parser-test` for usage examples."
  ([filename parsers-desc options]
   (parser/infer-parsers filename parsers-desc options))
  ([filename parsers-desc]
   (infer-parsers filename parsers-desc {}))
  ([filename]
   (infer-parsers filename {} {})))
