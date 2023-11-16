(ns tablehike.core
  (:require [datahike.api :as d]
            [tablehike.parse.parser :as parser]
            [tablehike.parse.utils :as parse-utils]
            [tablehike.read :as csv-read]
            [tablehike.schema :as schema])
  (:import [clojure.lang Indexed IPersistentVector]
           [tablehike.read TakeReducer]))


(defn- csv-row->entity-map-parser [idents
                                   parsers
                                   {tuples :db.type/tuple
                                    composite-tuples :db.type/compositeTuple
                                    refs :db.type/ref}
                                   options]
  (let [string->vector (csv-read/string->vector-parser options)
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
                                                                (map :column-name))
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
                             (if (= (.length ^IPersistentVector vals) 1)
                               [ident (if-some [ref-ident (get ref-attr->ref-ident ident)]
                                        [ref-ident (.nth vals 0)]
                                        (.nth vals 0))]
                               [ident vals])))))
                  (comp (filter some?)))
              idents)))))


(defn load-csv
    "Reads, parses, and loads data from CSV file named `filename` into a Datahike database via
  the connection `conn`, with optional specifications in `schema-spec` and `options`.

  *Please note that the functionality (API and implementation) documented here will likely undergo
  major changes in the near future.*

  Each column represents an attribute, with keywordized column name as attribute ident, or
  otherwise, an element in a tuple. Type and cardinality are automatically inferred, though they
  sometimes require specification; in particular, cardinality many is well-defined and can only
  be inferred in the presence of a separate attribute marked as unique (`:db.unique/identity` or
  `:db.unique/value`).

  `schema-spec` can be used to specify schema fully or partially for attributes introduced by
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
  - `:batch-size`: the number of rows to read and transact per batch (default 128,000)
  - `:num-rows`: the number of rows in the CSV file
  - `:parser-fn`: a map specifying custom parsers, with key-value pairs of keywordized column name
  or index to `[dtype parser-fn]` tuple, with `dtype` being the appropriate key from in
  `tablehike.parse.parser/default-coercer`"
  ([filename conn]
   (load-csv filename conn {} {}))
  ([filename conn schema-spec]
   (load-csv filename conn schema-spec {}))
  ([filename conn schema-spec options]
   (let [parsers (parser/csv->parsers filename options)
         schema (schema/build-schema
                 parsers
                 schema-spec
                 (d/schema @conn)
                 (d/reverse-schema @conn)
                 (csv-read/csv->header-skipped-row-iter filename options)
                 options)
         csv-row->entity-map (csv-row->entity-map-parser (map :db/ident schema)
                                                         parsers
                                                         schema-spec
                                                         options)
         row-iter (csv-read/csv->header-skipped-row-iter filename options)
         num-rows (long (get options :batch-size
                             (get options :n-records
                                  (get options :num-rows 128000))))]
     ; TODO: fix "Could check for overlap with any existing schema, but I don't read minds"
     (d/transact conn schema)
     (loop [continue? (.hasNext row-iter)]
       (if continue?
         (do (->> {:tx-data (-> (fn [v row]
                                  (conj! v (csv-row->entity-map row)))
                                (reduce (transient [])
                                        (TakeReducer. row-iter num-rows))
                                persistent!)}
                  (d/transact conn))
             (recur (.hasNext row-iter)))
         @conn)))))


(comment
  (require '[clojure.java.io :as io]
           '[clojure.string :as string]
           '[charred.api :as charred]
           '[clojure.java.shell :as sh]
           '[datahike.api :as d]
           '[tablehike.core :as tbh])


  ; TODO test with regular (vector of maps) schema spec

  (def cfg (d/create-database {}))
  (def conn (d/connect cfg))
  (d/delete-database cfg)

  (def pokemon-file "test/data/pokemon.csv")
  (load-csv pokemon-file conn)
  (count (d/datoms @conn :eavt))
  (:abilities (d/entity @conn 50))
  (count (d/schema @conn))
  (->> (d/q '[:find ?n ?a
              :where [?e :japanese_name ?n]
              [?e :abilities ?a]]
            @conn)
       (reduce (fn [m [n a]]
                 (if (some? (get m n))
                   (update m n inc)
                   (assoc m n 1)))
               {}))
  (d/delete-database cfg)

  (def dial-311-file "test/data/311_service_requests_2010-present_sample.csv")
  (def db (load-csv dial-311-file
                    conn
                    {}
                    {:vector-open \(
                     :vector-close \)
                     :schema-sample-size 1280000
                     :parser-sample-size 1280000}))
                      ;:parser-fn {40 [:vector
                      ;                (fn [v]
                      ;                  (let [len (.length ^String v)
                      ;                        re (re-pattern ", ")]
                      ;                    (mapv (get parser/default-coercers :float32)
                      ;                          (-> (subs v 1 (dec len))
                      ;                              (str/split re)))))]}}


(def cfg (d/create-database {:backend :file
                                   :path "test/databases/vbb-db"}))
(def conn (d/connect cfg))
(def data-dir "/Users/yiffle/programming/code/vbb-gtfs/data")
(def latest-db (tbh/load-csv (io/file data-dir "agencies.csv") conn))
(def latest-db (tbh/load-csv (io/file data-dir "routes.csv") conn))
(def latest-db (tbh/load-csv (io/file data-dir "trips.csv")
                             conn
                             {}
                             {:parser-sample-size 242608}))
  )
