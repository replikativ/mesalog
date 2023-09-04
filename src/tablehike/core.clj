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
    "Reads, parses, and loads data from CSV file named `csv-file` into the Datahike database having
  (optionally specified) config `cfg`, with likewise optional schema-related options for attributes.
  Each column represents an attribute, with keywordized column name as attribute ident, or otherwise,
  an element in a heterogeneous or homogeneous tuple.

  THE FOLLOWING IS COMPLETELY OUTDATED; UPDATE TBD:

  If `cfg` is omitted, and the last argument:
  1. is also absent, or has empty `:schema`, `:ref-map`, and `:composite-tuple-map`, `cfg` is inferred to be `{:schema-flexibility :read}`.
  2. has a non-empty value for one or more of `:schema`, `:ref-map`, and `:composite-tuple-map`, `cfg` is inferred to be `{}`, i.e. the default value.

  `:schema` in the last argument can be specified in two ways:
  1. Full specification via the usual Datahike transaction data format, i.e. a vector of maps,
  each corresponding to an attribute.
  2. Partial specification via an abridged format like the map returned by `datahike.api/reverse-schema`,
  albeit with slightly different keys, each having a set of attribute idents as the corresponding value.
  Available options:

  | Key                 | Description   |
  |---------------------|---------------|
  | `:unique-id`        | `:db/unique` value `:db.unique/identity`
  | `:unique-val`       | `:db/unique` value `:db.unique/value`
  | `:index`            | `:db/index` value `true`
  | `:cardinality-many` | `:db/cardinality` value `:db.cardinality/many`

  Ref- and tuple-valued attributes, i.e. those with `:db/valueType` `:db.type/ref` or `:db.type/tuple`, are
  however specified separately, via `:ref-map`, `:tuple-map`, or `:composite-tuple-map`, each a map as follows:

  | Key                     | Description   |
  |-------------------------|---------------|
  | `:ref-map`              | `:db.type/ref` attribute idents to referenced attribute idents
  | `:composite-tuple-map`  | Composite `:db.type/tuple` attribute idents to constituent attribute idents
  | `:tuple-map`            | Other (homogeneous, heterogeneous) `:db.type/tuple` attribute idents to constituent attribute idents

  Unspecified schema attribute values are defaults or inferred from the data given.

  Example invocations:
  ``` clojure
  (load-csv csv-file)
  (load-csv csv-file dh-cfg)
  (load-csv csv-file dh-cfg {:schema [{:db/ident :name
                                       ...}
                                      ...]
                             :ref-map {...}
                             :tuple-map {...}
                             :composite-tuple-map {...}})
  (load-csv csv-file dh-cfg {:schema {:unique-id #{...}
                                       ...}
                             :ref-map {...}
                             :tuple-map {...}
                             :composite-tuple-map {...}})
  ```

  Please see README for more detail."
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
