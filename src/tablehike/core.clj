(ns tablehike.core
  (:require [clojure.string :as str]
            [datahike.api :as d]
            [tablehike.parse.parser :as parser]
            [tablehike.parse.datetime :as dt]
            [tablehike.parse.utils :as parse-utils]
            [tablehike.read :as csv-read]
            [tablehike.schema :as schema])
  (:import [clojure.lang Indexed IPersistentVector IReduceInit]
           [java.util Iterator List]
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
  ([csv-file]
   (load-csv csv-file {} {} {}))
  ([csv-file cfg]
   (load-csv csv-file cfg {} {}))
  ([csv-file cfg schema-spec]
   (load-csv csv-file cfg schema-spec {}))
  ([csv-file cfg schema-spec options]
   (let [parsers (parser/csv->parsers csv-file options)
         cfg (or cfg {})
         cfg (when-not (d/database-exists? cfg)
               (d/create-database cfg))
         conn (d/connect cfg)
         schema (schema/build-schema parsers
                                     schema-spec
                                     (d/schema @conn)
                                     (d/reverse-schema @conn)
                                     (csv-read/csv->header-skipped-row-iter csv-file options)
                                     options)
         csv-row->entity-map (-> (map :db/ident schema)
                                 (csv-row->entity-map-parser parsers schema-spec options))
         row-iter (csv-read/csv->header-skipped-row-iter csv-file options)
         num-rows (long (get options :batch-size
                             (get options :n-records
                                  (get options :num-rows 128000))))]
                                        ; Could check for overlap with any existing schema, but I don't read minds
     (d/transact conn schema)
     (loop [continue? (.hasNext row-iter)]
       (when continue?
         (do (->> {:tx-data (-> (fn [v row]
                                  (conj! v (csv-row->entity-map row)))
                                (reduce (transient [])
                                        (TakeReducer. row-iter num-rows))
                                persistent!)}
                  (d/transact conn))
             (recur (.hasNext row-iter)))))
     cfg)))


(comment
  (require '[clojure.data.csv :as csv]
           '[clojure.java.io :as io]
           '[charred.api :as charred])
           ;'[tablecloth.api :as tc])

  ; TODO return nil type (or something else appropriate) if all missing!
  ; TODO test with regular (vector of maps) schema spec
  (def cfg (load-csv "resources/agencies.csv"))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  (def cfg (load-csv "resources/agencies.csv" {} {:db.unique/identity #{:agency/id}
                                                  :db.unique/value #{:agency/name :agency/url}}))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  (def cfg (load-csv "resources/levels.csv"))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  (def cfg (load-csv "resources/route_trips.csv"))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  ; test missing->nil!
  (def cfg (load-csv "resources/routes.csv"))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  (def cfg (load-csv "resources/shapes.csv"))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  (def cfg (load-csv "resources/shapes.csv"
            {}
            {:db.type/compositeTuple {:shape/key [:shape/id :shape/pt-sequence]}
             :db.unique/identity #{:shape/key}}))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  (def cfg (load-csv "resources/shapes.csv"
            {}
            {:db.type/compositeTuple {:shape/coordinates [:shape/pt-lat :shape/pt-lon]}}))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  (def cfg (load-csv "resources/shapes.csv"
            {}
            {:db.type/compositeTuple {:shape/key [:shape/id :shape/pt-sequence]
                                      :shape/coordinates [:shape/pt-lat :shape/pt-lon]}
             :db.unique/identity #{:shape/key}}))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  (def cfg (load-csv "resources/shapes.csv"
            {}
            {:db.unique/identity #{:shape/id}}))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  (def cfg (load-csv "resources/shapes.csv"
            {}
            {:db.type/tuple {:shape/key [:shape/id :shape/pt-sequence]
                             :shape/coordinates [:shape/pt-lat :shape/pt-lon]}
             :db.unique/identity #{:shape/key}}))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  (def cfg (load-csv "resources/shapes.csv"
            {}
            {:db.type/tuple {:shape/key [:shape/id :shape/pt-sequence]}
             :db.unique/identity #{:shape/key}}))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  (def cfg (load-csv "resources/shapes.csv"
            {}
            {:db.type/tuple {:shape/coordinates [:shape/pt-lat :shape/pt-lon]}}))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  ; TODO file Datahike bug report for cardinality-many tuples
  ; throws Execution error (UnsupportedOperationException): "count not supported on this type: Double"
  (def cfg (load-csv "resources/shapes.csv"
            {}
            {:db.unique/identity #{:shape/id}
             :db.type/tuple {:shape/coordinates [:shape/pt-lat :shape/pt-lon]}}))
  (def conn (d/connect cfg))
  (d/datoms @conn :eavt)
  (d/schema @conn)
  (d/transact conn {:tx-data [{:shape/id 185
                               :shape/coordinates [52.296719 13.631534]
                               :shape/pt-sequence 0}]})
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  ; TODO file Datahike bug report for cardinality-many tuples
  ; throws Execution error (UnsupportedOperationException): "count not supported on this type: Long"
  (def cfg (load-csv "resources/shapes.csv"
            {}
            {:db.unique/identity #{:shape/id}
             :db.type/tuple {:shape/key [:shape/id :shape/pt-sequence]}}))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  ; Nonsensical cases solely for test purposes
  (def cfg (load-csv "resources/shapes.csv"
            {}
            {:db.type/tuple {:shape/first [:shape/id :shape/pt-lat]
                             :shape/second [:shape/pt-lon :shape/pt-sequence]}}))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  cfg
  (d/delete-database cfg)

  ; TODO file Datahike bug report for cardinality-many tuples...
  ; ... "count not supported on this type: Float"
  (def cfg (load-csv "resources/shapes.csv"
            {}
            {:db.unique/identity #{:shape/id}
             :db.type/tuple {:shape/etc [:shape/pt-lat :shape/pt-lon :shape/pt-sequence]}}))
  (def conn (d/connect cfg))
  cfg
  (d/delete-database cfg)

  (def cfg (load-csv "resources/stops.csv"))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  (d/schema @conn)
  (d/delete-database cfg)

  ; TODO tuple type inconsistency
  (def cfg (load-csv "resources/stops.csv"
            {}
            {:db.unique/identity #{:stop/id}
             :db.cardinality/one #{:stop/name   ; just a few: the complete set is too long
                                   :stop/lat
                                   :stop/lon
                                   :stop/wheelchair-boarding}
             :db.type/string #{:stop/id :stop/name}  ; ditto, out of laziness
             :db.type/long #{:stop/location-type}   ; "
             :db.type/double #{:stop/lat :stop/lon}
             :db.type/tuple {:stop/coordinates [:stop/lat :stop/lon]}}))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  (d/schema @conn)
  (d/delete-database cfg)

  ; What if we fake it? Answer: user-specified schema takes precedence
  (def cfg (load-csv "resources/stops.csv"
            {}
            {:db.unique/identity #{:stop/id}
             :db.type/long #{:stop/id}}))
  ; explodes

  (def cfg (load-csv "resources/stops.csv"
            {}
            {:db.unique/identity #{:stop/id}
             :db.type/ref #{:stop/parent-station}}))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  (d/schema @conn)
  (d/delete-database cfg)

  (def cfg (load-csv "resources/stops.csv"
            {}
            {:db.unique/identity #{:stop/id}
             :db.type/ref #{:stop/parent-station}
             :db.type/tuple {:stop/coordinates [:stop/lat :stop/lon]}}))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  (d/schema @conn)
  (d/delete-database cfg)

  (def cfg (load-csv "resources/stops.csv"
            {}
            {:db.unique/identity #{:stop/id}
             :db.type/ref #{:stop/parent-station}
             :db.type/compositeTuple {:stop/coordinates [:stop/lat :stop/lon]}}))
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  (d/schema @conn)
  (d/delete-database cfg)

  ; TODO ugh please not whack-a-mole
  (def pokemon-file "test/data/pokemon.csv")
  (def cfg (load-csv pokemon-file))
  cfg
  (def conn (d/connect cfg))
  (count (d/datoms @conn :eavt))
  (:abilities (d/entity @conn 50))
  (count (d/schema @conn))
  (d/delete-database cfg)

  (def grilled (with-open [reader (io/reader pokemon-file)]
                 (doall
                  (charred/read-csv reader))))
  (count grilled)
  (map #(nth % 23) grilled)
  (subvec (nth grilled 1) 32)

  (def dial-311-file "test/data/311_service_requests_2010-present_sample.csv")
  (def wtf (load-csv dial-311-file
                     {}
                     {}
                     {:vector-open \(
                      :vector-close \)
                      :schema-sample-size 128000
                      :parser-sample-size 128000}))
                      ;:parser-fn {40 [:vector
                      ;                (fn [v]
                      ;                  (let [len (.length ^String v)
                      ;                        re (re-pattern ", ")]
                      ;                    (mapv (get parser/default-coercers :float32)
                      ;                          (-> (subs v 1 (dec len))
                      ;                              (str/split re)))))]}}
  wtf
  (count wtf)
  )
