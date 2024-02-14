(ns mesalog.api-test
  (:require [clojure.java.io :as io]
            [clojure.set :as s]
            [clojure.string :as string]
            [clojure.test :refer [deftest testing is use-fixtures]]
            [datahike.api :as d]
            [mesalog.api :refer :all]
            [mesalog.parse.parser :as parser]
            [mesalog.parse.utils :as utils]
            [tablecloth.api :as tc]
            [tech.v3.datatype.datetime :as dtype-dt])
  (:import [java.util UUID]
           [java.io File]))


(def ^:dynamic test-conn nil)


(defn- mesalog-test-fixture [f]
  (let [cfg (d/create-database {})]
    (binding [test-conn (d/connect cfg)]
      (f)
      (d/delete-database cfg))))

(use-fixtures :each mesalog-test-fixture)


(defn- tc-dataset [file]
  (tc/dataset file {:key-fn keyword}))


(def ^:private data-folder "data")
(def ^:private agencies-file (io/file data-folder "agencies.csv"))
(def ^:private routes-file (io/file data-folder "routes.csv"))
(def ^:private shapes-file (io/file data-folder "shapes.csv"))


; Questionable or nonsensical schema designs sometimes used for testing purposes only

(defn- test-num-rows
  ([num-rows batch-size]
   (let [options (cond-> {:num-rows num-rows}
                   (some? batch-size) (merge {:batch-size batch-size}))
         colname->ident (parser/colname->ident-fn options)
         id-ident (colname->ident "agency/id")]
     (load-csv agencies-file test-conn {} {} options)
     (is (-> (filter #(= (:a %) id-ident)
                     (d/datoms @test-conn :eavt))
             count
             (= num-rows)))))
  ([num-rows]
   (test-num-rows num-rows nil)))


(deftest num-rows-option
  (testing "`:num-rows` option works when `:batch-size` unspecified"
    (test-num-rows 10)))


(deftest num-rows-option-with-lower-batch-size
  (testing "`:num-rows` option works when specified `:batch-size` is lower"
    (test-num-rows 10 5)))


(deftest num-rows-option-with-higher-batch-size
  (testing "`:num-rows` option works when specified `:batch-size` is lower"
    (test-num-rows 10 20)))


(deftest empty-input
  (testing "empty CSV file"
    (is (-> (io/file data-folder "empty.csv")
            (load-csv test-conn)
            d/schema
            (= {})))))


(deftest existing-schema-conflict
  (testing "IllegalArgumentException thrown when existing and specified and/or inferred schemas are inconsistent"
    (load-csv "data/routes-conflict.csv" test-conn)
    (is (thrown? IllegalArgumentException
                 (load-csv "data/routes.csv" test-conn)))))


(deftest consistent-existing-schema
  (testing "Consistent existing schema is subset of latest schema"
    (load-csv "data/routes-partial.csv" test-conn)
    (let [old-schema (d/schema @test-conn)
          old-keys (keys old-schema)]
      (load-csv "data/routes.csv" test-conn)
      (is (= (select-keys (d/schema @test-conn) old-keys)
             old-schema)))))


(deftest vector-schema-spec
  (testing "Schema specification as a vector (i.e. the format expected by Datahike)"
    (let [schema-vec [{:db/ident        :shape/id
                       :db/valueType    :db.type/long
                       :db/cardinality  :db.cardinality/one
                       :db/unique       :db.unique/identity}
                      {:db/ident        :shape/pt-lat
                       :db/valueType    :db.type/float
                       :db/cardinality  :db.cardinality/many}
                      {:db/ident        :shape/pt-lon
                       :db/valueType    :db.type/float
                       :db/cardinality  :db.cardinality/many}
                      {:db/ident        :shape/pt-sequence
                       :db/cardinality  :db.cardinality/many}]
          schema (into {}
                       (map (fn [s]
                              [(:db/ident s) s]))
                       schema-vec)
          _ (load-csv shapes-file test-conn {} schema-vec)
          db-schema (d/schema @test-conn)]
      (is (every? (fn [[a s]]
                    (= (dissoc s :db/id) (a schema)))
                  (dissoc db-schema :shape/pt-sequence)))
      (is (= (:db/valueType (:shape/pt-sequence db-schema))
             :db.type/long)))))


(defn- filter-ds-col-names [pred ds-cols]
  (map #(:name (meta %))
       (filter pred ds-cols)))


(defn- missing-attrs-not-in-schema [ds-cols schema]
  (let [missing-attrs (filter-ds-col-names #(every? nil? %) ds-cols)]
    (is (-> (s/intersection (set (keys schema))
                            (set missing-attrs))
            count
            (= 0)))))


(defn- non-missing-attrs-in-schema [ds-cols schema]
  (let [attrs (filter-ds-col-names #(not-every? nil? %) ds-cols)]
    (is (= (set (keys schema))
           (set attrs)))))


(deftest attrs-in-schema
  (testing (str "attributes are transacted into schema if and only if"
                "corresponding columns have non-null values")
    (let [ds-cols (tc/columns (tc-dataset routes-file))
          _ (load-csv routes-file test-conn)
          schema (d/schema @test-conn)]
      (missing-attrs-not-in-schema ds-cols schema)
      (non-missing-attrs-in-schema ds-cols schema))))


; load-csv will throw a null pointer exception, but that seems like a
; coarse criterion to test with
(deftest parser-sample-size-option
  (testing ":parser-sample-size shows the expected behaviour"
    (let [parsers (parser/infer-parsers agencies-file
                                        {}
                                        {:parser-sample-size 20})
          nonnull-attrs (->> (filter :parse-fn parsers)
                             (map :column-name))
          ds-cols (-> (tc/dataset agencies-file)
                      (tc/select-rows (range 20))
                      tc/columns)]
      (is (= (set (filter-ds-col-names #(every? nil? %)
                                       ds-cols))
             (s/difference (set (map :column-name parsers))
                           nonnull-attrs)))
      (is (= (filter-ds-col-names #(not-every? nil? %) ds-cols)
             nonnull-attrs)))))


(deftest boolean-value-types
  (testing (str "columns with values meeting and not meeting boolean parsing criteria "
                "are correctly reflected in schema")
    (load-csv (io/file data-folder "boolean-or-not.csv") test-conn)
    (let [schema (d/schema @test-conn)]
      (doseq [col [:char :word]]
        (is (->> (:db/valueType (col schema))
                 (= :db.type/string))))
      (doseq [col [:bool :boolstr :boolean]]
        (is (->> (:db/valueType (col schema))
                 (= :db.type/boolean)))))))


(defn- ds->col-dtypes [ds]
  (into {}
        (map (fn [row]
               [(:col-name row)
                (utils/tech-v3->datahike-dtypes (:datatype row))]))
        (tc/rows (tc/info ds) :as-maps)))


(defn- test-attr-value-types
  ([schema col->dtype tuple->dtypes]
   (doseq [attr (keys schema)]
     (is (= (:db/valueType (attr schema))
            (if-some [tuple-types (attr tuple->dtypes)]
              tuple-types
              (attr col->dtype))))))
  ([schema col->dtype]
   (test-attr-value-types schema col->dtype {})))


(defn- test-attr-value-types-with-file [test-file]
  (load-csv test-file test-conn)
  (->> (ds->col-dtypes (tc-dataset test-file))
       (test-attr-value-types (d/schema @test-conn))))


(deftest numeric-types
  (testing "columns with numeric values (long and double) are correctly reflected in schema"
    (test-attr-value-types-with-file shapes-file)))


(deftest string-type
  (testing "columns with string values are correctly reflected in schema"
    (test-attr-value-types-with-file (io/file data-folder "levels.csv"))))


(deftest uuids
  (testing "UUIDs are correctly reflected in schema"
    (let [uuids (repeatedly 5 #(UUID/randomUUID))
          test-fname "uuid.csv"
          test-file (io/file test-fname)]
      (-> {:uuids (repeatedly 5 #(UUID/randomUUID))}
          tc/dataset
          (tc/write! test-fname))
      (try (test-attr-value-types-with-file test-file)
           (finally
             (.delete test-file))))))


(defn- test-datetime-type
  ([create-dt-fn dt-type]
   (let [fname "datetime-test.csv"]
     (try (-> (tc/dataset [{:a 0 :b (create-dt-fn)}
                           {:a 1 :b nil}
                           {:a 2 :b (create-dt-fn)}])
              (tc/write! fname))
          (->> (if (some? dt-type)
                 {"b" dt-type}
                 {})
               (load-csv fname test-conn))
          (is (-> (:b (d/schema @test-conn))
                  :db/valueType
                  (= :db.type/instant)))
          (finally
            (.delete (File. fname))))))
  ([create-dt-fn]
   (test-datetime-type create-dt-fn nil)))


(deftest zoned-date-time
  (testing "Zoned datetimes are correctly reflected in schema"
    (test-datetime-type dtype-dt/zoned-date-time)))


(deftest local-date-time
  (testing "Local datetimes are correctly reflected in schema"
    (test-datetime-type dtype-dt/local-date-time)))


(deftest local-date
  (testing "Local dates are correctly reflected in schema"
    (test-datetime-type dtype-dt/local-date)))


(deftest zoned-date-time-with-type
  (testing "Zoned datetimes with keyword type specification are correctly reflected in schema"
    (test-datetime-type dtype-dt/zoned-date-time :zoned-date-time)))


(deftest local-date-time-with-type
  (testing "Local datetimes with keyword type specification are correctly reflected in schema"
    (test-datetime-type dtype-dt/local-date-time :local-date-time)))


(deftest local-date-with-type
  (testing "Local dates with keyword type specification are correctly reflected in schema"
    (test-datetime-type dtype-dt/local-date :local-date)))


(deftest references-to-existing-entities
  (testing "Attributes referencing existing entities are correctly reflected in schema and loaded"
    (let [trips-file (io/file data-folder "trips.csv")
          trips (tc-dataset trips-file)
          trip->route-id (into {}
                               (map (fn [t]
                                      [(:trip/id t) (:trip/route-id t)]))
                               (tc/rows trips :as-maps))]
      (load-csv routes-file test-conn {:db.unique/identity #{:route/id}})
      (load-csv trips-file test-conn {:db.type/ref {:trip/route-id :route/id}})
      (is (every? (fn [[k v]]
                    (= (get trip->route-id k) v))
                  (into {} (d/q '[:find ?trip-id ?route-id
                                  :where [?r :route/id ?route-id]
                                         [?t :trip/id ?trip-id]
                                         [?t :trip/route-id ?r]]
                                @test-conn)))))))


(deftest unique-attributes
  (testing "Attributes specified as unique are correctly reflected in schema"
    (let [schema-spec {:db.unique/identity #{:agency/id}
                       :db.unique/value #{:agency/name :agency/url}}]
      (load-csv agencies-file test-conn {} schema-spec)
      (let [schema (d/schema @test-conn)]
        (doseq [[unique-type attrs] schema-spec
                attr attrs]
          (is (= (:db/unique (attr schema))
                 unique-type)))))))


(deftest indexed-attrs
  (testing "Attributes specified to be indexed have indexing set to true in schema"
    (load-csv agencies-file test-conn {} {:db/index #{:agency/name}})
    (is (-> (d/schema @test-conn)
            :agency/name
            :db/index))))


(deftest no-history-attrs
  (testing "Attributes specified as noHistory have that set to true in schema"
    (load-csv shapes-file test-conn {} {:db/noHistory #{:shape/pt-lat :shape/pt-lon}})
    (let [schema (d/schema @test-conn)]
      (is (:db/noHistory (:shape/pt-lat schema)))
      (is (:db/noHistory (:shape/pt-lon schema))))))


(deftest cols-to-tuple
  (testing "Tuples formed by separate columns are correctly reflected in schema"
    (->> {:db.type/tuple {:shape/coordinates
                          [:shape/pt-lat :shape/pt-lon]}}
         (load-csv shapes-file test-conn {}))
    (let [schema (d/schema @test-conn)
          coordinates-schema (:shape/coordinates schema)]
      (is (= (set (keys schema))
             #{:shape/id :shape/coordinates :shape/pt-sequence}))
      (is (= (:db/valueType coordinates-schema) :db.type/tuple))
      (is (= (:db/tupleType coordinates-schema) :db.type/double)))))


(deftest composite-tuples
  (testing "Composite tuples are correctly reflected in schema"
    (let [latlon [:shape/pt-lat :shape/pt-lon]]
      (->> {:db.type/compositeTuple {:shape/coordinates latlon}}
           (load-csv shapes-file test-conn {}))
      (let [schema (d/schema @test-conn)
            coordinates-schema (:shape/coordinates schema)]
        (is (= (set (keys schema))
               (into #{:shape/id :shape/coordinates :shape/pt-sequence} latlon)))
        (is (= (:db/valueType coordinates-schema) :db.type/tuple))
        (is (= (:db/tupleAttrs coordinates-schema) latlon))))))


(deftest default-cardinality
  (testing "Cardinality when cardinality and unique attributes are not specified"
    (let [latlon [:shape/pt-lat :shape/pt-lon]]
      (->> {:db.type/compositeTuple {:shape/coordinates latlon}}
           (load-csv shapes-file test-conn))
      (is (every? #(= (:db/cardinality %)
                      :db.cardinality/one)
                  (vals (d/schema @test-conn)))))))


(deftest cardinality-many
  (testing "Cardinality many inference"
    (let [latlon [:shape/pt-lat :shape/pt-lon]]
      (->> {:db.unique/identity #{:shape/id}
            :db.type/compositeTuple {:shape/coordinates latlon}}
           (load-csv shapes-file test-conn {}))
      (is (every? #(= (:db/cardinality %)
                      :db.cardinality/many)
                  (-> (d/schema @test-conn)
                      (dissoc :shape/id)
                      vals))))))


(deftest cardinality-one
  (testing "Cardinality one correctly inferred when tuple-valued unique identifier attribute specified"
    (let [latlon [:shape/pt-lat :shape/pt-lon]]
      (->> {:db.type/tuple {:shape/pt [:shape/id :shape/pt-sequence]}
            :db.unique/identity #{:shape/pt}
            :db.type/compositeTuple {:shape/coordinates latlon}}
           (load-csv shapes-file test-conn))
      (is (every? #(= (:db/cardinality %)
                      :db.cardinality/one)
                  (-> (d/schema @test-conn)
                      (select-keys (conj latlon :shape/coordinates))
                      vals))))))


(deftest heterogeneous-tuples
  (testing "Heterogeneous tuples correctly reflected in schema and loaded"
    (let [shapes-ds (tc-dataset shapes-file)
          tuple-cols [:shape/pt-lat :shape/pt-lon :shape/pt-sequence]
          _ (->> {:db.type/tuple {:shape/pt tuple-cols}}
                 (load-csv shapes-file test-conn {}))
          pt-schema (:shape/pt (d/schema @test-conn))]
      (is (= (:db/valueType pt-schema) :db.type/tuple))
      (is (= (:db/tupleTypes pt-schema)
             (mapv (ds->col-dtypes shapes-ds) tuple-cols))))))


(deftest homogeneous-sequence-valued-columns
  (testing (str "CSV column of homogeneous sequence values is transacted as cardinality-many tuple, "
                "except when there exist multiple rows belonging to the same entity")
    (let [file (io/file data-folder "pokemon.csv")
          name-abilities (into {}
                               (map (fn [p]
                                      (let [a (:abilities p)
                                            astr (->> (dec (count a))
                                                      (subs a 1))]
                                        [(:japanese_name p)
                                         (->> (into #{} (string/split astr #","))
                                              count)])))
                               (tc/rows (tc-dataset file) :as-maps))
          _ (load-csv file test-conn)
          abilities-schema (:abilities (d/schema @test-conn))]
      (is (= (:db/cardinality abilities-schema)
             :db.cardinality/many))
      (is (= (:db/valueType abilities-schema)
             :db.type/string))
      (is (-> (fn [m [n a]]
                (if (some? (get m n))
                  (update m n inc)
                  (assoc m n 1)))
              (reduce {} (d/q '[:find ?n ?a
                                :where [?e :japanese_name ?n]
                                [?e :abilities ?a]]
                              @test-conn))
              (= name-abilities))))))


(deftest heterogeneous-sequence-valued-columns
  (testing "CSV column of heterogeneous sequence values is transacted as heterogeneous tuple"
    (let [fname "test.csv"]
      (spit fname "col\n")
      (doseq [t [["a" 1]
                 ["b" 2]
                 ["c" 3]]]
        (spit fname
              (apply format "\"[%s,%d]\"\n" t)
              :append true))
      (load-csv fname test-conn)
      (let [schema (:col (d/schema @test-conn))]
        (is (= (:db/valueType schema) :db.type/tuple))
        (is (= (:db/tupleTypes schema) [:db.type/string :db.type/long]))))))
