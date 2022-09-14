(ns datahike-csv-loader.core-test
  (:require [clojure.java.io :as io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [charred.api :as charred]
            [clojure.test :refer [deftest testing is use-fixtures]]
            [datahike.api :as d]
            [datahike-csv-loader.utils :as utils]
            [datahike-csv-loader.core :refer :all]
            [tablecloth.api :as tc]))

(def ^:private datahike-cfg {:store {:backend :mem
                                     :unique-id "test-db"}
                             :index :datahike.index/persistent-set
                             :attribute-refs? false
                             :name "test-db"})

;; tablecloth claims to work with input stream arguments, but that doesn't seem to be true...
(def ^:private resources-folder "resources")
(def ^:private agencies-filename (str resources-folder "/agencies.csv"))
(def ^:private routes-filename (str resources-folder "/routes.csv"))
(def ^:private levels-filename (str resources-folder "/levels.csv"))
(def ^:private stops-filename (str resources-folder "/stops.csv"))
(def ^:private route-trips-filename (str resources-folder "/route_trips.csv"))
(def ^:private shapes-filename (str resources-folder "/shapes.csv"))

(def ^:private agency-simple-schema {:unique-id    #{:agency/id}
                                     :unique-val   #{:agency/name}
                                     :index        #{:agency/name}})
(def ^:private agency-schema [{:db/ident        :agency/id
                               :db/cardinality  :db.cardinality/one
                               :db/valueType    :db.type/long
                               :db/unique       :db.unique/identity}
                              {:db/ident        :agency/name
                               :db/cardinality  :db.cardinality/one
                               :db/valueType    :db.type/string}
                              {:db/ident        :agency/url
                               :db/cardinality  :db.cardinality/one
                               :db/valueType    :db.type/string}
                              {:db/ident        :agency/timezone
                               :db/cardinality  :db.cardinality/one
                               :db/valueType    :db.type/string}
                              {:db/ident        :agency/lang
                               :db/cardinality  :db.cardinality/one
                               :db/valueType    :db.type/string}
                              {:db/ident        :agency/phone
                               :db/cardinality  :db.cardinality/one
                               :db/valueType    :db.type/string}])

(def ^:private route-simple-schema {:unique-id #{:route/id}})
(def ^:private route-schema [{:db/ident        :route/id
                              :db/cardinality  :db.cardinality/one
                              :db/valueType    :db.type/string
                              :db/unique       :db.unique/identity}
                             {:db/ident        :route/agency-id
                              :db/cardinality  :db.cardinality/one
                              :db/valueType    :db.type/ref}
                             {:db/ident        :route/short-name
                              :db/cardinality  :db.cardinality/one
                              :db/valueType    :db.type/string}
                             {:db/ident        :route/long-name
                              :db/cardinality  :db.cardinality/one
                              :db/valueType    :db.type/string}
                             {:db/ident        :route/type
                              :db/cardinality  :db.cardinality/one
                              :db/valueType    :db.type/long}
                             ; Colours are actually hex, but this is just a test
                             {:db/ident        :route/color
                              :db/cardinality  :db.cardinality/one
                              :db/valueType    :db.type/string}
                             {:db/ident        :route/text-color
                              :db/cardinality  :db.cardinality/one
                              :db/valueType    :db.type/string}
                             {:db/ident        :route/desc
                              :db/cardinality  :db.cardinality/one
                              :db/valueType    :db.type/string}])
(def ^:private route-ref-map {:route/agency-id :agency/id})

(def ^:private level-simple-schema {:unique-id #{:level/id}})
(def ^:private level-schema [{:db/ident        :level/id
                              :db/cardinality  :db.cardinality/one
                              :db/valueType    :db.type/long
                              :db/unique       :db.unique/identity}
                             {:db/ident        :level/index
                              :db/cardinality  :db.cardinality/one
                              :db/valueType    :db.type/long}
                             {:db/ident        :level/name
                              :db/cardinality  :db.cardinality/one
                              :db/valueType    :db.type/string}])

(def ^:private stop-simple-schema {:unique-id  #{:stop/id}
                                   :index      #{:stop/name}})
(def ^:private stop-schema [{:db/ident        :stop/id
                             :db/cardinality  :db.cardinality/one
                             :db/valueType    :db.type/string
                             :db/unique       :db.unique/identity}
                            {:db/ident        :stop/code
                             :db/cardinality  :db.cardinality/one
                             :db/valueType    :db.type/string}
                            {:db/ident        :stop/name
                             :db/cardinality  :db.cardinality/one
                             :db/valueType    :db.type/string}
                            {:db/ident        :stop/desc
                             :db/cardinality  :db.cardinality/one
                             :db/valueType    :db.type/string}
                            {:db/ident        :stop/lat
                             :db/cardinality  :db.cardinality/one
                             :db/valueType    :db.type/double}
                            {:db/ident        :stop/lon
                             :db/cardinality  :db.cardinality/one
                             :db/valueType    :db.type/double}
                            {:db/ident        :stop/location-type
                             :db/cardinality  :db.cardinality/one
                             :db/valueType    :db.type/long}
                            {:db/ident        :stop/parent-station
                             :db/cardinality  :db.cardinality/one
                             :db/valueType    :db.type/ref}
                            {:db/ident        :stop/wheelchair-boarding
                             :db/cardinality  :db.cardinality/one
                             :db/valueType    :db.type/long}
                            {:db/ident        :stop/platform-code
                             :db/cardinality  :db.cardinality/one
                             ; should be string, but in the test sample...
                             :db/valueType    :db.type/long}
                            {:db/ident        :stop/zone-id
                             :db/cardinality  :db.cardinality/one
                             :db/valueType    :db.type/string}
                            {:db/ident        :stop/level-id
                             :db/cardinality  :db.cardinality/one
                             :db/valueType    :db.type/ref}])

(def ^:private stop-ref-map {:stop/parent-station   :stop/id
                             :stop/level-id         :level/id})

(def ^:private route-trip-simple-schema {:unique-id        #{:route/id}
                                         :cardinality-many #{:route/trip-id}})
(def ^:private route-trip-schema [{:db/ident        :route/trip-id
                                   :db/cardinality  :db.cardinality/many
                                   :db/valueType    :db.type/long}])

(def ^:private shape-simple-schema-1 {:unique-id           #{:shape/id}
                                      :cardinality-many    #{:shape/pt-lat-lon-sequence}})
(def ^:private shape-schema-1 [{:db/ident       :shape/id
                                :db/cardinality :db.cardinality/one
                                :db/valueType   :db.type/long
                                :db/unique       :db.unique/identity}
                               {:db/ident       :shape/pt-lat-lon-sequence
                                :db/cardinality :db.cardinality/many
                                :db/valueType   :db.type/tuple
                                :db/tupleTypes  [:db.type/double :db.type/double :db.type/long]}])
(def ^:private shape-tuple-map-1
  {:shape/pt-lat-lon-sequence [:shape/pt-lat :shape/pt-lon :shape/pt-sequence]})

(def ^:private shape-simple-schema-2 {:unique-id #{:shape/id-pt-sequence}})
(def ^:private shape-schema-2 [{:db/ident       :shape/id
                                :db/cardinality :db.cardinality/one
                                :db/valueType   :db.type/long}
                               {:db/ident       :shape/pt-sequence
                                :db/cardinality :db.cardinality/one
                                :db/valueType   :db.type/long}
                               {:db/ident       :shape/id-pt-sequence
                                :db/cardinality :db.cardinality/one
                                :db/valueType   :db.type/tuple
                                :db/tupleAttrs  [:shape/id :shape/pt-sequence]
                                :db/unique      :db.unique/identity}
                               {:db/ident       :shape/pt-lat-lon
                                :db/cardinality :db.cardinality/one
                                :db/valueType   :db.type/tuple
                                :db/tupleTypes  [:db.type/double :db.type/double]}])
(def ^:private shape-tuple-map-2 {:shape/pt-lat-lon [:shape/pt-lat :shape/pt-lon]})
(def ^:private shape-composite-tuple-map {:shape/id-pt-sequence [:shape/id :shape/pt-sequence]})

(defn- csv-to-maps [filename]
  (let [csv (with-open [reader (io/reader filename)]
              (charred/read-csv reader))
        cols (map keyword (first csv))]
    (mapv #(zipmap cols %) (rest csv))))

(defn- unwrap-refs [e ref-attrs]
  (reduce (fn [e a] (update e a :db/id))
          e
          ref-attrs))

(defn- clean-pulled-entities [entities ref-attrs]
  (map #(-> (dissoc % :db/id)
            (unwrap-refs ref-attrs))
       entities))

(defn- test-schema-attribute-vals
  ([db-schema entity-attrs entity-schema]
   (test-schema-attribute-vals db-schema entity-attrs entity-schema {} {} {}))
  ([db-schema entity-attrs entity-schema ref-map]
   (test-schema-attribute-vals db-schema entity-attrs entity-schema ref-map {} {}))
  ([db-schema entity-attrs entity-schema ref-map tuple-map]
   (test-schema-attribute-vals db-schema entity-attrs entity-schema ref-map tuple-map {}))
  ([db-schema entity-attrs entity-schema ref-map tuple-map composite-tuple-map]
   (doseq [attr (:unique-id entity-schema)]
     (is (= :db.unique/identity
            (:db/unique (attr db-schema)))))
   (doseq [attr (:unique-val entity-schema)]
     (is (= :db.unique/value
            (:db/unique (attr db-schema)))))
   (doseq [attr (:index entity-schema)]
     (is (:db/index (attr db-schema))))
   (doseq [attr (:cardinality-many entity-schema)]
     (is (= :db.cardinality/many
            (:db/cardinality (attr db-schema)))))
   (doseq [attr (keys ref-map)]
     (is (= :db.type/ref
            (:db/valueType (attr db-schema)))))
   (doseq [attr (keys tuple-map)]
     (is (= :db.type/tuple
            (:db/valueType (attr db-schema)))))
   (doseq [attr (keys composite-tuple-map)]
     (is (= :db.type/tuple
            (:db/valueType (attr db-schema)))))
   (doseq [attr (set/difference entity-attrs (:unique-id entity-schema) (:unique-val entity-schema))]
     (is (-> (:db/unique (attr db-schema))
             nil?)))
   (doseq [attr (set/difference entity-attrs (:index entity-schema))]
     (is (-> (:db/index (attr db-schema))
             nil?)))
   (doseq [attr (set/difference entity-attrs (:cardinality-many entity-schema))]
     (is (= :db.cardinality/one
            (:db/cardinality (attr db-schema)))))
   (doseq [attr (set/difference entity-attrs (keys ref-map))]
     (is (not= :db.type/ref
               (:db/valueType (attr db-schema)))))
   (doseq [attr (set/difference entity-attrs (keys tuple-map))]
     (is (not= :db.type/tuple
               (:db/valueType (attr db-schema)))))
   (doseq [attr (set/difference entity-attrs (keys composite-tuple-map))]
     (is (not= :db.type/tuple
               (:db/valueType (attr db-schema)))))))

(defn- process-agencies-from-csv [filename]
  (mapv (fn [a]
          (let [phone-fn #(if (str/blank? %) nil (read-string %))]
            (-> (update a :agency/id read-string)
                (update :agency/phone phone-fn))))
        (csv-to-maps filename)))

(defn- datahike-csv-loader-test-fixture [f]
  (f)
  (d/delete-database datahike-cfg))

(use-fixtures :each datahike-csv-loader-test-fixture)

(deftest test-csv-to-datahike-without-schema
  (testing "Test csv-to-datahike without schema"
    (load-csv agencies-filename)
    (let [exp (->> (process-agencies-from-csv agencies-filename)
                   (mapv #(utils/rm-empty-elements % {} false)))
          cfg {:schema-flexibility :read}
          conn (d/connect cfg)]
      (is (= (set exp)
             (set (->> (d/q '[:find [?e ...] :where [?e :agency/id _]]
                            @conn)
                       (d/pull-many @conn (keys (first exp))))))))))

(defn- get-db-ids [id-attr from-csv db]
  (->> (map (fn [r] [id-attr (id-attr r)]) from-csv)
       (d/pull-many db [:db/id])
       (mapv :db/id)))

(defn- test-agency-csv-to-datahike [schema-args]
  (load-csv agencies-filename datahike-cfg schema-args)
  ; workaround for stale connection bug (see also test-agency-route-trip-with-* tests)
  (let [conn (d/connect datahike-cfg)
        agencies-from-csv (process-agencies-from-csv agencies-filename)
        agency-attrs (keys (first agencies-from-csv))]
    (if (map? (:schema schema-args))
      (testing "Unique identity, unique value, and indexed schema attributes transacted"
        (test-schema-attribute-vals (d/schema @conn) (set agency-attrs) agency-simple-schema)))
    (testing "Agency data loaded correctly"
      (let [ids (d/q '[:find [?e ...] :where [?e :agency/id _]]
                     @conn)]
        (is (= (->> agencies-from-csv
                    (mapv #(utils/rm-empty-elements % {} false)))
               (->> (get-db-ids :agency/id agencies-from-csv @conn)
                    (d/pull-many @conn agency-attrs))))))))

(defn- test-route-csv-to-datahike [schema-args]
  (load-csv routes-filename datahike-cfg schema-args)
  ; workaround for stale connection bug (see also test-agency-route-trip-with-* tests)
  (let [conn (d/connect datahike-cfg)
        routes-csv (->> (csv-to-maps routes-filename)
                        (mapv #(-> (update % :route/agency-id read-string)
                                   (update :route/type read-string))))
        route-attrs (keys (first routes-csv))]
    (if (map? (:schema schema-args))
      (testing "Foreign ID (reference) attributes transacted"
        (test-schema-attribute-vals (d/schema @conn) (set route-attrs) route-simple-schema route-ref-map)))
    (let [ids (get-db-ids :route/id routes-csv @conn)
          routes-dh (-> (d/pull-many @conn route-attrs ids)
                        (clean-pulled-entities (keys route-ref-map)))
          routes-minus-agency-id (map #(dissoc % :route/agency-id) routes-dh)]
      (testing "Route foreign references in Datahike are correct"
        (is (= (map :route/agency-id routes-csv)
               (->> (map :route/agency-id routes-dh)
                    (d/pull-many @conn [:agency/id])
                    (map :agency/id)))))
      (testing "Other route data correctly loaded"
        (is (= (mapv #(-> (utils/rm-empty-elements % {} false)
                          (dissoc :route/agency-id))
                     routes-csv)
               routes-minus-agency-id))))))

(defn- test-route-trip-csv-to-datahike [schema-args]
  (load-csv route-trips-filename datahike-cfg schema-args)
  ; workaround for stale connection bug (see also test-agency-route-trip-with-* tests)
  (let [conn (d/connect datahike-cfg)
        route-trip-maps (mapv #(update % :route/trip-id read-string)
                              (csv-to-maps route-trips-filename))
        route-trip-attrs (keys (first route-trip-maps))]
    (if (map? (:schema schema-args))
      (testing "Cardinality-many schema attributes transacted"
        (test-schema-attribute-vals (d/schema @conn) (set route-trip-attrs) route-trip-simple-schema)))
    (testing "Trip data correctly associated to routes"
      (let [route-trips (reduce (fn [m rt]
                                  (let [{:route/keys [id trip-id]} rt]
                                    (if (m id)
                                      (update m id #(conj % trip-id))
                                      (assoc m id #{trip-id}))))
                                {}
                                route-trip-maps)]
        (is (= (->> (map (fn [rid] [:route/id rid])
                         (keys route-trips))
                    (d/pull-many @conn [:route/id :route/trip-id])
                    (reduce (fn [m rt]
                              (->> (set (:route/trip-id rt))
                                   (assoc m (:route/id rt))))
                            {}))
               route-trips))))))

; Stale connection bug => can't create connection then pass it to functions
(deftest test-agency-route-trip-with-simple-schema
  (test-agency-csv-to-datahike {:schema agency-simple-schema})
  (test-route-csv-to-datahike {:schema route-simple-schema, :ref-map route-ref-map})
  (test-route-trip-csv-to-datahike {:schema route-trip-simple-schema}))

; Stale connection bug => can't create connection then pass it to functions
(deftest test-agency-route-trip-with-schema
  (test-agency-csv-to-datahike {:schema agency-schema})
  (test-route-csv-to-datahike {:schema route-schema, :ref-map route-ref-map})
  (test-route-trip-csv-to-datahike {:schema route-trip-schema}))

(defn- test-refs-in-schema [schema-args]
  (testing "IllegalArgumentException is thrown when attribute not present in schema is referenced"
    (is (thrown? IllegalArgumentException (load-csv routes-filename datahike-cfg schema-args)))))

(deftest test-refs-in-schema-with-simple-schema
  (test-refs-in-schema {:schema  route-simple-schema
                        :ref-map route-ref-map}))

(deftest test-refs-in-schema-with-schema
  (test-refs-in-schema {:schema  route-schema
                        :ref-map route-ref-map}))

(defn- test-stop-csv-to-datahike [levels-schema-args stops-schema-args]
  (load-csv levels-filename datahike-cfg levels-schema-args)
  (load-csv stops-filename datahike-cfg stops-schema-args)
  (let [conn (d/connect datahike-cfg)
        stops-csv (->> (csv-to-maps stops-filename)
                       (mapv #(-> (reduce (fn [stop k]
                                            (if (seq (k stop))
                                              (update stop k read-string)
                                              (update stop k (constantly nil))))
                                          %
                                          #{:stop/lat
                                            :stop/lon
                                            :stop/location-type
                                            :stop/wheelchair-boarding
                                            :stop/platform-code
                                            :stop/level-id}))))
        stop-attrs (keys (first stops-csv))]
    (if (map? (:schema stops-schema-args))
      (testing "Schema attributes correctly transacted"
        (test-schema-attribute-vals (d/schema @conn) (set stop-attrs) stop-simple-schema stop-ref-map)))
    (let [ids (get-db-ids :stop/id stops-csv @conn)
          stops-dh (map #(unwrap-refs % (keys stop-ref-map))
                        (d/pull-many @conn stop-attrs ids))
          dissoc-refs #(-> (dissoc % :stop/level-id)
                           (dissoc :stop/parent-station))]
      (testing "Stop self (parent) references in Datahike are correct"
        (is (= (->> (mapv :stop/parent-station stops-csv)
                    (remove empty?))
               (->> (map :stop/parent-station stops-dh)
                    (remove nil?)
                    (d/pull-many @conn [:stop/id])
                    (map :stop/id)))))
      (testing "Other stop data correctly loaded"
        (is (= (mapv #(-> (utils/rm-empty-elements % {} false)
                          dissoc-refs)
                     stops-csv)
               (map dissoc-refs stops-dh)))))))

(deftest test-stop-csv-to-datahike-with-simple-schema
  (test-stop-csv-to-datahike {:schema level-simple-schema}
                             {:schema stop-simple-schema, :ref-map stop-ref-map}))

(deftest test-stop-csv-to-datahike-with-schema
  (test-stop-csv-to-datahike {:schema level-schema}
                             {:schema stop-schema, :ref-map stop-ref-map}))

(defn- test-heterogeneous-tuple-csv-to-datahike [schema-args]
  (load-csv shapes-filename datahike-cfg schema-args)
  (let [conn (d/connect datahike-cfg)
        shapes-from-csv (->> (with-open [reader (io/reader shapes-filename)]
                               (charred/read-csv reader))
                             rest
                             (map #(map read-string %)))
        lookup-refs (->> (distinct (map first shapes-from-csv))
                         (map (fn [i] [:shape/id i])))]
    (is (= (->> (group-by first shapes-from-csv)
                (map (fn [[k v]] (map rest v))))
           (->> (d/pull-many @conn '[*] lookup-refs)
                (map :shape/pt-lat-lon-sequence))))))

(deftest test-heterogeneous-tuple-csv-to-datahike-with-simple-schema
  (test-heterogeneous-tuple-csv-to-datahike {:schema shape-simple-schema-1
                                             :tuple-map shape-tuple-map-1}))

(deftest test-heterogeneous-tuple-csv-to-datahike-with-schema
  (test-heterogeneous-tuple-csv-to-datahike {:schema shape-schema-1
                                             :tuple-map shape-tuple-map-1}))

(defn- test-composite-and-homogeneous-tuples-csv-to-datahike [schema-args]
  (load-csv shapes-filename datahike-cfg schema-args)
  (let [conn (d/connect datahike-cfg)
        ids (d/q '[:find [?e ...] :where [?e :shape/id _]]
                 @conn)
        shapes-from-csv (->> (csv-to-maps shapes-filename)
                             (mapv #(reduce-kv (fn [m k v]
                                                 (assoc m k (read-string v)))
                                               {}
                                               %)))]
    (is (= (set (->> (d/pull-many @conn '[*] ids)
                     (map #(dissoc % :db/id))))
           (set (mapv #(-> (assoc % :shape/id-pt-sequence [(:shape/id %) (:shape/pt-sequence %)])
                           (assoc :shape/pt-lat-lon [(:shape/pt-lat %) (:shape/pt-lon %)])
                           (dissoc :shape/pt-lat)
                           (dissoc :shape/pt-lon))
                      shapes-from-csv))))))

(deftest test-composite-and-homogeneous-tuples-csv-to-datahike-with-simple-schema
  (test-composite-and-homogeneous-tuples-csv-to-datahike {:schema shape-simple-schema-2
                                                          :tuple-map shape-tuple-map-2
                                                          :composite-tuple-map shape-composite-tuple-map}))

(deftest test-composite-and-homogeneous-tuples-csv-to-datahike-with-schema
  (test-composite-and-homogeneous-tuples-csv-to-datahike {:schema shape-schema-2
                                                          :tuple-map shape-tuple-map-2
                                                          :composite-tuple-map shape-composite-tuple-map}))
