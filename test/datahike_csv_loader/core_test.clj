(ns datahike-csv-loader.core-test
  (:require [clojure.java.io :as io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [charred.api :as charred]
            [clojure.test :refer [deftest testing is]]
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

(def ^:private agency-cfg {:unique-id      #{:agency/id}
                           :unique-val  #{:agency/name}
                           :index   #{:agency/name}})
(def ^:private route-cfg {:unique-id   #{:route/id}
                          :ref  {:route/agency-id :agency/id}})
(def ^:private level-cfg {:unique-id   #{:level/id}})
(def ^:private stop-cfg {:unique-id    #{:stop/id}
                         :ref   {:stop/parent-station :stop/id
                                 :stop/level-id       :level/id}
                         :index #{:stop/name}})
(def ^:private route-trip-cfg {:unique-id   #{:route/id}
                               :cardinality-many  #{:route/trip-id}})
(def ^:private shape-cfg-1 {:unique-id   #{:shape/id}
                            :tuple {:shape/pt-lat-lon-sequence [:shape/pt-lat :shape/pt-lon :shape/pt-sequence]}
                            :cardinality-many  #{:shape/pt-lat-lon-sequence}})
(def ^:private shape-cfg-2 {:unique-id   #{:shape/id-pt-sequence}
                            :tuple {:shape/id-pt-sequence [:shape/id :shape/pt-sequence]
                                    :shape/pt-lat-lon [:shape/pt-lat :shape/pt-lon]}})

(def ^:dynamic *conn*)

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

(defn test-schema-attribute-vals [entity-cfg schema entity-attrs]
  (doseq [attr (:unique-id entity-cfg)]
    (is (= :db.unique/identity
           (:db/unique (attr schema)))))
  (doseq [attr (:unique-val entity-cfg)]
    (is (= :db.unique/value
           (:db/unique (attr schema)))))
  (doseq [attr (:index entity-cfg)]
    (is (:db/index (attr schema))))
  (doseq [attr (keys (:ref entity-cfg))]
    (is (= :db.type/ref
           (:db/valueType (attr schema)))))
  (doseq [attr (:cardinality-many entity-cfg)]
    (is (= :db.cardinality/many
           (:db/cardinality (attr schema)))))
  (doseq [attr (set/difference entity-attrs (:unique-id entity-cfg) (:unique-val entity-cfg))]
    (is (-> (:db/unique (attr schema))
            nil?)))
  (doseq [attr (set/difference entity-attrs (:index entity-cfg))]
    (is (-> (:db/index (attr schema))
            nil?)))
  (doseq [attr (set/difference entity-attrs (keys (:ref entity-cfg)))]
    (is (not= :db.type/ref
              (:db/valueType (attr schema)))))
  (doseq [attr (set/difference entity-attrs (:cardinality-many entity-cfg))]
    (is (= :db.cardinality/one
           (:db/cardinality (attr schema))))))

(defn process-agencies-from-csv [filename]
  (map (fn [a]
         (let [phone-fn #(if (str/blank? %) nil (read-string %))]
           (-> (update a :agency/id read-string)
               (update :agency/phone phone-fn))))
       (csv-to-maps filename)))

(deftest test-csv-to-datahike-without-col-schema
  (testing "Test csv-to-datahike without col-schema"
    (d/delete-database datahike-cfg)
    (d/create-database datahike-cfg)
    (binding [*conn* (d/connect datahike-cfg)]
      (load-csv *conn* agencies-filename)
      (let [exp (->> (process-agencies-from-csv agencies-filename)
                     (map #(utils/rm-empty-elements % {} false)))]
        (is (= (set exp)
               (set (->> (d/q '[:find [?e ...] :where [?e :agency/id _]]
                              @*conn*)
                         (d/pull-many @*conn* (keys (first exp)))))))))))

(defn test-agency-csv-to-datahike []
  (let [agencies-from-csv (process-agencies-from-csv agencies-filename)
        agency-attrs (keys (first agencies-from-csv))]
    (load-csv *conn* agencies-filename agency-cfg)
    (testing "Unique identity, unique value, and indexed schema attributes transacted"
      (test-schema-attribute-vals agency-cfg (d/schema @*conn*) (set agency-attrs)))
    (testing "Agency data loaded correctly"
      (let [ids (d/q '[:find [?e ...] :where [?e :agency/id _]]
                     @*conn*)]
        (is (= (set (d/pull-many @*conn* agency-attrs ids))
               (set (map #(utils/rm-empty-elements % {} false) agencies-from-csv))))))))

(defn test-route-csv-to-datahike []
  (let [routes-csv (->> (csv-to-maps routes-filename)
                        (map #(-> (update % :route/agency-id read-string)
                                  (update :route/type read-string))))
        route-attrs (keys (first routes-csv))]
    (load-csv *conn* routes-filename route-cfg)
    (testing "Foreign ID (reference) attributes transacted"
      (test-schema-attribute-vals route-cfg (d/schema @*conn*) (set route-attrs)))
    (let [ids (->> (map (fn [r] [:route/id (:route/id r)]) routes-csv)
                   ; TODO factor out
                   (d/pull-many @*conn* [:db/id])
                   (map :db/id))
          routes-dh (-> (d/pull-many @*conn* route-attrs ids)
                        (clean-pulled-entities (keys (:ref route-cfg))))
          routes-minus-agency-id (map #(dissoc % :route/agency-id) routes-dh)]
      (testing "Route foreign references in Datahike are correct"
        (is (= (map :route/agency-id routes-csv)
               (->> (map :route/agency-id routes-dh)
                    (d/pull-many @*conn* [:agency/id])
                    (map :agency/id)))))
      (testing "Other route data correctly loaded"
        (is (= (set (map #(-> (utils/rm-empty-elements % {} false)
                              (dissoc :route/agency-id))
                         routes-csv))
               (set routes-minus-agency-id)))))))

(defn test-route-trip-csv-to-datahike []
  (let [route-trip-maps (map #(update % :route/trip-id read-string)
                             (csv-to-maps route-trips-filename))
        route-trip-attrs (keys (first route-trip-maps))]
    (load-csv *conn* route-trips-filename route-trip-cfg)
    (testing "Cardinality-many schema attributes transacted"
      (->> (set route-trip-attrs)
           (test-schema-attribute-vals route-trip-cfg (d/schema @*conn*))))
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
                    (d/pull-many @*conn* [:route/id :route/trip-id])
                    (reduce (fn [m rt]
                              (->> (set (:route/trip-id rt))
                                   (assoc m (:route/id rt))))
                            {}))
               route-trips))))))

(deftest test-agency-route-trip
  (d/delete-database datahike-cfg)
  (d/create-database datahike-cfg)
  (binding [*conn* (d/connect datahike-cfg)]
    (test-agency-csv-to-datahike)
    (test-route-csv-to-datahike)
    (test-route-trip-csv-to-datahike)))

(deftest test-refs-in-schema
  (testing "IllegalArgumentException is thrown when attribute not present in schema is referenced"
    (d/delete-database datahike-cfg)
    (d/create-database datahike-cfg)
    (binding [*conn* (d/connect datahike-cfg)]
      (is (thrown? IllegalArgumentException (load-csv *conn* routes-filename route-cfg))))))

(deftest test-stop-csv-to-datahike
  (d/delete-database datahike-cfg)
  (d/create-database datahike-cfg)
  (binding [*conn* (d/connect datahike-cfg)]
    (load-csv *conn* levels-filename level-cfg)
    (let [stops-csv (->> (csv-to-maps stops-filename)
                         (map #(-> (reduce (fn [stop k]
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
      (load-csv *conn* stops-filename stop-cfg)
      (testing "Schema attributes correctly transacted"
        (test-schema-attribute-vals stop-cfg (d/schema @*conn*) (set stop-attrs)))
      (let [ids (->> (map (fn [r] [:stop/id (:stop/id r)]) stops-csv)
                     (d/pull-many @*conn* [:db/id])
                     (map :db/id))
            stops-dh (map #(unwrap-refs % (keys (:ref stop-cfg)))
                          (d/pull-many @*conn* stop-attrs ids))
            dissoc-refs #(-> (dissoc % :stop/level-id)
                             (dissoc :stop/parent-station))]
        (testing "Stop self (parent) references in Datahike are correct"
          (is (= (->> (map :stop/parent-station stops-csv)
                      (remove empty?))
                 (->> (map :stop/parent-station stops-dh)
                      (remove nil?)
                      (d/pull-many @*conn* [:stop/id])
                      (map :stop/id)))))
        (testing "Other stop data correctly loaded"
          (is (= (map #(-> (utils/rm-empty-elements % {} false)
                           dissoc-refs)
                      stops-csv)
                 (map dissoc-refs stops-dh))))))))

(deftest test-heterogeneous-tuple-csv-to-datahike
  (d/delete-database datahike-cfg)
  (d/create-database datahike-cfg)
  (binding [*conn* (d/connect datahike-cfg)]
    (let [shapes-from-csv (->> (with-open [reader (io/reader shapes-filename)]
                                 (charred/read-csv reader))
                               rest
                               (map #(map read-string %)))
          lookup-refs (->> (distinct (map first shapes-from-csv))
                           (map (fn [i] [:shape/id i])))]
      (load-csv *conn* shapes-filename shape-cfg-1)
      (is (= (->> (group-by first shapes-from-csv)
                  (map (fn [[k v]] (map rest v))))
             (->> (d/pull-many @*conn* '[*] lookup-refs)
                  (map :shape/pt-lat-lon-sequence)))))))

(deftest test-composite-and-homogeneous-tuples-csv-to-datahike
  (d/delete-database datahike-cfg)
  (d/create-database datahike-cfg)
  (binding [*conn* (d/connect datahike-cfg)]
    (load-csv *conn* shapes-filename shape-cfg-2)
    (let [shapes-from-csv (->> (csv-to-maps shapes-filename)
                               (map #(reduce-kv (fn [m k v]
                                                  (assoc m k (read-string v)))
                                                {}
                                                %)))
          ids (d/q '[:find [?e ...] :where [?e :shape/id _]]
                   @*conn*)]
      (is (= (set (->> (d/pull-many @*conn* '[*] ids)
                       (map #(dissoc % :db/id))))
             (set (map #(-> (assoc % :shape/id-pt-sequence [(:shape/id %) (:shape/pt-sequence %)])
                            (assoc :shape/pt-lat-lon [(:shape/pt-lat %) (:shape/pt-lon %)])
                            (dissoc :shape/pt-lat)
                            (dissoc :shape/pt-lon))
                       shapes-from-csv)))))))
