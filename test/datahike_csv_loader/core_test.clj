(ns datahike-csv-loader.core-test
  (:require [clojure.java.io :as io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [charred.api :as charred]
            [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
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
    (map #(zipmap cols %) (rest csv))))

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

(defn test-agency-create-dataset [agencies-ds]
  (testing (str "create-dataset handles natural numbers, strings, and blanks in " agencies-filename)
    (is (->> (csv-to-maps agencies-filename)
             (map (fn [a]
                    (let [phone-fn #(if (str/blank? %) nil (read-string %))]
                      (-> (update a :agency/id read-string)
                          (update :agency/phone phone-fn)))))
             (map = (tc/rows agencies-ds :as-maps))
             (every? identity)))))

(defn test-agency-csv-to-datahike [agencies-ds]
  (let [agency-attrs (tc/column-names agencies-ds)]
    (csv-to-datahike agencies-filename agency-cfg *conn*)
    (testing "Unique identity, unique value, and indexed schema attributes transacted"
      (test-schema-attribute-vals agency-cfg (d/schema @*conn*) (set agency-attrs)))
    (testing "Agency data loaded correctly"
      (let [ids (d/q '[:find [?e ...] :where [?e :agency/id _]]
                     @*conn*)]
        (= (-> (d/pull-many @*conn* agency-attrs ids)
               (clean-pulled-entities (keys (:ref agency-cfg)))
               set)
           (set (dataset-for-transact agencies-ds)))))))

(defn test-route-create-dataset [routes-ds]
  (testing "create-dataset handles foreign IDs (references)"
    (->> (csv-to-maps routes-filename)
         (map #(-> (:route/agency-id %)
                   read-string))
         (map = (->> (:route/agency-id routes-ds)
                     (d/pull-many @*conn* [:agency/id])
                     (map :agency/id)))
         (every? identity))))

(defn test-route-csv-to-datahike [routes-ds]
  (let [route-attrs (tc/column-names routes-ds)]
    (csv-to-datahike routes-filename route-cfg *conn*)
    (testing "Foreign ID (reference) attributes transacted"
      (test-schema-attribute-vals route-cfg
                                  (d/schema @*conn*)
                                  (disj (set route-attrs) :db/id)))
    (testing "Route data, including foreign references, loaded into Datahike"
      (let [ids (d/q '[:find [?e ...] :where [?e :route/id _]]
                     @*conn*)]
        (= (-> (d/pull-many @*conn* route-attrs ids)
               (clean-pulled-entities (keys (:ref route-cfg)))
               set)
           (->> (dataset-for-transact routes-ds)
                (map #(dissoc % :db/id))
                set))))))

(defn test-route-trip-csv-to-datahike []
  (let [route-trip-maps (map #(update % :route/trip-id read-string)
                             (csv-to-maps route-trips-filename))
        route-trip-attrs (keys (first route-trip-maps))]
    (csv-to-datahike route-trips-filename route-trip-cfg *conn*)
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
    (let [agencies-ds (create-dataset agencies-filename)]
      (test-agency-create-dataset agencies-ds)
      (test-agency-csv-to-datahike agencies-ds)
      (let [routes-ds (create-dataset routes-filename (:ref route-cfg) @*conn*)]
        (test-route-create-dataset routes-ds)
        (test-route-csv-to-datahike routes-ds)
        (test-route-trip-csv-to-datahike)))))

(deftest test-refs-in-schema
  (testing "IllegalArgumentException is thrown when attribute not present in schema is referenced"
    (d/delete-database datahike-cfg)
    (d/create-database datahike-cfg)
    (binding [*conn* (d/connect datahike-cfg)]
      (is (thrown? IllegalArgumentException (csv-to-datahike routes-filename route-cfg *conn*))))))

(defn test-stop-create-dataset [stops-ds stop-maps]
  (let [keys-of-interest #{:stop/id :stop/name :stop/lon :stop/lat}]
    (testing "create-dataset handles floats and more strings correctly"
      (is (->> (map #(-> (select-keys % keys-of-interest)
                         (update :stop/lat read-string)
                         (update :stop/lon read-string))
                    stop-maps)
               (map = (->> (tc/rows stops-ds :as-maps)
                           (map #(select-keys % keys-of-interest))))
               (every? identity))))))

(defn test-stop-csv-to-datahike [stops-ds stop-maps]
  (let [stop-attrs (tc/column-names stops-ds)]
    (csv-to-datahike stops-filename stop-cfg *conn*)
    (testing "Schema attributes correctly transacted"
      (test-schema-attribute-vals stop-cfg
                                  (d/schema @*conn*)
                                  (disj (set stop-attrs) :db/id)))
    (testing "Stop data loaded into Datahike"
      (let [ids (d/q '[:find [?e ...] :where [?e :stop/id _]]
                     @*conn*)
            stops-dh (map #(unwrap-refs % (keys (:ref stop-cfg)))
                          (d/pull-many @*conn* stop-attrs ids))
            dissoc-keys (fn [stops] (map #(-> (dissoc % :db/id)
                                              (dissoc :stop/level-id)
                                              (dissoc :stop/parent-station))
                                         stops))]
        (is (= (set (dissoc-keys stops-dh))
               (set (-> (dataset-for-transact stops-ds)
                        dissoc-keys))))
        (testing "Self-references are correct"
          (let [db-to-stop-ids (reduce (fn [m s] (assoc m (:db/id s) (:stop/id s)))
                                       {}
                                       stops-dh)]
            (is (= (reduce (fn [m s] (->> (db-to-stop-ids (:stop/parent-station s))
                                          (assoc m (:stop/id s))))
                           {}
                           stops-dh)
                   (reduce (fn [m s]
                             (let [parent-station (:stop/parent-station s)]
                               (->> (if (empty? parent-station) nil parent-station)
                                    (assoc m (:stop/id s)))))
                           {}
                           stop-maps)))))))))

(deftest test-stop
  (d/delete-database datahike-cfg)
  (d/create-database datahike-cfg)
  (binding [*conn* (d/connect datahike-cfg)]
    (csv-to-datahike levels-filename level-cfg *conn*)
    (let [stops-ds (create-dataset stops-filename (:ref stop-cfg) @*conn*)
          stop-maps (csv-to-maps stops-filename)]
      (test-stop-create-dataset stops-ds stop-maps)
      (test-stop-csv-to-datahike stops-ds stop-maps))))

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
      (csv-to-datahike shapes-filename shape-cfg-1 *conn*)
      (is (= (->> (group-by first shapes-from-csv)
                  (map (fn [[k v]] (map rest v))))
             (->> (d/pull-many @*conn* '[*] lookup-refs)
                  (map :shape/pt-lat-lon-sequence)))))))

(deftest test-composite-and-homogeneous-tuples-csv-to-datahike
  (d/delete-database datahike-cfg)
  (d/create-database datahike-cfg)
  (binding [*conn* (d/connect datahike-cfg)]
    (csv-to-datahike shapes-filename shape-cfg-2 *conn*)
    (let [shapes-from-csv (->> (csv-to-maps shapes-filename)
                               (map #(reduce-kv (fn [m k v]
                                                  (assoc m k (read-string v)))
                                                {}
                                                %)))
          eids (->> (d/q '[:find ?e :where [?e :shape/id _]]
                         @*conn*)
                    (map first)
                    sort)]
      (is (= (->> (d/pull-many @*conn* '[*] eids)
                  (map #(dissoc % :db/id)))
             (map #(-> (assoc % :shape/id-pt-sequence [(:shape/id %) (:shape/pt-sequence %)])
                       (assoc :shape/pt-lat-lon [(:shape/pt-lat %) (:shape/pt-lon %)])
                       (dissoc :shape/pt-lat)
                       (dissoc :shape/pt-lon))
                  shapes-from-csv))))))
