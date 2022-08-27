(ns datahike-csv-loader.core
  (:require [clojure.string :as str]
            [datahike.api :as d]
            [tablecloth.api :as tc]))

(defn- tc-to-datahike-types [datatype]
  (case datatype
    :float64 :db.type/double
    (:int16 :int32 :int64) :db.type/long
    (keyword "db.type" (name datatype))))

(defn- datahike-to-tc-types [datatype]
  (case datatype
    :db.type/double :float64
    :db.type/long :int64
    (keyword (name datatype))))

(defn- assoc-mismatched-ref-type [m k coltypes ref-type]
  (if (not= (k coltypes) ref-type)
    (assoc m k ref-type)
    m))

(defn- get-column-info [ds]
  (tc/info ds :columns))

(defn- filter-ref-cols [ref-cols col-names]
  (->> (filter (fn [[k v]] (v col-names))
               ref-cols)
       (into {})))

(defn- convert-ref-col-types [ds cols-info self-ref-cols other-ref-cols schema]
  (let [coltypes (zipmap (:name cols-info) (:datatype cols-info))
        self-ref-types (reduce (fn [m [k v]]
                                 (->> (v coltypes)
                                      (assoc-mismatched-ref-type m k coltypes)))
                               {}
                               self-ref-cols)
        other-ref-types (reduce (fn [m [k v]]
                                  (if (some? (v schema))
                                    (->> (:db/valueType (v schema))
                                         datahike-to-tc-types
                                         (assoc-mismatched-ref-type m k coltypes))
                                    (throw (IllegalArgumentException. "Foreign IDs must refer to attribute already in schema"))))
                                {}
                                other-ref-cols)
        conversion-map (merge self-ref-types other-ref-types)]
    (if (not-empty conversion-map)
      (tc/convert-types ds conversion-map)
      ds)))

(defn- add-tempid-col [ds]
  (let [range-end (- (+ (tc/row-count ds) 1))
        tempid-range (range -1 range-end -1)]
    (tc/add-column ds :db/id tempid-range)))

(defn- refs-to-tempids [ds self-ref-cols]
  (reduce-kv (fn [m k v]
               (let [non-nil-refs (->> (zipmap (v ds) (:db/id ds))
                                       (filter (fn [[k v]] (some? k)))
                                       (into {}))]
                 (assoc m k non-nil-refs)))
             {}
             self-ref-cols))

(defn- refs-to-eids [ds foreign-ref-cols db]
  (reduce-kv (fn [m k v]
               (let [non-nil-refs (filter some? (k ds))]
                 (->> (map (fn [ref-val] [v ref-val]) non-nil-refs)
                      (d/pull-many db '[:db/id])
                      (map :db/id)
                      (zipmap non-nil-refs)
                      (assoc m k))))
             {}
             foreign-ref-cols))

(defn- refs-to-ids [ds self-ref-cols foreign-ref-cols db]
  (merge (refs-to-tempids ds self-ref-cols)
         (refs-to-eids ds foreign-ref-cols db)))

(defn- update-ref-cols [ds ref-id-maps]
  (let [refcols (keys ref-id-maps)]
    (->> (map (fn [k]
                (partial map #((k ref-id-maps) %)))
              refcols)
         (tc/update-columns ds refcols))))

(defn- handle-ref-cols [ds cols-info self-ref-cols foreign-ref-cols db]
  (let [ds (->> (d/schema db)
                (convert-ref-col-types ds cols-info self-ref-cols foreign-ref-cols)
                add-tempid-col)]
    (update-ref-cols ds (refs-to-ids ds self-ref-cols foreign-ref-cols db))))

(defn- dataset-with-ref-cols [ds ref-cols db]
  (let [cols-info (get-column-info ds)
        self-ref-cols (filter-ref-cols ref-cols (set (:name cols-info)))
        schema (d/schema db)
        foreign-ref-cols (when (< (count self-ref-cols) (count ref-cols))
                           (filter-ref-cols ref-cols schema))]
    (if (or (pos? (count self-ref-cols))
            (pos? (count foreign-ref-cols)))
      (handle-ref-cols ds cols-info self-ref-cols foreign-ref-cols db)
      ds)))

(defn create-dataset
  ([csv] (create-dataset csv nil nil))
  ([csv ref-cols db] (cond-> (tc/dataset csv {:key-fn keyword})
                       (and (some? ref-cols)
                            (pos? (count ref-cols))) (dataset-with-ref-cols ref-cols db))))

(defn- column-schema-attributes [cfg col-info]
  (let [{:keys [id unique index ref cardinality-many]} cfg
        {col-name :name, col-datatype :datatype} col-info]
    (cond-> {:db/ident       col-name
             :db/cardinality (if (col-name cardinality-many)
                               :db.cardinality/many
                               :db.cardinality/one)
             :db/valueType  (if (col-name ref)
                              :db.type/ref
                              (tc-to-datahike-types col-datatype))}
      (col-name unique) (assoc :db/unique :db.unique/value)
      ;; unique identity overrides unique value if both are specified
      (col-name id) (assoc :db/unique :db.unique/identity)
      ;; :db/index true is not recommended for unique identity attribute
      (and (col-name index) (not (col-name id))) (assoc :db/index true))))

(defn extract-schema [schema cols-cfg ds]
  (if-let [cardinality-many-attrs (:cardinality-many cols-cfg)]
    (when (> (count cardinality-many-attrs) 1)
      (throw (IllegalArgumentException. "Each file is allowed at most one cardinality-many attribute"))))
  (let [include-cols (-> (filter #(% schema) (tc/column-names ds))
                         (conj :db/id)
                         set
                         complement)]
    (mapv #(column-schema-attributes cols-cfg %)
          (-> (tc/select-columns ds include-cols)
              get-column-info
              (tc/rows :as-maps)))))

(defn write-schema [conn cols-cfg ds]
  (->> (extract-schema (d/schema @conn) cols-cfg ds)
       (d/transact conn)))

(defn dataset-for-transact [ds]
  (mapv #(-> (fn [m k v]
               (if (some? v)
                 (conj! m [k v])
                 m))
             (reduce-kv (transient {}) %)
             persistent!)
        (tc/rows ds :as-maps)))

(defn csv-to-datahike [csv-file csv-cfg conn]
  (let [ds (create-dataset csv-file (:ref csv-cfg) @conn)]
    (write-schema conn csv-cfg ds)
    (->> (dataset-for-transact ds)
         (d/transact conn))))
