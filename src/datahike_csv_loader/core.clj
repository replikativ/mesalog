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
        foreign-ref-cols (->> (filter (fn [[k v]] (not (k self-ref-cols)))
                                      ref-cols)
                              (into {}))]
    (if (or (pos? (count self-ref-cols))
            (pos? (count foreign-ref-cols)))
      (handle-ref-cols ds cols-info self-ref-cols foreign-ref-cols db)
      ds)))

(defn create-dataset
  ([csv] (create-dataset csv nil nil))
  ([csv ref-cols db] (cond-> (tc/dataset csv {:key-fn keyword})
                       (and (some? ref-cols)
                            (pos? (count ref-cols))) (dataset-with-ref-cols ref-cols db))))

(defn- column-info-maps [ds cols]
  (-> (tc/select-columns ds cols)
      get-column-info
      (tc/rows :as-maps)))

(defn- required-schema-attrs
  ([cfg col-name] (required-schema-attrs cfg col-name nil))
  ([cfg col-name col-dtype] (let [{:keys [ref cardinality-many tuple]} cfg]
                              {:db/ident          col-name
                               :db/cardinality    (if (col-name cardinality-many)
                                                    :db.cardinality/many
                                                    :db.cardinality/one)
                               :db/valueType      (cond
                                                    (col-name ref) :db.type/ref
                                                    (col-name tuple) :db.type/tuple
                                                    :else (tc-to-datahike-types col-dtype))})))

(defn- optional-schema-attrs [cfg col-name required-attrs]
  (let [{:keys [unique-id unique-val index]} cfg]
    (cond-> required-attrs
      (col-name unique-val) (assoc :db/unique :db.unique/value)
      ;; unique identity overrides unique value if both are specified
      (col-name unique-id) (assoc :db/unique :db.unique/identity)
      ;; :db/index true is not recommended for unique identity attribute
      (and (col-name index) (not (col-name unique-id))) (assoc :db/index true))))

(defn- column-schema-attrs
  ([cfg col-name] (column-schema-attrs cfg col-name nil))
  ([cfg col-name col-dtype] (->> (required-schema-attrs cfg col-name col-dtype)
                                 (optional-schema-attrs cfg col-name))))

(defn extract-schema [db-schema cols-cfg ds]
  (if-let [cardinality-many-attrs (:cardinality-many cols-cfg)]
    (when (> (count cardinality-many-attrs) 1)
      (throw (IllegalArgumentException. "Each file is allowed at most one cardinality-many attribute"))))
  (let [{:keys [unique-id tuple]} cols-cfg
        [composite-tuples other-tuples] (reduce (fn [tuples k]
                                                  (if (k unique-id)
                                                    (update tuples 0 #(conj % k))
                                                    (update tuples 1 #(conj % k))))
                                                ['() '()]
                                                (keys tuple))
        composite-tuple-schemas (map #(-> (column-schema-attrs cols-cfg %)
                                          (assoc :db/tupleAttrs (% tuple)))
                                     composite-tuples)
        other-tuple-schemas (map (fn [k]
                                   (let [tuple-dtypes (->> (column-info-maps ds (k tuple))
                                                           (mapv #(tc-to-datahike-types (:datatype %))))
                                         tuple-dtypes-count (count (set tuple-dtypes))
                                         tuple-schema (column-schema-attrs cols-cfg k)]
                                     (if (> tuple-dtypes-count 1)
                                       (assoc tuple-schema :db/tupleTypes tuple-dtypes)
                                       (assoc tuple-schema :db/tupleType (first tuple-dtypes)))))
                                 other-tuples)
        tuple-cols-to-drop (mapcat #(% tuple) other-tuples)
        include-cols (complement (-> (filter #(% db-schema) (tc/column-names ds))
                                     (into tuple-cols-to-drop)
                                     (conj :db/id)
                                     set))]
    (->> (column-info-maps ds include-cols)
         (mapv #(column-schema-attrs cols-cfg (:name %) (:datatype %)))
         (concat composite-tuple-schemas other-tuple-schemas))))

(defn- merge-entity-rows [rows merge-attr]
  (reduce (fn [vals row]
            (-> (merge vals (dissoc row merge-attr))
                (update merge-attr #(conj % (merge-attr row)))))
          (update (first rows) merge-attr vector)
          (rest rows)))

(defn dataset-for-transact
  ([ds]
   (dataset-for-transact ds nil []))
  ([ds cfg tuple-names]
   (let [ds-to-tx (mapv (fn [row]
                          (let [nils-removed (reduce-kv (fn [m k v]
                                                          (if (some? v)
                                                            (conj! m [k v])
                                                            m))
                                                        (transient {})
                                                        row)]
                            (persistent! (reduce (fn [row tname]
                                                   (let [tuple-cols (tname (:tuple cfg))
                                                         tval (mapv row tuple-cols)]
                                                     (if (some? (reduce #(or %1 %2) tval))
                                                       (reduce (fn [m t] (dissoc! m t))
                                                               (assoc! row tname tval)
                                                               tuple-cols)
                                                       row)))
                                                 nils-removed
                                                 tuple-names))))
                        (tc/rows ds :as-maps))]
     (if (:cardinality-many cfg)
       (let [id-attr (first (:id cfg))
             merge-attr (first (:cardinality-many cfg))]
         (->> (vals (group-by id-attr ds-to-tx))
              (map #(merge-entity-rows % merge-attr))))
       ds-to-tx))))

(defn csv-to-datahike [csv-file cols-cfg conn]
  (let [ds (create-dataset csv-file (:ref cols-cfg) @conn)
        data-schema (extract-schema (d/schema @conn) cols-cfg ds)]
    (d/transact conn data-schema)
    (->> (filter #(and (= (:db/valueType %) :db.type/tuple)
                       (or (:db/tupleType %) (:db/tupleTypes %)))
                 data-schema)
         (map :db/ident)
         (dataset-for-transact ds cols-cfg)
         (d/transact conn))))
