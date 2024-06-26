(ns mesalog.schema
  (:require [clojure.set :as clj-set]
            [clojure.string :as string]
            [ham-fisted.reduce :as hamf-rf]
            [mesalog.parse.parser :as parser]
            [mesalog.parse.utils :as parse-utils]
            [mesalog.read :as csv-read])
  (:import [clojure.lang Indexed]
           [java.util HashMap HashSet]
           [mesalog.read TakeReducer]))


(defn- map-col-idents->schema-dtypes [parsers]
  (into {}
        (comp (filter (comp some? :parser-dtype))
              (map (fn [{:keys [column-ident parser-dtype] :as p}]
                     [column-ident
                      (if (identical? :vector parser-dtype)
                        (mapv #(get % :parser-dtype)
                              (get p :field-parser-data))
                        parser-dtype)])))
        parsers))


; :db/valueType not assigned if a column is vector-valued and:
; 1. values have length two, with keywords in the first position
; 2. values have varying length but elements of homogeneous type
(defn- init-col-attr-schema
  ([col-ident col-schema-dtype col-parser]
   (when (some? col-schema-dtype)
     (merge {:db/ident col-ident}
            (if (vector? col-schema-dtype)
              (let [{:keys [min-length max-length]} col-parser
                    homogeneous (parse-utils/homogeneous-sequence? col-schema-dtype)]
                (if (= min-length max-length)
                  (if homogeneous
                    {:db/valueType :db.type/tuple
                     :db/tupleType (first col-schema-dtype)}
                    ; possible type ref: leave :db/valueType undetermined until later
                    (if (and (= 2 min-length)
                             (= :db.type/keyword (nth col-schema-dtype 0)))
                      {}
                      {:db/valueType :db.type/tuple
                       :db/tupleTypes col-schema-dtype}))
                  (if homogeneous
                    {:db/cardinality :db.cardinality/many}
                    (let [parts '("Unrecognized data type in column"
                                  col-ident
                                  ": attribute with variable length/cardinality"
                                  "and heterogeneous type not allowed")
                          msg (string/join " " parts)]
                      (throw (IllegalArgumentException. msg))))))
              {:db/valueType col-schema-dtype}))))
  ([col-ident col-schema-dtype]
   (init-col-attr-schema col-ident col-schema-dtype nil)))


(defn- map-tuple-idents->tx-schemas
  [schema-tuple-attrs col-ident->schema-dtype]
  (let [init-tuple-schema
        (fn [t tuple-types]
          (merge {:db/ident t
                  :db/valueType :db.type/tuple}
                 (if (apply = tuple-types)
                   {:db/tupleType (first tuple-types)}
                   {:db/tupleTypes tuple-types})))]
    (cond
      (map? schema-tuple-attrs)
      (map (fn [[t attrs]]
             (and col-ident->schema-dtype
                  [t (->> (mapv col-ident->schema-dtype attrs)
                          (init-tuple-schema t))])))
      (set? schema-tuple-attrs)
      (map (fn [t]
             (and col-ident->schema-dtype
                  [t (->> (get col-ident->schema-dtype t)
                          (init-tuple-schema t))])))
      (nil? schema-tuple-attrs)
      (map identity))))


(defn- map-idents->tx-schemas [parsers
                               {tuples :db.type/tuple
                                composite-tuples :db.type/compositeTuple
                                refs :db.type/ref
                                :as schema-spec}]
  (let [col-ident->dtype (map-col-idents->schema-dtypes parsers)
        col-ident->index (parse-utils/map-col-idents->indices parsers)
        col-ident-dtype->tx-schema (fn [col dtype]
                                     (->> (get col-ident->index col)
                                          (nth parsers)
                                          (init-col-attr-schema col dtype)))]
    ; Give user-specified schema precedence in either case: the customer is always right! ;-)
    ; (That also means taking the blame/fall if they're wrong: no free lunch!)
    (if (vector? schema-spec)
      (into {}
            (map (fn [{a-ident :db/ident :as a-schema}]
                   [a-ident (-> {:db/valueType (a-ident col-ident->dtype)}
                                (merge a-schema))]))
            schema-spec)
      (let [ref-idents (if (map? refs)
                         (keys refs)
                         refs)
            ident->tx-schema
            ; First init schemas of individual composite tuple columns and of the tuples.
            ; Exclude if any of the attrs is missing throughout.
            (-> (fn [m t cols]
                  (let [col-dtypes (mapv col-ident->dtype cols)]
                    (if (every? identity col-dtypes)
                      (-> (fn [m i col]
                            (->> (nth col-dtypes i)
                                 (init-col-attr-schema col)
                                 (assoc m col)))
                          (reduce-kv m cols)
                          (assoc t {:db/ident t
                                    :db/valueType :db.type/tuple
                                    :db/tupleAttrs cols}))
                      m)))
                (reduce-kv {} composite-tuples)
                                        ; Then init schemas for regular tuples
                (into (comp (map-tuple-idents->tx-schemas tuples col-ident->dtype)
                            (filter some?))
                      tuples)
                ; Then for refs
                (into (map (fn [ident]
                             [ident {:db/ident ident
                                     :db/valueType :db.type/ref}]))
                      ref-idents))
            ; First exclude refs, tuple colnames, and vector-valued tuple cols from schema init
            ident->tx-schema
            (->> (if (map? tuples)
                   (mapcat identity (vals tuples))
                   tuples)
                 (concat ref-idents)
                 (apply dissoc col-ident->dtype)
                 ; Then init schemas for remaining (non-tuple) columns
                 (into ident->tx-schema
                       (map (fn [[col dtype]]
                              [col (col-ident-dtype->tx-schema col dtype)]))))
            update-attr-schema
            (fn [a-schema schema-a]
              (let [schema-a-ns (namespace schema-a)
                    [k v] (if (->> (name schema-a)
                                   (contains? #{"index" "isComponent" "noHistory"})
                                   (and (.equals "db" schema-a-ns)))
                            ; doesn't apply to :db/ident, :db/id, :db/doc
                            [schema-a true]
                            (if (.equals "db.type" schema-a-ns)
                              [:db/valueType schema-a]
                              ; :db.cardinality/<one|many>, :db.unique/<identity|value>
                              [(keyword (string/replace schema-a-ns \. \/))
                               schema-a]))]
                (assoc a-schema k v)))]
        (->> (dissoc schema-spec :db.type/tuple :db.type/compositeTuple :db.type/ref)
             (reduce-kv (fn [m schema-a col-attrs]
                          (reduce (fn [m a]
                                    (if (contains? m a)
                                      (update m a #(update-attr-schema % schema-a))
                                      ; in any unusual case where columns are used as both
                                      ; independent attributes and part of a tuple
                                      (if (contains? col-ident->dtype a)
                                        (assoc m a (update-attr-schema
                                                    (->> (col-ident->dtype a)
                                                         (col-ident-dtype->tx-schema a))
                                                    schema-a))
                                        m)))
                                  m
                                  col-attrs))
                        ident->tx-schema)
             (into {}
                   ; Assign:
                   ; 1. cardinality-one to unique attrs
                   ; 2. Homogeneous tuple type to cardinality-one, homogeneous vectors
                   (map (fn [[ident schema]]
                          (let [schema (if (get schema :db/unique)
                                         (assoc schema :db/cardinality :db.cardinality/one)
                                         schema)]
                            [ident (if (and (-> (:db/cardinality schema)
                                                (identical? :db.cardinality/one))
                                            (-> (ident col-ident->dtype)
                                                parse-utils/homogeneous-sequence?))
                                     (-> (assoc schema :db/valueType :db.type/tuple)
                                         (assoc :db/tupleType
                                                (first (ident col-ident->dtype))))
                                     schema)])))))))))


(definterface ISchemaBuilder
  (updateMaybeRefs [i ^String v])
  (updateSchema [^long row-idx row-vals])
  (finalizeSchema []))


(deftype SchemaBuilder [^Indexed col-index->name
                        ident->indices
                        ident->dtype
                        ^HashMap ident->tx-schema
                        unique-attr-idents
                        string->vector-parser
                        ^HashSet maybe-refs
                        id-col-indices
                        ^HashMap id-attr-val->row-idx
                        ^"[Ljava.lang.Object;" a-col-idx->evs
                        ^HashSet maybe-tuples]
  ISchemaBuilder
  (updateMaybeRefs [_this i ^String v]
    (when (and (some? maybe-refs)
               (.contains maybe-refs i))
      (let [vector-vals (string->vector-parser v)
            maybe-ident (nth vector-vals 0)
            ident (.nth col-index->name i)
            vector-dtypes (get ident->dtype ident)]
        ; if the first element isn't an ident, or the second has a dtype
        ; not matching that of the ident, this column consists of mere tuples
        (when-not (and (contains? unique-attr-idents maybe-ident)
                       (identical? (get ident->dtype maybe-ident)
                                   (nth vector-dtypes 1)))
          (do (.put ident->tx-schema
                    ident
                    (-> (.get ident->tx-schema ident)
                        (assoc :db/valueType :db.type/tuple)
                        (assoc :db/tupleTypes vector-dtypes)))
              (.remove maybe-refs i))))))
  (updateSchema [_this row-idx row-vals]
    (if (nil? id-col-indices)
      (-> (fn [_ i ^String v]
            (when-not (csv-read/missing-value? v)
              (.updateMaybeRefs _this i v)))
          (reduce-kv nil row-vals))
      (let [row-id-attr-val (mapv #(nth row-vals %) id-col-indices)
            existing-entity (.get id-attr-val->row-idx row-id-attr-val)
            entity-row-idx (or existing-entity
                               (.put id-attr-val->row-idx row-id-attr-val row-idx)  ; always nil
                               row-idx)]
        (-> (fn [_ i ^String v]
              (when-not (csv-read/missing-value? v)
                (.updateMaybeRefs _this i v)
                (when-let [a-evs ^"[Ljava.lang.Object;" (aget a-col-idx->evs i)]
                  (if existing-entity
                    ; if the current row represents a previously encountered entity with a
                    ; different value for this column, then either of these holds for the column:
                    ; 1. It's cardinality-many.
                    ; 2. It's part of a tuple
                    (when (not (-> (aget a-evs entity-row-idx)
                                   (.equals ^String v)))
                      (do (when (and maybe-tuples
                                     (.contains maybe-tuples i))
                            (let [ident (.nth col-index->name i)]
                              (do (.put ident->tx-schema
                                        ident
                                        (-> (.get ident->tx-schema ident)
                                            (assoc :db/valueType :db.type/tuple)
                                            (assoc :db/tupleType
                                                   (nth (get ident->dtype ident) 0))))
                                  (.remove maybe-tuples i))))
                          (aset a-col-idx->evs i nil)))
                    (aset a-evs entity-row-idx v)))))
            (reduce-kv nil row-vals)))))
  (finalizeSchema [_this]
    (->> (into (sorted-map)
               (map (fn [[ident schema]]
                      (let [indices (get ident->indices ident)
                            schema (if (some? (get schema :db/valueType))
                                     schema
                                     (->> (if (and maybe-refs
                                                   (.contains maybe-refs
                                                              (nth indices 0)))
                                            :db.type/ref
                                            (nth (get ident->dtype ident) 0))
                                          (assoc schema :db/valueType)))]
                        [indices (if (some? (get schema :db/cardinality))
                                   schema
                                   ; cardinality-1 if there are no ID columns, or every
                                   ; attr column still has an entry in a-col-idx->evs
                                   (->> (if (or (nil? a-col-idx->evs)
                                                (-> #(some? (aget a-col-idx->evs %))
                                                    (every? indices)))
                                          :db.cardinality/one
                                          :db.cardinality/many)
                                        (assoc schema :db/cardinality)))])))
               ident->tx-schema)
         (into [] (map #(nth % 1))))))


(defn- update-schema! [^ISchemaBuilder schema-builder row-idx row-vals]
  (.updateSchema schema-builder row-idx row-vals))


(defn- finalize-schema [^ISchemaBuilder schema-builder]
  (.finalizeSchema schema-builder))


(defn schema-builder [parsers
                      {tuples :db.type/tuple
                       composite-tuples :db.type/compositeTuple
                       :as schema-spec}
                      schema
                      {rschema-unique-id :db.unique/id
                       rschema-unique-val :db.unique/value}
                      options]
  (let [; mapping from idents for existing attributes in the DB, but colnames for CSV attrs
        ; since this information won't be needed at the ident level for CSV contents anyway
        ident->dtype (merge (into {}
                                  (map (fn [[ident schema]]
                                         [ident (get schema :db/valueType)]))
                                  schema)
                            (map-col-idents->schema-dtypes parsers))
        ident->tx-schema (map-idents->tx-schemas parsers schema-spec)
        csv-unique-attrs (into #{}
                               (comp (map (fn [[ident {unique :db/unique}]]
                                            (when (or (identical? :db.unique/identity unique)
                                                      (identical? :db.unique/value unique))
                                              ident)))
                                     (filter some?))
                               ident->tx-schema)
        ident->indices (-> (keys ident->tx-schema)
                           (parse-utils/map-idents->indices parsers tuples composite-tuples))
        id-col-indices (when-not (empty? csv-unique-attrs)
                         ((first csv-unique-attrs) ident->indices))
        get-indices-with-missing (fn [attr]
                                   (reduce-kv (fn [s ident schema]
                                                (if (nil? (get schema attr))
                                                  (into s (get ident->indices ident))
                                                  s))
                                              #{}
                                              ident->tx-schema))
        no-cardinality (get-indices-with-missing :db/cardinality)
        ; 1. maybe-refs may be refs or (length-two, keyword-first) tuples
        ; 2. maybe-tuples have CSV values that are homogeneous, variable-length vectors:
        ; If any entity turns out to have multiple values for one such column,
        ; then it is assigned a :tuple schema value type;
        ; otherwise it's a cardinality-many attribute of whatever type the elements are
        {maybe-tuples true maybe-refs false}
        (->> (group-by (fn [x]
                         (->> (nth parsers x)
                              :field-parser-data
                              (map :parser-dtype)
                              parse-utils/homogeneous-sequence?))
                       (get-indices-with-missing :db/valueType))
             (into {} (map (fn [[k v]]
                             [k (HashSet. (into #{} v))]))))
        unique-attrs (when (> (count maybe-refs) 0)
                       (clj-set/union rschema-unique-id rschema-unique-val csv-unique-attrs))
        init-schema-builder
        (fn [id-col-indices id-attr-val->row-idx a-col-idx->evs maybe-tuples]
          (SchemaBuilder. (mapv :column-ident parsers)
                          ident->indices
                          ident->dtype
                          (HashMap. ident->tx-schema)
                          unique-attrs
                          (parse-utils/vector-str->elmt-strs-fn options)
                          maybe-refs
                          id-col-indices
                          id-attr-val->row-idx
                          a-col-idx->evs
                          maybe-tuples))]
    (if (some? id-col-indices)
      (init-schema-builder id-col-indices
                           (HashMap.)
                           (reduce (fn [array idx]
                                     (->> (or (:schema-sample-size options) 12800)
                                          object-array
                                          (aset ^"[Ljava.lang.Object;" array idx))
                                     array)
                                   (object-array (count parsers))
                                   (clj-set/union no-cardinality maybe-tuples))
                           maybe-tuples)
      (init-schema-builder nil nil nil nil))))


(defn build-schema [parsers schema-spec schema rschema row-iter options]
  (let [builder (schema-builder parsers schema-spec schema rschema options)]
    (reduce (->> (update-schema! builder row-idx row)
                 (hamf-rf/indexed-accum acc row-idx row))
            nil
            (->> (or (:schema-sample-size options) 12800)
                 (TakeReducer. row-iter)))
    (finalize-schema builder)))
