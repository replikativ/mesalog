(ns tablehike.schema
  (:require [clojure.set :as set]
            [clojure.string :as string]
            [tablehike.parse.utils :as parse-utils]
            [tablehike.parse.datetime :as dt]
            [tablehike.utils :as utils])
  (:import [java.util HashMap HashSet]))

; TODO
; - type-hint properly


(defn- parser->schema-dtype [parser-dtype]
  (let [translate (fn [dtype]
                    (cond
                      (identical? :bool dtype) :db.type/boolean
                      (identical? :float32 dtype) :db.type/float
                      (identical? :float64 dtype) :db.type/double
                      (contains? #{:int16 :int32 :int64} dtype) :db.type/long
                      (contains? dt/datetime-datatypes dtype) :db.type/instant
                      :else (keyword "db.type" (name dtype))))]
    (if (identical? :vector parser-dtype)
      (mapv translate (get p :field-parser-data))
      (translate parser-dtype))))


(defn- map-col-names->schema-dtypes [parsers]
  (into {}
        (map (fn [m {:keys [column-name parser-dtype] :as p}]
               [column-name
                (if (identical? :vector parser-dtype)
                  (mapv parser->schema-dtype (get p :field-parser-data))
                  (parser->schema-dtype parser-dtype))]))
        parsers))


(defn- map-col-names->indices [parsers]
  (into {}
        ; map-indexed should do too, since the indices are supposed to be strictly chronological
        (map (fn [{:keys [column-idx column-name]}]
               [column-name column-idx]))
        parsers))


(defn- map-idents->indices [parsers tuples composite-tuples]
  (let [col-name->index (map-col-names->indices parsers)
        all-tuples-map (cond-> composite-tuples
                         (map? tuples) (merge tuples))]
    (into (->> (apply dissoc col-name->index)
               (keys all-tuples-map))
          (map (fn [[t cols]]
                 [t (mapv col-name->index cols)]))
          all-tuples-map)))


(defn- map-indices->idents [tuples parsers]
  (let [tuple-col-names->ident (when (map? tuples)
                                 (reduce-kv (fn [m t attrs]
                                              (reduce (fn [m col-name]
                                                        (assoc m col-name t))
                                                      m
                                                      attrs))
                                            {}
                                            tuples))]
    (into {}
          (map (fn [{:keys [column-idx column-name]}]
                 [column-idx (-> (column-name tuple-col-names->ident)
                                 (or column-name))]))
          parsers)))


(defn- init-col-attr-schema [col-name col-parser col-schema-dtype]
  (merge {:db/ident col-name}
         (if (vector? col-schema-dtype)
           (let [{:keys [min-length max-length]} col-parser
                 homogeneous (apply = col-schema-dtype)]
             (if (= min-length max-length)
               (if homogeneous
                 {:db/valueType :db.type/tuple
                  :db/tupleType (first col-schema-dtype)}
                 ; possible type ref: leave :db/valueType undetermined until later
                 (if (and (= 2 min-length)
                          (= :db.type/keyword (nth col-schema-dtype 0)))
                   {}
                   {:db/valueType :db.type/tuple
                    :db/tupleTypes col-schema-dtype})))
             (if homogeneous
               {:db/cardinality :db.cardinality/many}
               (let [parts '("Unrecognized data type in column"
                             col-name
                             ": attribute with variable length/cardinality"
                             "and heterogeneous type not allowed")
                     msg (string/join " " parts)]
                 (throw (IllegalArgumentException. msg)))))
           {:db/valueType col-schema-dtype})))


(defn- map-tuple-idents->tx-schemas
  [schema-tuple-attrs col-name->schema-dtype]
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
             [t (->> (map col-name->schema-dtype attrs)
                     (init-tuple-schema t))]))
      (set? schema-tuple-attrs)
      (map (fn [t]
             [t (->> (get col-name->schema-dtype t)
                     (init-tuple-schema t))])))))


(defn- is-homogeneous-vector? [dtype]
  (and (vector? dtype)
       (apply = dtype)))


; TODO check that :db/isComponent attrs are refs?
(defn- map-idents->tx-schemas [parsers
                               {tuples :db.type/tuple
                                composite-tuples :db.type/compositeTuple
                                :as schema-arg}]
  (let [col-name->dtype (map-col-names->schema-dtypes parsers)
        col-name->index (map-col-names->indices parsers)]
    (if (vector? schema-arg)
      ; Give user-specified schema precedence: the customer is always right! ;-)
      ; (That also means taking the blame/fall if they're wrong: no free lunch!)
      (into col-name->dtype
            (map (fn [{a-ident :db/ident :as a-schema}]
                   [a-ident (-> (a-ident col-name->dtype)
                                (merge a-schema))]))
            schema-arg)
      (let [; First init schemas of individual composite tuple columns and of the tuples
            ident->tx-schema
            (-> (fn [m t cols]
                  (-> (fn [m col]
                        (let [p (->> (col col-name->index)
                                     (nth parsers))]
                          (->> (col-name->dtype col)
                               (init-col-attr-schema col p)
                               (assoc m col))))
                      (reduce m cols)
                      (assoc t {:db/ident t
                                :db/valueType :db.type/tuple
                                :db/tupleAttrs cols})))
                (reduce-kv {} composite-tuples)
                ; Then init schemas for regular tuples
                (into (map-tuple-idents->tx-schemas tuples col-name->dtype)
                      tuples))
            ; First exclude tuple colnames and vector-valued tuple cols from schema init
            ident->tx-schema
            (->> (if (map? tuples)
                   (mapcat identity (vals tuples))
                   tuples)
                 (apply dissoc col-name->dtype)
                 ; Then init schemas for remaining (non-tuple) columns
                 (into ident->tx-schema
                       (map (fn [[col dtype]]
                              (let [p (->> (col col-name->index)
                                           (nth parsers))]
                                [col (init-col-attr-schema col p dtype)])))))
            update-attr-schema
            (fn [a-schema schema-a]
              (let [schema-a-ns (namespace schema-a)
                    [k v] (if (.equals "db" schema-a-ns)
                            ; :db/unique, :db/index, :db/noHistory
                            ; doesn't apply to :db/ident, :db/id, :db/doc: for schema attributes only
                            [schema-a true]
                            (if (.equals "db.type" schema-a-ns)
                              [:db/valueType schema-a]
                              ; :db.cardinality/<one|many>, :db.unique/<identity|value>
                              [(keyword (str/replace schema-a-ns \. \/))
                               schema-a]))]
                (assoc a-schema k v)))]
        (->> (reduce-kv (fn [m schema-a col-attrs]
                          (reduce (fn [m a]
                                    (update m a #(update-attr-schema % schema-a))
                                    m
                                    col-attrs)))
                        ident->tx-schema
                        (-> (dissoc schema-arg :db.type/tuple)
                            (dissoc :db.type/compositeTuple)))
             (into {}
                   (map (fn [[ident schema]]
                          (let [schema (if (get schema :db/unique)
                                         (assoc schema :db/cardinality :db.cardinality/one)
                                         schema)]
                            [ident (if (and (-> (:db/cardinality schema)
                                                (identical? :db.cardinality/one))
                                            (-> (ident col-name->dtype)
                                                is-homogeneous-vector?))
                                     (-> (assoc schema :db/valueType :db.type/tuple)
                                         (assoc :db/tupleType
                                                (first (ident col-name->dtype))))
                                     schema)])))))))))


(definterface ISchemaBuilder
  (updateSchema [^long row-idx row-vals])
  (finalizeSchema []))


(deftype SchemaBuilder [index->ident
                        ident->dtype
                        ^HashMap ident->tx-schema
                        unique-attr-idents
                        vector-read-opts
                        ^HashSet maybe-refs
                        id-col-indices
                        ^HashMap id-attr-val->row-idx
                        ^"[Ljava.lang.Object;" a-col-idx->evs
                        ^HashSet maybe-tuples]
  ISchemaBuilder
  (updateSchema [row-idx row-vals]
    (let [row-id-attr-val (mapv #(nth row-vals %) id-col-indices)
          entity-row-idx (when (some? id-col-indices)
                           (or (.get id-attr-val->row-idx row-id-attr-val)
                               (.put id-attr-val->row-idx row-id-attr-val row-idx)  ; always nil
                               row-idx))]
      (reduce-kv
       (fn [_ i v]
         (when-not (utils/missing-value? v)
           (when (.contains maybe-refs i)
             (let [vector-vals (utils/vector-string->csv-vector v vector-read-opts)
                   maybe-ident (nth vector-vals 0)
                   ident (get index->ident i)
                   vector-dtypes (get ident->dtype ident)]
               ; if the first element isn't an ident, or the second has a dtype
               ; not matching that of the ident, this column consists of mere tuples
               (when-not (and (contains? unique-attr-idents maybe-ident)
                              (identical? (get ident->dtype maybe-ident)
                                          (nth vector-dtypes 1)))
                 (.put ident->tx-schema
                       ident
                       #(-> (.get ident->tx-schema ident)
                            (assoc :db/valueType :db.type/tuple)
                            (assoc :db/tupleTypes vector-dtypes))))))
           (when-some [a-evs (and (some? id-col-indices)
                                  (aget a-col-idx->evs i))]
             ; if the current row represents a previously encountered entity with a
             ; different value for this column, then either of these holds for the column:
             ; 1. It's cardinality-many.
             ; 2. It's a tuple
             (when (-> (aget a-evs entity-row-idx)
                       (.equals v)
                       not)
               (let [ident (get index->ident i)
                     tx-schema (.get ident->tx-schema ident)
                     maybe-tuple (.contains maybe-tuples i)]
                 (do (->> (if maybe-tuple
                            #(-> (assoc tx-schema :db/valueType :db.type/tuple)
                                 (assoc :db/tupleType
                                        (nth (get ident->dtype ident) 0)))
                            #(assoc tx-schema :db/cardinality :db.cardinality/many))
                          (.put ident->tx-schema ident))
                     (aset a-col-idx->evs i nil)
                     (when maybe-tuple (.remove maybe-tuples i)))))))
         nil
         row-vals))))
  (finalizeSchema []
    (let [ident->index (->> index->ident
                            (into {} (map (fn [[k v]]
                                            [v k]))))]
      (->> (into (sorted-map)
                 (map (fn [[ident schema]]
                        (let [i (get ident->index ident)]
                          [i (cond-> (if (nil? (get schema :db/valueType))
                                       (if (.contains maybe-refs i)
                                         (assoc schema :db/valueType :db.type/ref)
                                         (->> (nth (get ident->dtype ident) 0)
                                              (assoc schema :db/valueType)))
                                       schema)
                               (nil? (get schema :db/cardinality))
                               (assoc :db/cardinality :db.cardinality/one))])))
                 ident->tx-schema)
           (into [] (map #(nth % 1)))))))


; TODO test cases:
; - empty
;
(defn schema-builder [parsers
                      {tuples :db.type/tuple
                       composite-tuples :db.type/compositeTuple
                       :as schema-arg}
                      schema
                      {rschema-unique-id :db.unique/id
                       rschema-unique-val :db.unique/value}
                      options]
  (let [index->ident (map-indices->idents tuples parsers)
        ; mapping from idents for existing attributes in the DB, but colnames for CSV attrs
        ; since this information won't be needed at the ident level for CSV contents anyway
        ident->dtype (merge (into {}
                                  (map (fn [ident schema]
                                         [ident (get schema :db/valueType)]))
                                  schema)
                            (map-col-names->schema-dtypes parsers))
        ident->tx-schema (map-idents->tx-schemas parsers schema-arg)
        ident->index (map-idents->indices parsers tuples composite-tuples)
        csv-unique-attrs (into #{}
                               (map (fn [[ident {unique :db/unique}]]
                                      (when (or (identical? :db.unique/identity unique)
                                                (identical? :db.unique/value unique))
                                        ident)))
                               ident->tx-schema)
        id-col-indices ((first csv-unique-attrs) ident->index)
        unique-attrs (when (> (count maybe-refs) 0)
                       (set/union rschema-unique-id rschema-unique-val csv-unique-attrs))
        get-indices-with-missing (fn [attr]
                                   (into #{}
                                         (map (fn [ident schema]
                                                (when (nil? (get schema attr))
                                                  (get ident->index ident))))
                                         ident->tx-schema))
        no-cardinality (get-indices-with-missing :db/cardinality)
        ; 1. maybe-refs may be refs or (length-two, keyword-first) tuples
        ; 2. maybe-tuples have CSV values that are homogeneous, variable-length vectors:
        ; If any entity turns out to have multiple values for one such column,
        ; then it is assigned a :tuple schema value type;
        ; otherwise it's a cardinality-many attribute of whatever type the elements are
        {maybe-tuples true maybe-refs false}
        (->> (group-by #(->> (nth parsers %)
                             :field-parser-data
                             is-homogeneous-vector?)
                       (get-indices-with-missing :db/valueType))
             (into {} (map [[k v]]
                           [k (HashSet. (into #{} v))])))
        vector-read-opts (utils/options-for-vector-read options)
        init-schema-builder (partial SchemaBuilder.
                                     index->ident
                                     ident->dtype
                                     ident->tx-schema
                                     unique-attrs
                                     vector-read-opts
                                     maybe-refs)]
    (if (some? id-col-indices)
      (init-schema-builder id-col-indices
                           (HashMap.)
                           (reduce (fn [array idx]
                                     (->> (object-array utils/schema-inference-batch-size)
                                          (aset array idx)))
                                   (object-array (count parsers))
                                   (set/union no-cardinality maybe-tuples))
                           maybe-tuples)
      (init-schema-builder nil nil nil nil))))
;                                      (-> (if-let [value-type (-> (nth ident->tx-schema idx)
;                                                                  (get :db/valueType))]
;                                            (as-> (name value-type) type-name
;                                              (case type-name
;                                                "instant" java.util.Date
;                                                "uuid" java.util.UUID
;                                                "keyword" clojure.lang.Keyword
;                                                "symbol" clojure.lang.Symbol
;                                                (->> (capitalize type-name)
;                                                     (str "java.lang.")
;                                                     java.lang.Class/forName)))
;                                            Object)
;                                          (make-array utils/schema-inference-batch-size))))