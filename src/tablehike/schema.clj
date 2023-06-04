(ns tablehike.schema
  (:require [clojure.set :as set]
            [clojure.string :as string]
            [tablehike.parse.utils :as parse-utils]
            [tablehike.parse.datetime :as dt]
            [tablehike.utils :as utils])
  (:import [java.util HashMap HashSet]))


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


; TODO inline?
(defn- map-col-names->schema-dtypes [parsers]
  (into {}
        (map (fn [m {:keys [column-name parser-dtype] :as p}]
               [column-name
                (if (identical? :vector parser-dtype)
                  (mapv parser->schema-type (get p :field-parser-data))
                  (parser->schema-type parser-dtype))]))
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


; TODO check that :db/isComponent attrs are refs???
(defn- init-tx-schema [schema parsers]
  (let [col-name->dtype (map-col-names->schema-dtypes parsers)
        col-name->index (map-col-names->indices parsers)]
    (if (vector? schema)
      ; Give user-specified schema precedence: the customer is always right! ;-)
      ; (That also means taking the blame/fall if they're wrong: no free lunch!)
      (into col-name->dtype
            (map (fn [{a-ident :db/ident :as a-schema}]
                   [a-ident (-> (a-ident col-name->dtype)
                                (merge a-schema))]))
            schema)
      (let [{tuples :db.type/tuple
             composite-tuples :db.type/compositeTuple} schema
            ; First init schemas of individual composite tuple columns and of the tuples
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
            (fn [a a-schema schema-a]
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
                                    (update m a #(update-attr-schema a % schema-a))
                                    m
                                    col-attrs))
                          ident->tx-schema
                          (-> (dissoc schema :db.type/tuple)
                              (dissoc :db.type/compositeTuple))))
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
                                         (assoc :db/tupleType (first (ident col-name->dtype))))
                                     schema)])))
                   ident->tx-schema))))))


; TODO rm?
(defn- map-multi-col-idents->indices [ident->colnames col-name->index]
  (reduce-kv (fn [m ident colnames]
               (assoc m ident (mapv #(% col-name->index) colnames)))
             {}
             ident->colnames))


; TODO rm?
(defn- map-idents->indices [idents col-name->index tuple-ident->colnames]
  (into {}
        (fn [ident]
          [ident (or (get col-name->index ident)
                     (->> (get tuple-ident->colnames ident)
                          (map col-name->index)))])
        idents))


; TODO rm?
(defn- map-col-indices->idents [{tuples :db.type/tuple} parsers]
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


(definterface ISchemaBuilder
  (updateTempids [unique-attr-val])
  (updateSchema [^long row-idx ^long col-idx val]))


; TODO missing values
; TODO composite and "composite" TUPLES
; TODO finalise schema: what???
(deftype SchemaBuilder [^long id-col-indices
                        ^HashMap id-attr-val->row-idx
                        ^"[Ljava.lang.Object;" a-col-idx->evs
                        maybe-tuples
                        maybe-refs
                        unique-attr-idents
                        tx-dtypes
                        schema]
  ISchemaBuilder
  (updateSchema [row-idx row-vals]
    (if (some? id-col-indices)
      (let [row-id-attr-val (mapv #(nth row-vals %) id-col-indices)
            entity-row-idx (or (.get id-attr-val->row-idx row-id-attr-val)
                               (.put id-attr-val->row-idx row-id-attr-val row-idx)  ; always nil
                               row-idx)]
        (reduce-kv (fn [_ i v]
                     (if-some [a-evs (aget a-col-idx->evs i)]
                       ; TODO type hint or use only string arrays for schema building
                       (when (-> (aget a-evs entity-row-idx)
                                 (.equals v)
                                 not)
                         (if (.contains maybe-tuples i)
                           (let [a-schema (nth schema i)]
                             (do (->> #(-> (assoc % :db/valueType :db.type/tuple)
                                           (assoc :db/tupleType (nth tx-dtypes i)))
                                      (update schema i)
                                      (set! schema))
                                 (aset a-col-idx->evs i nil)
                                 (.remove maybe-tuples i)))
                           (do (->> #(assoc % :db/cardinality :db.cardinality/many)
                                    (update schema i)
                                    (set! schema))
                               (aset a-col-idx->evs i nil))
                           )
                         ; TODO maybe-refs. Is it mutually exclusive with the if-clause???
                         )
                     ))
      )
      )
    )))


(defn schema-builder [parsers tx-schemas {rschema-unique-id :db.unique/id
                                          rschema-unique-val :db.unique/value}]
  (let [col-name->index (map-col-names->indices parsers)
        csv-unique-attrs (into #{}
                               (map (fn [[ident {unique :db/unique}]]
                                      (when (or (identical? :db.unique/identity unique)
                                                (identical? :db.unique/value unique))
                                        ident)))
                               tx-schemas)
        id-col-indices (map col-name->index
                            (->> (first csv-unique-attrs)
                                 (into [])
             ))
        get-indices-with-missing (fn [attr]
                                   (into #{}
                                         (map (fn [a-ident a-schema]
                                                (when (nil? (get a-schema attr))
                                                  (get col-name->index a-ident))))
                                         tx-schemas))
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
        unique-attrs (when (> (count maybe-refs 0))
                       (set/union rschema-unique-id rschema-unique-val csv-unique-attrs))
        tx-dtypes (map-col-names->schema-dtypes parsers)]
    (if (some? id-col-indices)
      (SchemaBuilder. id-col-indices
                      (HashMap.)
                      (reduce (fn [array idx]
                                (->> (object-array utils/schema-inference-batch-size)
                                     (aset array idx)))
                              (object-array (count parsers))
                              (set/union no-cardinality maybe-tuples))
                      maybe-tuples maybe-refs
                      unique-attrs tx-dtypes tx-schemas)
      (SchemaBuilder. nil nil nil nil maybe-refs unique-attrs tx-dtypes tx-schemas))))
;                                      (-> (if-let [value-type (-> (nth tx-schemas idx)
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
