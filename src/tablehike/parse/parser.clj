(ns tablehike.parse.parser
  (:require [datahike.api :as d]
            [clojure.java.io :as io]
            [charred.api :as charred]
            [charred.coerce :as coerce]
            [tablehike.parse.datetime :as dt]
            [tech.v3.dataset.impl.column-base :as column-base]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.dataset.io.column-parsers :refer [parse-failure missing] :as parsers]
            [tech.v3.dataset.io.context :as parse-context]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.datetime.operations :as dt-ops]
            [tech.v3.parallel.for :as pfor]
            [ham-fisted.api :as hamf])
  (:import [clojure.lang IFn]
           [clojure.lang IReduceInit]
           [java.time.format DateTimeFormatter]
           [java.util Iterator List]
           [ham_fisted IMutList Casts]
           [org.roaringbitmap RoaringBitmap]
           [tech.v3.dataset Text]))


(deftype ^:private TakeReducer [^Iterator src
                                ^{:unsynchronized-mutable true
                                  :tag long} count]
  IReduceInit
  (reduce [this rfn acc]
    (let [cnt count]
      (loop [idx 0
             continue? (.hasNext src)
             acc acc]
        (if (and continue? (< idx cnt))
          (let [acc (rfn acc (.next src))]
            (recur (unchecked-inc idx) (.hasNext src) acc))
          (do
            (set! count (- cnt idx))
            acc))))))


(definterface PParser
  (inferType [^long idx value])
  (getData []))


(defn infer-type!
  [^PParser p ^long idx value]
  (.inferType p idx value))


(defn- wrap-parser-data
  ([parser-dtype parse-fn missing-indexes failed-indexes failed-values]
   (cond-> {:parser-dtype parser-dtype
            :parse-fn parse-fn
            :missing-indexes missing-indexes}
     failed-indexes (merge {:failed-indexes failed-indexes
                            :failed-values failed-values})))
  ([parser-dtype parse-fn missing-indexes]
   (wrap-parser-data parser-dtype parse-fn missing-indexes nil nil)))


(defn make-safe-parse-fn [parse-fn]
  (fn [val]
    (try
      (parse-fn val)
      (catch Throwable _e
        parse-failure))))


(defn datetime-formatter-parse-fn
  "Given a datatype and a formatter return a function that attempts to
  parse that specific datatype, then convert into a java.util.Date."
  [dtype formatter]
  (make-safe-parse-fn (dt/datetime-formatter-parse-fn dtype formatter)))


(def default-coercers
  (-> (apply dissoc parsers/default-coercers [:bool :text])
      (assoc :string #(if (string? %) % (str %)))
      dt/update-datetime-coercers))


(defn- parser-description->dtype-fn-tuple [parser-entry]
  (let [[dtype parse-fn] (if (vector? parser-entry)
                           parser-entry
                           [parser-entry])
        formattable-dt-dtypes (dissoc dt/datetime-datatypes :instant :packed-instant)]
    (assert (keyword? dtype))
    [dtype
     (cond
       (instance? IFn parse-fn)
       parse-fn
       (and (formattable-dt-dtypes dtype)
            (string? parse-fn))
       (datetime-formatter-parse-fn dtype (DateTimeFormatter/ofPattern parse-fn))
       (and (formattable-dt-dtypes dtype)
            (instance? DateTimeFormatter parse-fn))
       (datetime-formatter-parse-fn dtype parse-fn)
       :else
       (if-let [ret-fn (get default-coercers dtype)]
         ret-fn
         (throw (IllegalArgumentException.
                 (format "Unrecognized data and parse function type: %s and %s" dtype parse-fn)))))]))


(defn missing-value?
  "Is this a missing value coming from a CSV file"
  [value]
  (cond
    (or (instance? Double value) (instance? Float value))
    (Double/isNaN (Casts/doubleCast value))
    (not (instance? Number value))
    (or (nil? value)
        (.equals "" value)
        (identical? value :tech.v3.dataset/missing)
        (and (string? value) (re-matches #"(?i)^n\/?a$" value)))))


(defn fast-dtype [value]
  (if (string? value)
    :string
    (dtype/datatype value)))


(deftype FixedTypeParser [parser-dtype
                          parse-fn
                          ^RoaringBitmap missing-indexes
                          ^RoaringBitmap failed-indexes
                          ^IMutList failed-values]
  PParser
  (inferType [_this idx value]
    (cond
      (missing-value? value)
      (.add missing-indexes (unchecked-int idx))
      (or (string? value)
          (not (identical? (fast-dtype value) parser-dtype)))
      (when (identical? parse-failure (parse-fn value))
        (do (.add failed-indexes (unchecked-int idx))
            (.add failed-values value)))))
  (getData [_this]
    (wrap-parser-data parser-dtype parse-fn missing-indexes failed-values failed-indexes)))


(defn make-fixed-parser
  ^PParser [parser-entry options]
  (let [[dtype parse-fn]    (parser-description->dtype-fn-tuple parser-entry)
        failed-values       (dtype/make-container :list :object 0)
        failed-indexes      (bitmap/->bitmap)
        missing-indexes     (bitmap/->bitmap)]
    (FixedTypeParser. dtype parse-fn missing-indexes failed-values failed-indexes)))


(def default-parser-datatype-sequence
  [:boolean :int16 :int32 :int64 :float64 :uuid
   :local-date :zoned-date-time :string])


;; Gets next parser function that can be applied to value without resulting in a failure,
;; and its associated datatype
(defn- find-next-parser
  [value parser-dtype ^List promotion-list]
  (let [start-idx (.indexOf (mapv first promotion-list) parser-dtype)
        n-elems (.size promotion-list)]
    (if (== start-idx -1)
      [:object nil]
      (long (loop [idx (inc start-idx)]
              (if (< idx n-elems)
                (let [[parser-dtype parse-fn]
                      (.get promotion-list idx)
                      parsed-value (parse-fn value)]
                  (if (= parsed-value parse-failure)
                    (recur (inc idx))
                    [parser-dtype parse-fn]))
                [:object nil]))))))


(deftype PromotionalStringParser [^{:unsynchronized-mutable true} parser-dtype
                                  ^{:unsynchronized-mutable true} parse-fn
                                  ^RoaringBitmap missing-indexes
                                  ^List promotion-list
                                  column-name]
  PParser
  (inferType [_this idx value]
    (cond
      (missing-value? value)
      (.add missing-indexes (unchecked-int idx))
      (and (not (identical? (fast-dtype value) parser-dtype))
           parse-fn)
      (let [parsed-value (parse-fn value)]
        (when (identical? parse-failure parsed-value)
          (let [[new-dtype new-parse-fn] (find-next-parser value parser-dtype promotion-list)]
            (if new-parse-fn
              (do
                (set! parser-dtype new-dtype)
                (set! parse-fn new-parse-fn))
              (throw (IllegalArgumentException.
                      (format "Unable to parse value %s in row %s of column %s" value idx column-name)))))))
      (nil? parse-fn)
      (throw (IllegalArgumentException.
              (format "`nil` parse function not allowed in promotional parser but found in column %s"
                      column-name)))))
  (getData [_this]
    (wrap-parser-data parser-dtype parse-fn missing-indexes)))


(defn promotional-string-parser
  (^PParser [column-name parser-datatype-sequence options]
   (let [first-dtype (first parser-datatype-sequence)]
     (PromotionalStringParser. first-dtype
                               (default-coercers first-dtype)
                               (bitmap/->bitmap)
                               (mapv (juxt identity default-coercers)
                                     parser-datatype-sequence)
                               column-name)))
  (^PParser [column-name options]
   (promotional-string-parser column-name default-parser-datatype-sequence options)))


(defn- make-colname [col-idx->colname col-idx]
  (let [colname (col-idx->colname col-idx)]
    (if (empty? colname)
      (str "column-" col-idx)
      colname)))


(defn- options->parser-fn
  "Given the (user-specified) options map for parsing, create the specific parse context,
  i.e. a function that produces a column parser for a given column name or index.
  `:parser-type` can be `nil`, for example if only columns with specified parser functions
  or datatypes should be parsed."
  [options parser-type col-idx->colname]
  (let [default-parse-fn (case (get options :parser-type parser-type)
                           :string promotional-string-parser
                           nil (constantly nil))
        parser-descriptor (:parser-fn options)]
    (fn [col-idx]
      (cond
        (nil? parser-descriptor)
        (default-parse-fn options)
        (map? parser-descriptor)
        (let [colname (col-idx->colname col-idx)
              colname (if (empty? colname)
                        (make-colname col-idx)
                        colname)
              col-parser-desc (or (get parser-descriptor colname)
                                  (get parser-descriptor col-idx))]
          (if col-parser-desc
            (make-fixed-parser col-parser-desc options)
            (default-parse-fn options)))
        :else
        (make-fixed-parser parser-descriptor options)))))


(defn- options->col-idx-parse-context
  "Given an option map, a parse type, and a function mapping column index to column name,
  return a map of parsers and a function to get a parser from a given column idx. Returns:
  {:parsers - parsers
   :col-idx->parser - given a column idx, get a parser.  Mutates parsers."
  [options parse-type col-idx->colname]
  (let [parse-context (options->parser-fn options parse-type col-idx->colname)
        parsers (parse-context/->ObjectArrayList (object-array 16))
        colparser-compute-fn (fn [col-idx]
                               (let [col-idx (long col-idx)]
                                 {:column-idx col-idx
                                  :column-name (keyword (make-colname col-idx->colname col-idx))
                                  :column-parser (parse-context col-idx)}))
        col-idx->parser (fn [col-idx]
                          (let [col-idx (long col-idx)]
                            (if-let [parser (.readObject parsers col-idx)]
                              (parser :column-parser)
                              (let [parser (colparser-compute-fn col-idx)]
                                (.writeObject parsers col-idx parser)
                                (parser :column-parser)))))]
    {:parsers parsers
     :col-idx->parser col-idx->parser}))


(defn- derive-parsers [^Iterator row-iter header-row options]
  (let [num-rows (long (get options :n-records
                            (get options :num-rows Long/MAX_VALUE)))
        {:keys [parsers col-idx->parser]}
        (options->col-idx-parse-context
         options :string (fn [^long col-idx]
                           (get header-row col-idx)))]
    (reduce (hamf/indexed-accum
             acc row-idx row
             (reduce (hamf/indexed-accum
                      acc col-idx field
                      (-> (col-idx->parser col-idx)
                          (infer-type! row-idx field)))
                     nil
                     row))
            nil
            (TakeReducer. row-iter num-rows))
    (mapv (fn [{:keys [column-parser]}]
            (.getData column-parser))
          parsers)))


(defn csv->parsers
  [input {:keys [header-row?]
          parser-fns :parser-fn
          :or {header-row? true}
          :as options}]
  (let [row-iter (->> (update options :batch-size #(or % 128000))
                      (charred/read-csv-supplier (ds-io/input-stream-or-reader input))
                      (coerce/->iterator)
                      pfor/->iterator)
        n-initial-skip-rows (long (get options :n-initial-skip-rows 0))
        _ (dotimes [idx n-initial-skip-rows]
            (when (.hasNext row-iter) (.next row-iter)))
        header-row (when (and header-row? (.hasNext row-iter))
                     (vec (.next row-iter)))]
    (when (.hasNext row-iter)
      (derive-parsers row-iter header-row (dissoc options :batch-size)))))
