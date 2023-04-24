(set! *warn-on-reflection* 1)

(ns tablehike.parse.parser
  (:require [datahike.api :as d]
            [clojure.java.io :as io]
            [clojure.set :as cljset]
            [charred.api :as charred]
            [charred.coerce :as coerce]
            [tablehike.parse.datetime :as dt]
            [tablehike.parse.utils :as utils]
            [tech.v3.dataset.impl.column-base :as column-base]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.dataset.io.column-parsers :refer [parse-failure missing] :as parsers]
            [tech.v3.dataset.io.context :as parse-context]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.datetime.operations :as dt-ops]
            [tech.v3.parallel.for :as pfor]
            [ham-fisted.api :as hamf]
            [clojure.string :as str])
  (:import [clojure.lang IFn IReduceInit PersistentVector]
           [java.time.format DateTimeFormatter]
           [java.util Iterator List]
           [ham_fisted IMutList Casts]
           [org.roaringbitmap RoaringBitmap]
           [tech.v3.dataset Text]
           [tech.v3.dataset.io.context ObjectArrayList]))


(def default-coercers
  (-> (apply dissoc parsers/default-coercers [:bool :text])
      (assoc :string #(if (string? %) % (str %)))
      dt/update-datetime-coercers))


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
  (parseValue [^long idx value])
  (dataIntoMap []))


(defn parse-value!
  [^PParser p ^long idx value]
  (.parseValue p idx value))


(defn data-into-map [^PParser p]
  (.dataIntoMap p))


(defn- parser-data-into-map
  ([col-idx col-name parser-dtype parse-fn missing-indexes failed-indexes failed-values]
   (cond-> {:column-idx col-idx
            :column-name col-name
            :parser-dtype parser-dtype
            :parse-fn parse-fn
            :missing-indexes missing-indexes}
     failed-indexes (merge {:failed-indexes failed-indexes
                            :failed-values failed-values})))
  ([col-idx col-name parser-dtype parse-fn missing-indexes]
   (parser-data-into-map col-idx col-name parser-dtype parse-fn missing-indexes nil nil)))


(defn- parser-description->dtype-fn-tuple [parser-entry]
  (let [[dtype parse-fn] (if (vector? parser-entry)
                           parser-entry
                           [parser-entry])
        formattable-dt-dtypes (cljset/difference dt/datetime-datatypes #{:instant :packed-instant})
        datetime-formatter-parse-fn (fn [dtype formatter]
                                      (utils/make-safe-parse-fn
                                       (dt/datetime-formatter-parse-fn dtype formatter)))]
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


(deftype FixedTypeParser [col-idx col-name parser-dtype parse-fn
                          ^RoaringBitmap missing-indexes
                          ^RoaringBitmap failed-indexes
                          ^IMutList failed-values]
  PParser
  (parseValue [_this idx value]
    (cond
      (utils/missing-value? value)
      (.add missing-indexes (unchecked-int idx))
      (or (string? value)
          (not (identical? (utils/fast-dtype value) parser-dtype)))
      (when (identical? parse-failure (parse-fn value))
        (do (.add failed-indexes (unchecked-int idx))
            (.add failed-values value)))))
  (dataIntoMap [_this]
    (parser-data-into-map col-idx col-name parser-dtype parse-fn
                          missing-indexes failed-values failed-indexes)))


(defn fixed-type-parser
  ^PParser [col-idx col-name parser-descriptor]
  (let [[dtype parse-fn]    (parser-description->dtype-fn-tuple parser-descriptor)
        failed-values       (dtype/make-container :list :object 0)
        failed-indexes      (bitmap/->bitmap)
        missing-indexes     (bitmap/->bitmap)]
    (FixedTypeParser. col-idx col-name dtype parse-fn missing-indexes failed-values failed-indexes)))


(def default-parser-datatype-sequence
  [:boolean :int16 :int32 :int64 :float64 :uuid
   :zoned-date-time :local-date-time :local-date :string])


;; Gets next parser function that can be applied to value without resulting in a failure,
;; and its associated datatype
(defn- find-next-parser
  [value parser-dtype ^List promotion-list]
  (let [start-idx (.indexOf ^List (mapv first promotion-list) parser-dtype)
        n-elems (.size promotion-list)]
    (if (== start-idx -1)
      [:object nil]
      (loop [idx (inc start-idx)]
        (if (< idx n-elems)
          (let [[parser-dtype parse-fn] (.get promotion-list idx)
                parsed-value (parse-fn value)]
            (if (= parsed-value parse-failure)
              (recur (inc idx))
              [parser-dtype parse-fn]))
          [:object nil])))))


(deftype PromotionalStringParser [col-idx col-name
                                  ^{:unsynchronized-mutable true} parser-dtype
                                  ^{:unsynchronized-mutable true} parse-fn
                                  ^RoaringBitmap missing-indexes
                                  ^List promotion-list]
  PParser
  (parseValue [_this idx value]
    (cond
      (utils/missing-value? value)
      (.add missing-indexes (unchecked-int idx))
      (and (not (identical? (utils/fast-dtype value) parser-dtype))
           parse-fn)
      (let [parsed-value (parse-fn value)]
        (when (identical? parse-failure parsed-value)
          (let [[new-dtype new-parse-fn] (find-next-parser value parser-dtype promotion-list)]
            (if new-parse-fn
              (do
                (set! parser-dtype new-dtype)
                (set! parse-fn new-parse-fn))
              (throw (IllegalArgumentException.
                      (format "Unable to parse value %s in row %s of column %s" value idx col-name)))))))
      (nil? parse-fn)
      (throw (IllegalArgumentException.
              (format "`nil` parse function not allowed in promotional parser but found in column %s"
                      col-name)))))
  (dataIntoMap [_this]
    (parser-data-into-map col-idx col-name parser-dtype parse-fn missing-indexes)))


(defn promotional-string-parser
  (^PParser [col-idx col-name parser-datatype-sequence]
   (let [first-dtype (first parser-datatype-sequence)]
     (PromotionalStringParser. col-idx
                               col-name
                               first-dtype
                               (default-coercers first-dtype)
                               (bitmap/->bitmap)
                               (mapv (juxt identity default-coercers) parser-datatype-sequence))))
  (^PParser [col-idx col-name]
   (promotional-string-parser col-idx col-name default-parser-datatype-sequence)))


(defn- trim-nils [^PersistentVector parsers]
  (let [last-idx (dec (.length parsers))]
    (or (reduce (fn [_ i]
                  (when (some? (nth parsers i))
                    (reduced (subvec parsers 0 (inc i)))))
                nil
                (into [] (range last-idx -1 -1)))
        [])))


(deftype VectorParser [col-idx col-name
                       ^ObjectArrayList parsers
                       col-idx->parser
                       ^:unsynchronized-mutable ^int min-length
                       ^:unsynchronized-mutable ^int max-length
                       ^RoaringBitmap missing-indexes]
  PParser
  (parseValue [_this idx value]
    (let [vector-length (.length ^PersistentVector value)]
      (when (< vector-length min-length)
        (set! min-length vector-length))
      (when (> vector-length max-length)
        (set! max-length vector-length))
      (doseq [i (range vector-length)]
        (-> (col-idx->parser i)
            (parse-value! idx (nth value i))))))
  (dataIntoMap [_this]
    {:column-idx col-idx
     :column-name col-name
     :parser-dtype :vector
     :min-length min-length
     :max-length max-length
     :field-parser-data (trim-nils (mapv #(when %
                                            (dissoc (data-into-map %)
                                                    :column-name))
                                         parsers))}))


(defn- options->row-iter [input options]
  (let [row-iter (->> (charred/read-csv-supplier (ds-io/input-stream-or-reader input) options)
                      (coerce/->iterator)
                      pfor/->iterator)]
    (dotimes [idx (long (get options :n-initial-skip-rows 0))]
      (when (.hasNext row-iter) (.next row-iter)))
    row-iter))


(defn- iter->header-row [^Iterator row-iter]
  (when (.hasNext row-iter)
    (vec (.next row-iter))))


(defn- options->num-rows [options]
  (long (get options :batch-size
             (get options :n-records
                  (get options :num-rows Long/MAX_VALUE)))))


(defn- make-colname
  ([col-idx col-idx->colname]
   (let [colname (when col-idx->colname
                   (col-idx->colname col-idx))]
     (if (empty? colname)
       (str "column-" col-idx)
       colname)))
  ([col-idx]
   (make-colname col-idx nil)))


(defn- options->parser-fn
  "Given the (user-specified) options map for parsing, create the specific parse context, i.e.
  a function that produces a column parser for a given column name or index; applicable only to
  scalar columns. `:parser-type` can be `nil`, for example if only columns with specified parser
  functions or datatypes should be parsed."
  ([options parser-type col-idx->colname]
   (let [default-parse-fn (case (get options :parser-type parser-type)
                            :string promotional-string-parser
                            nil (constantly nil))
         parser-descriptor (:parser-fn options)]
     (fn [col-idx]
       (let [colname (when col-idx->colname (col-idx->colname col-idx))
             colname (keyword (if (empty? colname)
                                (make-colname col-idx)
                                colname))]
         (cond
           (nil? parser-descriptor)
           (default-parse-fn col-idx colname)
           (map? parser-descriptor)
           (if-let [col-parser-desc (or (get parser-descriptor colname)
                                        (get parser-descriptor col-idx))]
             (fixed-type-parser col-idx colname col-parser-desc)
             (default-parse-fn col-idx colname))
           :else
           (fixed-type-parser col-idx colname parser-descriptor))))))
  ([options parser-type]
   (options->parser-fn options parser-type nil)))


(defn- options->col-idx-parse-context
  "Given an option map, a parse type, and a function mapping column index to column name,
  return a map of parsers and a function to get a parser from a given column idx. Returns:
  {:parsers - parsers
   :col-idx->parser - given a column idx, get a parser.  Mutates parsers."
  ([options parser-type col-idx->colname]
   (let [make-parser-fn (options->parser-fn options parser-type col-idx->colname)
         parsers (ObjectArrayList. (object-array 16))
         ; TODO benchmark against `reify Function`
         colparser-compute-fn (fn [col-idx]
                                (make-parser-fn (long col-idx)))
         col-idx->parser (fn [col-idx]
                           (let [col-idx (long col-idx)]
                             (if-let [parser (.readObject parsers col-idx)]
                               parser
                               (let [parser (colparser-compute-fn col-idx)]
                                 (.writeObject parsers col-idx parser)
                                 parser))))]
     {:parsers parsers
      :col-idx->parser col-idx->parser}))
  ([options parser-type] (options->col-idx-parse-context options parser-type nil)))


(defn- iter->parsers [header-row ^Iterator row-iter num-rows options]
  (let [{:keys [parsers col-idx->parser]}
        (options->col-idx-parse-context
         options :string (fn [^long col-idx]
                           (get header-row col-idx)))]
    (reduce (hamf/indexed-accum
             acc row-idx row
             (reduce (hamf/indexed-accum
                      acc col-idx field
                      (-> (col-idx->parser col-idx)
                          (parse-value! row-idx field)))
                     nil
                     row))
            nil
            (TakeReducer. row-iter num-rows))
    parsers))


(defn- options->parsers [input {:keys [header-row?]
                                :or {header-row? true}
                                :as options}]
  (let [row-iter ^Iterator (options->row-iter input options)
        header-row (when header-row?
                     (iter->header-row row-iter))
        num-rows (options->num-rows options)]
    (when (.hasNext row-iter)
      (iter->parsers header-row row-iter num-rows options))))


(defn vector-parser [col-idx col-name options]
  (let [{:keys [parsers col-idx->parser]}
        (options->col-idx-parse-context options :string)]
    (VectorParser. col-idx col-name parsers col-idx->parser Integer/MAX_VALUE 0 (bitmap/->bitmap))))


(defn- col-vector-parse-context [parsers options]
  (let [parser-data (mapv #(when % (data-into-map %)) parsers)
        vector-parsers (-> (fn [parser {:keys [column-idx column-name parser-dtype]}]
                             (when (and (instance? PromotionalStringParser parser)
                                        (identical? parser-dtype :string))
                               (vector-parser column-idx column-name options)))
                           (mapv parsers parser-data)
                           object-array
                           ObjectArrayList.)
        missing-indexes (mapv :missing-indexes parser-data)
        row-missing-in-col? (fn [row-idx col-idx]
                              (.contains
                               ^RoaringBitmap (nth missing-indexes col-idx)
                               (unchecked-int row-idx)))]
    {:vector-parsers vector-parsers
     :row-missing-in-col? row-missing-in-col?}))


(defn- iter->vector-parsers [parsers ^Iterator row-iter num-rows options]
  (let [vector-open (get options :vector-open \[)
        vector-close (get options :vector-close \])
        vector-opts (if-some [vs (get options :vector-separator)]
                      (assoc options :separator vs)
                      options)
        {:keys [^ObjectArrayList vector-parsers row-missing-in-col?]}
        (col-vector-parse-context parsers options)]
    (reduce (hamf/indexed-accum
             acc row-idx row
             (reduce (hamf/indexed-accum
                      acc col-idx field
                      (if-some [parser (.readObject vector-parsers col-idx)]
                        (when (not (row-missing-in-col? row-idx col-idx))
                          (let [len (.length ^String field)]
                            (if (and (> len 0)
                                     (identical? (nth field 0) vector-open)
                                     (-> (nth field (dec len))
                                         (identical? vector-close)))
                              (parse-value! parser
                                            row-idx
                                            (-> (subs field 1 (dec len))
                                                (charred/read-csv vector-opts)
                                                (nth 0)))
                              (.writeObject vector-parsers col-idx nil))))))
                     nil
                     row))
            nil
            (TakeReducer. row-iter num-rows))
    vector-parsers))


(defn- options->vector-parsers [parsers input {:keys [header-row?]
                                               :or {header-row? true}
                                               :as options}]
  (let [row-iter ^Iterator (options->row-iter input options)
        _ (when header-row?
            (iter->header-row row-iter))
        num-rows (options->num-rows options)]
    (when (.hasNext row-iter)
      (iter->vector-parsers parsers row-iter num-rows options))))


(defn csv->parsers
  ([input options]
   (let [options (update options :batch-size #(or % 128000))
         parsers (options->parsers input options)]
     (->> (options->vector-parsers parsers input options)
          (mapv (fn [p vp]
                  (when-some [parser (or vp p)]
                    (data-into-map parser)))
                parsers)
          trim-nils)))
  ([input]
   (csv->parsers input {})))


(comment
  (require '[clojure.java.io :as io]
           '[clojure.string :as str]
           '[charred.api :as charred]
           '[tech.v3.dataset.io.csv :as csv]
           '[tech.v3.dataset.io.datetime :as parse-dt])

  (with-open [reader (io/reader "test/data/array-test.csv")]
    (doall
     (charred/read-csv reader)))

  (def gotta-catch-em-all (csv->parsers "test/data/pokemon.csv"))
  gotta-catch-em-all
  (type gotta-catch-em-all)
  (count gotta-catch-em-all)
  (nth gotta-catch-em-all 23)

  (def dial-311 (csv->parsers "test/data/311_service_requests_2010-present_sample.csv"))
  dial-311
  (keyword (:column-name (nth dial-311 2)))
  (count dial-311)
  (subvec dial-311 32)

  (def dial-311-full (csv->parsers "test/data/311_service_requests_2010-present_sample.csv" {:batch-size Long/MAX_VALUE}))
  dial-311-full
  (def dial-311-techio (csv/csv->dataset "test/data/311_service_requests_2010-present_sample.csv"))

  ; TODO handle spaces in col names
  (str (keyword "Location Type"))
  (def dial-311-custom-vec-parse
    (let [options {;:batch-size Long/MAX_VALUE
                   :parser-fn {40 [:vector
                                   (fn [v]
                                     (let [len (.length ^String v)
                                           re (re-pattern ", ")]
                                       (mapv (get default-coercers :float32)
                                             (-> (subs v 1 (dec len))
                                                 (str/split re)))))]}}]
      (csv->parsers "test/data/311_service_requests_2010-present_sample.csv" options)))
  dial-311-custom-vec-parse
  (subvec dial-311-custom-vec-parse 32)

  (import '[java.time LocalDate LocalDateTime LocalTime]
          '[java.time.format DateTimeFormatter DateTimeFormatterBuilder])

  (parse-dt/parse-local-date-time "09/02/2020 04:14:43 PM")
  (LocalTime/parse "01:02:03 AM" (DateTimeFormatter/ofPattern "hh:mm:ss a" Locale/ENGLISH))
  (LocalDate/parse "09022020" (DateTimeFormatter/ofPattern "ddMMyyyy" Locale/ENGLISH))
  (dt/parse-local-date-time "09/02/2020 04:14:43 PM")

  (ObjectArrayList. (object-array 16))
  )
