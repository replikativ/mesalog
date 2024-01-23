(ns tablehike.parse.parser
  (:require [clojure.set :as clj-set]
            [clojure.string :as string]
            [tablehike.parse.datetime :as dt]
            [tablehike.parse.utils :refer [parse-failure] :as utils]
            [tablehike.read :as csv-read]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.bitmap :as bitmap]
            [ham-fisted.reduce :as hamf-rf])
  (:import [clojure.lang IFn PersistentVector]
           [java.util Iterator List UUID]
           [ham_fisted IMutList]
           [org.roaringbitmap RoaringBitmap]
           [tablehike.read TakeReducer]
           [tech.v3.datatype ArrayHelpers ObjectBuffer]))


(def missing :tablehike/missing)


(def default-coercers
  (into dt/datatype->general-parse-fn-map
        {:boolean #(if (string? %)
                     (let [^String data %]
                       (cond
                         (or (.equalsIgnoreCase "t" data)
                             (.equalsIgnoreCase "y" data)
                             (.equalsIgnoreCase "yes" data)
                             (.equalsIgnoreCase "True" data)
                             (.equalsIgnoreCase "positive" data))
                         true
                         (or (.equalsIgnoreCase "f" data)
                             (.equalsIgnoreCase "n" data)
                             (.equalsIgnoreCase "no" data)
                             (.equalsIgnoreCase "false" data)
                             (.equalsIgnoreCase "negative" data))
                         false
                         :else
                         parse-failure))
                     (boolean %))
         :int64 (utils/make-safe-parse-fn #(if (string? %)
                                             (Long/parseLong %)
                                             (long %)))
         :float32 (utils/make-safe-parse-fn #(if (string? %)
                                               (let [fval (Float/parseFloat %)]
                                                 (if (Float/isNaN fval)
                                                   missing
                                                   fval))
                                               (float %)))
         :float64 (utils/make-safe-parse-fn #(if (string? %)
                                               (let [dval (Double/parseDouble %)]
                                                 (if (Double/isNaN dval)
                                                   missing
                                                   dval))
                                               (double %)))
         :uuid (utils/make-safe-parse-fn #(if (string? %)
                                            (UUID/fromString %)
                                            (if (instance? UUID %)
                                              %
                                              parse-failure)))
         :keyword #(if-let [retval (keyword %)]
                     retval
                     parse-failure)
         :symbol #(if-let [retval (symbol %)]
                    retval
                    parse-failure)
         :string #(if (string? %)
                    %
                    (str %))}))


(deftype ObjectArrayList [^{:unsynchronized-mutable true
                            :tag 'objects} data]
  ObjectBuffer
  (lsize [_this] (alength ^objects data))
  (writeObject [_this idx value]
    (when (>= idx (alength ^objects data))
      (let [old-len (alength ^objects data)
            new-len (* 2 idx)
            new-data (object-array new-len)]
        (System/arraycopy data 0 new-data 0 old-len)
        (set! data new-data)))
    (ArrayHelpers/aset ^objects data idx value))
  (readObject [_this idx]
    (when (< idx (alength ^objects data))
      (aget ^objects data idx))))


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
        formattable-dt-dtypes (disj dt/datetime-datatypes :instant)]
    (assert (keyword? dtype))
    [dtype
     (cond
       (instance? IFn parse-fn)
       (utils/make-safe-parse-fn parse-fn)
       (and (formattable-dt-dtypes dtype)
            (string? parse-fn))
       (dt/datetime-formatter-parse-fn dtype (DateTimeFormatter/ofPattern parse-fn))
       (and (formattable-dt-dtypes dtype)
            (instance? DateTimeFormatter parse-fn))
       (dt/datetime-formatter-parse-fn dtype parse-fn)
       :else
       (if-let [ret-fn (get default-coercers dtype)]
         ret-fn
         (-> (format "Unrecognized data and parse function type: %s and %s" dtype parse-fn)
             IllegalArgumentException.
             throw)))]))


(deftype FixedTypeParser [col-idx col-name parser-dtype parse-fn
                          ^RoaringBitmap missing-indexes
                          ^RoaringBitmap failed-indexes
                          ^IMutList failed-values]
  PParser
  (parseValue [_this idx value]
    (cond
      (csv-read/missing-value? value)
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
  [:boolean :int64 :float64 :uuid
   :zoned-date-time :local-date-time :local-date :string])


;; Gets next parser function that can be applied to value without resulting in a failure,
;; and its associated datatype
(defn- find-next-parser
  [value parser-dtype ^List promotion-list]
  (let [start-idx (.indexOf ^List (mapv first promotion-list) parser-dtype)
        n-elems (.size promotion-list)]
    (if (and (== start-idx -1)
             (some? parser-dtype))
      [:object nil]
      (loop [idx (inc start-idx)]
        (if (< idx n-elems)
          (let [[parser-dtype parse-fn] (.get promotion-list idx)
                parsed-value (parse-fn value)]
            (if (= parsed-value parse-failure)
              (recur (inc idx))
              [parser-dtype parse-fn]))
          [:object nil])))))


(deftype PromotionalStringParser [col-idx
                                  col-name
                                  ^{:unsynchronized-mutable true} parser-dtype
                                  ^{:unsynchronized-mutable true} parse-fn
                                  ^RoaringBitmap missing-indexes
                                  ^List promotion-list]
  PParser
  (parseValue [_this idx value]
    (if (csv-read/missing-value? value)
      (.add missing-indexes (unchecked-int idx))
      (do
        (when (nil? parser-dtype)
          (let [[first-dtype first-parse-fn] (first promotion-list)]
            (do (set! parser-dtype first-dtype)
                (set! parse-fn first-parse-fn))))
        (cond
          (and (not (-> (utils/fast-dtype value)
                        (identical? parser-dtype)))
               parse-fn)
          (when (identical? parse-failure
                            (parse-fn value))
            (let [[new-dtype new-parse-fn]
                  (find-next-parser value parser-dtype promotion-list)]
              (if new-parse-fn
                (do (set! parser-dtype new-dtype)
                    (set! parse-fn new-parse-fn))
                (throw (IllegalArgumentException.
                        (format "Unable to parse value %s in row %s of column %s"
                                value idx col-name))))))
          (nil? parse-fn)
          (-> "`nil` parse function not allowed in promotional parser but found in column %s"
              (format col-name)
              IllegalArgumentException.
              throw)))))
  (dataIntoMap [_this]
    (parser-data-into-map col-idx col-name parser-dtype parse-fn missing-indexes)))


(defn promotional-string-parser
  (^PParser [col-idx col-name parser-datatype-sequence]
   (PromotionalStringParser. col-idx
                             col-name
                             nil
                             nil
                             (bitmap/->bitmap)
                             (mapv (juxt identity default-coercers) parser-datatype-sequence)))
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
  ([options parser-type parser-descriptor col-idx->colname]
   (let [default-parse-fn (case (get options :parser-type parser-type)
                            :string promotional-string-parser
                            nil (constantly nil))
         parser-descriptor (or parser-descriptor (:parser-fn options))]
     (fn [col-idx]
       (let [colname (when col-idx->colname (col-idx->colname col-idx))
             colname (keyword (if (empty? colname)
                                (make-colname col-idx)
                                (string/replace colname #"\s+" "")))]
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
  ([options parser-type parser-descriptor]
   (options->parser-fn options parser-type parser-descriptor nil)))


(defn- options->col-idx-parse-context
  "Given an option map, a parse type, and a function mapping column index to column name,
  return a map of parsers and a function to get a parser from a given column idx. Returns:
  {:parsers - parsers
   :col-idx->parser - given a column idx, get a parser.  Mutates parsers."
  ([options parser-type parser-descriptor col-idx->colname]
   (let [make-parser-fn (options->parser-fn options
                                            parser-type
                                            parser-descriptor
                                            col-idx->colname)
         parsers (ObjectArrayList. (object-array 16))
         col-idx->parser (fn [col-idx]
                           (let [col-idx (long col-idx)]
                             (if-let [parser (.readObject parsers col-idx)]
                               parser
                               (let [parser (make-parser-fn (long col-idx))]
                                 (.writeObject parsers col-idx parser)
                                 parser))))]
     {:parsers parsers
      :col-idx->parser col-idx->parser}))
  ([options parser-type parser-descriptor]
   (options->col-idx-parse-context options parser-type parser-descriptor nil)))


(defn- iter->parsers
  ^ObjectArrayList [header-row ^Iterator row-iter num-rows options]
  (let [{:keys [parsers col-idx->parser]}
        (options->col-idx-parse-context
         options :string nil (fn [^long col-idx]
                               (get header-row col-idx)))]
    ; TODO Does reduce-kv work instead? If yes, is it comparable in performance?
    (reduce (hamf-rf/indexed-accum
             acc row-idx row
             (reduce (hamf-rf/indexed-accum
                      acc col-idx field
                      (-> (col-idx->parser col-idx)
                          (parse-value! row-idx field)))
                     nil
                     row))
            nil
            (TakeReducer. row-iter num-rows))
    parsers))


(defn- options->parsers [input options]
  (let [row-iter ^Iterator (csv-read/csv->row-iter input options)
        header-row (csv-read/row-iter->header-row row-iter options)]
    (when (.hasNext row-iter)
      (iter->parsers header-row row-iter (:parser-sample-size options) options))))


(defn vector-parser [col-idx col-name options]
  (let [parser-opts (get :parser-fn options)
        {:keys [parsers col-idx->parser]}
        (->> (or (get parser-opts col-name)
                 (get parser-opts col-idx))
             (options->col-idx-parse-context options :string))]
    (->> (bitmap/->bitmap)
         (VectorParser. col-idx col-name parsers col-idx->parser Integer/MAX_VALUE 0))))


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
        {:keys [^ObjectArrayList vector-parsers row-missing-in-col?]}
        (col-vector-parse-context parsers options)
        string->vector (csv-read/string->vector-parser options)]
    (reduce (hamf-rf/indexed-accum
             acc row-idx row
             (reduce (hamf-rf/indexed-accum
                      acc col-idx field
                      (if-some [parser (.readObject vector-parsers col-idx)]
                        (when (not (row-missing-in-col? row-idx col-idx))
                          (let [len (.length ^String field)]
                            (if (and (> len 0)
                                     (identical? (nth field 0) vector-open)
                                     (-> (nth field (dec len))
                                         (identical? vector-close)))
                              (->> (string->vector field)
                                   (parse-value! parser row-idx))
                              (.writeObject vector-parsers col-idx nil))))))
                     nil
                     row))
            nil
            (TakeReducer. row-iter num-rows))
    vector-parsers))


(defn- options->vector-parsers [parsers input options]
  (let [row-iter (csv-read/csv->header-skipped-row-iter input options)]
    (when (.hasNext row-iter)
      (iter->vector-parsers parsers row-iter (:parser-sample-size options) options))))


(defn csv->parsers
  ([input options]
   (let [options (update options :parser-sample-size #(or % 12800))
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
