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
  (let [double-parser (utils/make-safe-parse-fn #(if (string? %)
                                                   (let [dval (Double/parseDouble %)]
                                                     (if (Double/isNaN dval)
                                                       missing
                                                       dval))
                                                   (double %)))]
    (into dt/datatype->general-parse-fn-map
          #:db.type{:instant (-> (dt/datetime->date-parse-fn dt/instant-parse-fn)
                                 utils/make-safe-parse-fn)
                    :boolean #(if (string? %)
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
                    :bigint (utils/make-safe-parse-fn #(if (string? %)
                                                         (Integer/parseInt %)
                                                         (int %)))
                    :long (utils/make-safe-parse-fn #(if (string? %)
                                                       (Long/parseLong %)
                                                       (long %)))
                    :float (utils/make-safe-parse-fn
                            #(if (string? %)
                               (let [fval (Float/parseFloat %)]
                                 (if (Float/isNaN fval)
                                   missing
                                   fval))
                               (float %)))
                    :double double-parser
                    :number double-parser
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
                               (str %))})))


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


(defn parse-value! [^PParser p ^long idx value]
  (.parseValue p idx value))


(defn data-into-map [^PParser p]
  (.dataIntoMap p))


(defn tech-v3->datahike-dtypes [dt]
  (cond
    (identical? :float32 dt) :db.type/float
    (identical? :float64 dt) :db.type/double
    (contains? #{:int16 :int32 :int64} dt) :db.type/long
    (identical? :string dt) :db.type/string
    (identical? :bool dt) :db.type/boolean
    ; Could be in the `:else`, but datetimes seem common enough to warrant this spot
    (or (contains? dt/datetime-datatypes dt)
        (identical? :db.type/instant dt)) dt
    (contains? #{:uuid :keyword :symbol} dt) (keyword "db.type" (name dt))
    (identical? :big-integer dt) :db.type/bigint
    (identical? :decimal dt) :db.type/bigdec
    ; Refs and anything else supported but somehow not caught above.
    ; Where applicable, error can be thrown downstream on parse failure.
    :else dt))


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


(defn- dtype-parser-fn-tuple [parser-descriptor]
  (let [[dtype parse-fn] (if (vector? parser-descriptor)
                           parser-descriptor
                           [parser-descriptor])]
    (when-not ((clj-set/union (set (keys default-coercers))
                              #{:db.type/bigdec :db.type/ref :db.type/tuple})
               dtype)
      (throw (IllegalArgumentException.
              (format "Unrecognized data type: %s" dtype))))
    [dtype
     (if (instance? IFn parse-fn)
       (utils/make-safe-parse-fn parse-fn)
       (if-let [ret-fn (get default-coercers dtype)]
         ret-fn
         (->> (format "Default coercer unavailable for data type %s" dtype)
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
          (-> (tech-v3->datahike-dtypes (utils/fast-dtype value))
              (identical? parser-dtype)
              not))
      (when (identical? parse-failure (parse-fn value))
        (do (.add failed-indexes (unchecked-int idx))
            (.add failed-values value)))))
  (dataIntoMap [_this]
    (parser-data-into-map col-idx col-name parser-dtype parse-fn
                          missing-indexes failed-indexes failed-values)))


(defn fixed-type-parser
  ^PParser [col-idx col-name parser-descriptor]
  (let [[dtype parse-fn]    (dtype-parser-fn-tuple parser-descriptor)
        missing-indexes     (bitmap/->bitmap)
        failed-indexes      (bitmap/->bitmap)
        failed-values       (dtype/make-container :list :object 0)]
    (FixedTypeParser. col-idx col-name dtype parse-fn missing-indexes failed-indexes failed-values)))


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
                        tech-v3->datahike-dtypes
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
                             (mapv (juxt identity default-coercers)
                                   parser-datatype-sequence)))
  (^PParser [col-idx col-name]
   (let [default-parser-dtype-sequence
         (-> (into [:db.type/boolean :db.type/long :db.type/double :db.type/uuid]
                   dt/datetime-datatypes)
             (conj :db.type/string))]
     (promotional-string-parser col-idx col-name default-parser-dtype-sequence))))


(defn- trim-nils [^PersistentVector parsers]
  (let [last-idx (dec (.length parsers))]
    (or (reduce (fn [_ i]
                  (when (some? (nth parsers i))
                    (reduced (subvec parsers 0 (inc i)))))
                nil
                (into [] (range last-idx -1 -1)))
        [])))


(deftype VectorParser [col-idx
                       col-name
                       ^ObjectArrayList parsers
                       vector-str->elmt-strs
                       col-idx->parser
                       ^:unsynchronized-mutable ^int min-length
                       ^:unsynchronized-mutable ^int max-length
                       ^RoaringBitmap missing-indexes
                       ^RoaringBitmap failed-indexes
                       ^IMutList failed-values]
  PParser
  (parseValue [_this idx value]
    (if (csv-read/missing-value? value)
      (.add missing-indexes (unchecked-int idx))
      (let [elmts (if (string? value)
                    (vector-str->elmt-strs value)
                    (if (vector? value)
                      value
                      parse-failure))]
        (if (identical? elmts parse-failure)
          (if failed-indexes
            (do (.add failed-indexes (unchecked-int idx))
                (.add failed-values value))
            parse-failure)
          (let [vector-length (.length ^PersistentVector elmts)]
            (when (< vector-length min-length)
              (set! min-length vector-length))
            (when (> vector-length max-length)
              (set! max-length vector-length))
            (doseq [i (range vector-length)]
              (-> (col-idx->parser i)
                  (parse-value! idx (nth elmts i)))))))))
  (dataIntoMap [_this]
    (-> (parser-data-into-map col-idx col-name :vector nil missing-indexes failed-indexes failed-values)
        (merge {:min-length min-length
                :max-length max-length
                :field-parser-data (trim-nils (mapv #(when %
                                                       (dissoc (data-into-map %) :column-name))
                                                    parsers))}))))


(defn- vector-elmt-idx->name [idx]
  (str "element-" idx))


(defn- parser-array-list
  ([data] (-> (object-array (or data 16))
              ObjectArrayList.))
  ([] (parser-array-list nil)))


(defn- col-idx->parser-fn
  ([^ObjectArrayList parsers make-parser-fn]
   (fn [col-idx]
     (let [col-idx (long col-idx)
           parser (.readObject parsers col-idx)]
       (if (or parser (nil? make-parser-fn))
         parser
         (when-let [parser (make-parser-fn col-idx)]
           (.writeObject parsers col-idx parser)
           parser)))))
  ([^ObjectArrayList parsers]
   (col-idx->parser-fn parsers nil)))


(defn vector-parser
  ([col-idx col-name vector-str->elmt-strs strict parsers-init]
   (let [parsers (or parsers-init (parser-array-list))
         make-parser-fn (when (nil? parsers-init)
                          (fn [col-idx]
                            (->> (vector-elmt-idx->name col-idx)
                                 (promotional-string-parser col-idx))))
         col-idx->parser (col-idx->parser-fn parsers make-parser-fn)]
     (VectorParser. col-idx
                    col-name
                    parsers
                    vector-str->elmt-strs
                    col-idx->parser
                    Integer/MAX_VALUE
                    0
                    (bitmap/->bitmap)
                    (if strict nil (bitmap/->bitmap))
                    (if strict
                      nil
                      (dtype/make-container :list :object 0)))))
  ([col-idx col-name vector-str->elmt-strs strict]
   (vector-parser col-idx col-name vector-str->elmt-strs strict nil)))


(defn- fixed-types-vector-parser [col-idx col-name parser-spec options]
  (let [dtypes (get parser-spec 0)
        dtypes-count (count dtypes)
        parse-fns (get parser-spec 1)
        vector-str->elmt-strs (utils/vector-str->elmt-strs-fn options)]
    (when (= dtypes-count 0)
      (throw (IllegalArgumentException.
              (str "At least 1 data type must be specified for vector parser, "
                   (format "but 0 given for column %s (%s)" col-idx col-name)))))
    (when (and (some? parse-fns)
               (not= dtypes-count (count parse-fns)))
      (->> (format "in vector parser description for column %s (%s)" col-idx col-name)
           (str "Number of data types and parse functions specified unequal ")
           IllegalArgumentException.
           throw))
    (->> (map #(fixed-type-parser %
                                  (vector-elmt-idx->name %)
                                  [(nth dtypes %)
                                   (when (some? parse-fns)
                                     (nth parse-fns %))])
              (range dtypes-count))
         parser-array-list
         (vector-parser col-idx col-name vector-str->elmt-strs false))))


;; TODO move?
(defn col-idx->col-name-fn [options]
  (fn [idx]
    (let [default #(str "column-" %)]
      (if-some [idx->colname (:idx->colname options)]
        (if-some [colname (idx->colname idx)]
          (if (> (.length ^String colname) 0)
            colname
            (default idx))
          (default idx))
        (default idx)))))


;; TODO move?
(defn colname->ident-fn [options]
  (or (:colname->ident options)
      #(-> (string/replace % #"\s+" "-")
           keyword)))


;; Create a function that produces a parser for a given column index
(defn- create-col-parser-fn [header-row parsers-spec options]
  (let [include-cols (or (:include-cols options)
                         (constantly true))
        idx->colname-default (col-idx->col-name-fn options)
        idx->colname #(if (and (some? header-row)
                               (< % (.length ^PersistentVector header-row)))
                        (let [colname (nth header-row %)]
                          (if (> (.length ^String colname) 0)
                            colname
                            (idx->colname-default %)))
                        (idx->colname-default %))
        colname->ident (colname->ident-fn options)
        cascading-get (fn [f col-idx col-name]
                        (or (f col-idx)
                            (or (f col-name)
                                (f (colname->ident col-name)))))]
    (fn [col-idx]
      (let [col-name (idx->colname col-idx)]
        (when (cascading-get include-cols col-idx col-name)
          (if-some [parser-spec (cascading-get parsers-spec col-idx col-name)]
            (if (and (vector? parser-spec)
                     (vector? (nth parser-spec 0)))
              (fixed-types-vector-parser col-idx col-name parser-spec options)
              (fixed-type-parser col-idx col-name parser-spec))
            (promotional-string-parser col-idx col-name)))))))


(defn- iter->parsers
  ^ObjectArrayList [header-row ^Iterator row-iter parsers-spec options]
  (let [parsers (parser-array-list)
        col-idx->parser (->> (create-col-parser-fn header-row parsers-spec options)
                             (col-idx->parser-fn parsers))]
    ; TODO Does reduce-kv work instead? If yes, is it comparable in performance?
    (reduce (hamf-rf/indexed-accum
             acc row-idx row
             (reduce (hamf-rf/indexed-accum
                      acc col-idx field
                      (some-> (col-idx->parser col-idx)
                              (parse-value! row-idx field)))
                     nil
                     row))
            nil
            (TakeReducer. row-iter (:parser-sample-size options)))
    parsers))


(defn- csv->parsers [input parsers-spec options]
  (let [row-iter ^Iterator (csv-read/csv->row-iter input options)
        header-row (csv-read/row-iter->header-row row-iter options)]
    (when (.hasNext row-iter)
      (iter->parsers header-row row-iter parsers-spec options))))


(defn- iter->vector-parsers [parsers ^Iterator row-iter options]
  (let [vector-str->elmt-strs (utils/vector-str->elmt-strs-fn options)
        parser-data (mapv #(when % (data-into-map %)) parsers)
        vector-parsers (-> (fn [parser {:keys [column-idx column-name parser-dtype]}]
                             (when (and (instance? PromotionalStringParser parser)
                                        (identical? parser-dtype :db.type/string))
                               (vector-parser column-idx column-name vector-str->elmt-strs true)))
                           (mapv parsers parser-data)
                           object-array
                           ObjectArrayList.)
        missing-indexes (mapv :missing-indexes parser-data)
        row-missing-in-col? (fn [row-idx col-idx]
                              (.contains
                               ^RoaringBitmap (nth missing-indexes col-idx)
                               (unchecked-int row-idx)))]
    (reduce (hamf-rf/indexed-accum
             acc row-idx row
             (reduce (hamf-rf/indexed-accum
                      acc col-idx field
                      (when-some [parser (.readObject vector-parsers col-idx)]
                        (when (not (row-missing-in-col? row-idx col-idx))
                          (when (identical? (parse-value! parser row-idx field)
                                            parse-failure)
                            (.writeObject vector-parsers col-idx nil)))))
                      nil
                      row))
            nil
            (TakeReducer. row-iter (:parser-sample-size options)))
    vector-parsers))


(defn- csv->vector-parsers [parsers input options]
  (let [row-iter (csv-read/csv->header-skipped-row-iter input options)]
    (when (.hasNext row-iter)
      (iter->vector-parsers parsers row-iter options))))


(defn infer-parsers
  ([input parsers-spec options]
   (let [options (update options :parser-sample-size #(or % 12800))
         parsers (csv->parsers input parsers-spec options)]
     (->> (csv->vector-parsers parsers input options)
          (mapv (fn [p vp]
                  (when-some [parser (or vp p)]
                    (data-into-map parser)))
                parsers)
          (into [] (filter some?)))))
  ([input parsers-spec]
   (infer-parsers input parsers-spec {}))
  ([input]
   (infer-parsers input {} {})))
