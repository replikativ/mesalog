(ns ^:no-doc tablehike.read
  (:require [charred.api :as charred]
            [charred.coerce :as coerce]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.parallel.for :as pfor])
  (:import [clojure.lang IReduceInit]
           [java.util Iterator]
           [ham_fisted Casts]))


(deftype TakeReducer [^Iterator src
                      ^{:unsynchronized-mutable true
                        :tag long} count]
  IReduceInit
  (reduce [this rfn acc]
    (let [cnt count]
      (loop [idx       0
             continue? (.hasNext src)
             acc       acc]
        (if (and continue? (< idx cnt))
          (let [acc (rfn acc (.next src))]
            (recur (unchecked-inc idx) (.hasNext src) acc))
          (do
            (set! count (- cnt idx))
            acc))))))


(defn csv->row-iter [input options]
  (->> (charred/read-csv-supplier (ds-io/input-stream-or-reader input) options)
       (coerce/->iterator)
       pfor/->iterator))


(defn row-iter->header-row [^Iterator row-iter
                            {:keys [n-initial-skip-rows header-row?]
                             :or {n-initial-skip-rows 0
                                  header-row? true}}]
  (dotimes [_ n-initial-skip-rows]
    (when (.hasNext row-iter)
      (.next row-iter)))
  (when (and header-row? (.hasNext row-iter))
    (vec (.next row-iter))))


(defn csv->header-skipped-row-iter ^Iterator [input options]
  (let [row-iter ^Iterator (csv->row-iter input options)
        _ (row-iter->header-row row-iter options)]
    row-iter))


(defn missing-value?
  "Is this a missing value coming from a CSV file"
  [value]
  (cond
    (or (instance? Double value) (instance? Float value))
    (Double/isNaN (Casts/doubleCast value))
    (not (instance? Number value))
    (or (nil? value)
        (.equals "" value)
        (identical? value :tablehike/missing)
        (and (string? value) (re-matches #"(?i)^n\/?a$" value)))))
