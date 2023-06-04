(ns ^:no-doc tablehike.utils
  (:require [charred.api :as charred]
            [charred.coerce :as coerce]
            [tech.v3.dataset.io :as ds-io]
            [tech.v3.parallel.for :as pfor])
  (:import [java.util Iterator]))


(def schema-inference-batch-size 10000)


(defn rm-empty-elements [coll init init-transient?]
  (reduce-kv (fn [m k v]
               (if (if (seqable? v) (seq v) (some? v))
                 ((if init-transient? assoc! assoc) m k v)
                 m))
             init
             coll))


(defn merge-tuple-cols [tuple-map init init-transient?]
  (reduce (fn [row tname]
            (let [tuple-cols (tname tuple-map)
                  tval (mapv row tuple-cols)
                  assoc-fn (if init-transient? assoc! assoc)
                  dissoc-fn (if init-transient? dissoc! dissoc)]
              (if (some? (reduce #(or %1 %2) tval))
                (reduce (fn [m t] (dissoc-fn m t))
                        (assoc-fn row tname tval)
                        tuple-cols)
                row)))
          init
          (keys tuple-map)))


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


(defn csv->header-skipped-iter [input options]
  (let [row-iter ^Iterator (csv->row-iter input options)
        _ (row-iter->header-row row-iter options)]
    row-iter))
