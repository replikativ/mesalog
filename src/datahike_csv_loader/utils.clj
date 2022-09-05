(ns ^:no-doc datahike-csv-loader.utils)

(defn rm-empty-elements [coll init init-transient?]
  (reduce-kv (fn [m k v]
               (if (if (seqable? v) (seq v) (some? v))
                 ((if init-transient? assoc! assoc) m k v)
                 m))
             init
             coll))

(defn merge-tuple-cols [tuple-names tuple-schema init init-transient?]
  (reduce (fn [row tname]
            (let [tuple-cols (tname tuple-schema)
                  tval (mapv row tuple-cols)
                  assoc-fn (if init-transient? assoc! assoc)
                  dissoc-fn (if init-transient? dissoc! dissoc)]
              (if (some? (reduce #(or %1 %2) tval))
                (reduce (fn [m t] (dissoc-fn m t))
                        (assoc-fn row tname tval)
                        tuple-cols)
                row)))
          init
          tuple-names))
