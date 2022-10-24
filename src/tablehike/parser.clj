(ns tablehike.parser
  (:require [datahike.api :as d]
            [clojure.java.io :as io]
            [charred.api :as ch]))


(def csv (ch/read-csv (io/file "resources/levels.csv")))

;; first row is assumed to be column titles
(def column-names (mapv keyword (first csv)))

;; rest is data
(def data (rest csv))

;; first data column
(first data)

;; parsing
(def parsers {:missing #(if-not (empty? ^String %)
                          (throw (IllegalArgumentException. "String not empty."))
                          nil)
              :long #(Long/parseLong ^String %)
              :float #(Float/parseFloat ^String %)
              :boolean #(cond
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
                         false)
              :string identity})

(def safe-parsers (into {} (map (fn [[k v]]
                                  [k (fn [s]
                                       (try
                                         (v s)
                                         (catch Exception _
                                           :not-parseable)))])
                                parsers)))

(def parser-precedence [:missing :long :float :boolean :string])

(defn infer-type [datum]
  ;; TODO short circuit on existing types first
  (reduce (fn [r p]
            (let [parsed ((safe-parsers p) datum)]
              (if (not= parsed :not-parseable)
                (reduced p)
                r)))
          :not-parseable
          parser-precedence))

(defn infer-types [data]
  (reduce (fn [types row]
            (mapv #(conj %1 (infer-type %2)) types row))
          (repeat (count (first data)) #{})
          data))



(defn parse [types data]
  (mapv (fn [row]
          (mapv (fn [t d]
                  ((safe-parsers t) d)) types row))
        data))


(comment
  (infer-types data)

  (def parsed (parse [:long :long :string] data))

  (map #(zipmap column-names %) parsed)

  (let [types (infer-types data)]
    ;; transact schema for types
    (types->schema types)
    (->> data
       (parse types)
       (->entities)
       (partition 1024)
       (transact (d/db conn)))))
