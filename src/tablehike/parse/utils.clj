(ns tablehike.parse.utils
  (:require [tech.v3.dataset.io.column-parsers :refer [parse-failure missing]]
            [tech.v3.datatype :as dtype])
  (:import [ham_fisted Casts]))


(defn make-safe-parse-fn [parse-fn]
  (fn [val]
    (try
      (parse-fn val)
      (catch Throwable _e
        parse-failure))))


(defn missing-value?
  "Is this a missing value coming from a CSV file"
  [value]
  (cond
    (or (instance? Double value) (instance? Float value))
    (Double/isNaN (Casts/doubleCast value))
    (not (instance? Number value))
    (or (nil? value)
        (.equals "" value)
        (identical? value missing)
        (and (string? value) (re-matches #"(?i)^n\/?a$" value)))))


(defn fast-dtype [value]
  (if (string? value)
    :string
    (dtype/datatype value)))
