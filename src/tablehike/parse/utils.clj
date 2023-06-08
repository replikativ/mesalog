(ns tablehike.parse.utils
  (:require [tech.v3.dataset.io.column-parsers :refer [parse-failure]]
            [tech.v3.datatype :as dtype])
  (:import [ham_fisted Casts]))


(defn make-safe-parse-fn [parse-fn]
  (fn [val]
    (try
      (parse-fn val)
      (catch Throwable _e
        parse-failure))))


(defn fast-dtype [value]
  (if (string? value)
    :string
    (dtype/datatype value)))
