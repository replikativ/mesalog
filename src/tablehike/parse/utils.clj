(ns tablehike.parse.utils
  (:require [charred.api :as charred]
            [tech.v3.datatype :as dtype]))


(def parse-failure :tablehike/parse-failure)


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


(defn homogeneous-sequence? [v]
  (and (sequential? v)
       (apply = v)))


(defn map-col-names->indices [parsers]
  (into {}
        (map (fn [{:keys [column-idx column-name]}]
               [column-name column-idx]))
        parsers))


(defn map-idents->indices
  ([idents parsers tuples composite-tuples]
   (let [col-name->index (map-col-names->indices parsers)
         all-tuples-map (cond-> composite-tuples
                          (map? tuples) (merge tuples))]
     (into {}
           (map (fn [ident]
                  [ident (mapv col-name->index
                               (condp contains? ident
                                 col-name->index [ident]
                                 all-tuples-map (get all-tuples-map ident)))]))
           idents)))
  ([idents parsers tuples]
   (map-idents->indices idents parsers tuples nil)))
