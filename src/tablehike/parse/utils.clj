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


(defn map-col-idents->indices [parsers]
  (into {}
        (map (fn [{:keys [column-idx column-ident]}]
               [column-ident column-idx]))
        parsers))


(defn map-idents->indices
  ([idents parsers tuples composite-tuples]
   (let [col-ident->index (map-col-idents->indices parsers)
         all-tuples-map (cond-> composite-tuples
                          (map? tuples) (merge tuples))]
     (into {}
           (map (fn [ident]
                  [ident (mapv col-ident->index
                               (condp contains? ident
                                 col-ident->index [ident]
                                 all-tuples-map (get all-tuples-map ident)))]))
           idents)))
  ([idents parsers tuples]
   (map-idents->indices idents parsers tuples nil)))


(defn- strip-vector-str-delims-fn [options]
  (let [{delims-use :vector-delims-use
         open :vector-open-char
         close :vector-close-char
         :or {delims-use true
              open \[
              close \]}} options]
    (fn [str]
      (let [len (.length ^String str)]
        (if (or delims-use (= len 0))
          (if (and (identical? (nth str 0) open)
                   (-> (nth str (dec len))
                       (identical? close)))
            (subs str 1 (dec len))
            parse-failure)
          str)))))


(defn tokenise-csv-str-fn [options]
  (let [opts (if-some [vs (get options :vector-separator)]
               (assoc options :separator vs)
               options)]
    (fn [str]
      (nth (charred/read-csv str opts) 0))))


(defn vector-str->elmt-strs-fn [options]
  (fn [str]
    (let [elmts-str ((strip-vector-str-delims-fn options) str)]
      (if (identical? elmts-str parse-failure)
        parse-failure
        ((tokenise-csv-str-fn options) elmts-str)))))
