(ns tablehike.demo
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.math :refer [ceil]]
            [datahike.api :as d]
            [tablehike.core :as tbh]
            [tablehike.parse.datetime :as dt]
            [tablehike.parse.parser :as parser]
            [tablehike.read :as csv-read]
            [tablehike.schema :as schema])
  (:import [tablehike.read TakeReducer]))


; TODO find sane location in repo for demo data

(def cfg (d/create-database {}))
(def conn (d/connect cfg))

(def data-dir "/Users/yiffle/programming/code/vbb-gtfs/data")

; Cardinality
;
(def shapes-file (io/file data-dir "shapes-sample.csv"))

; Cardinality-many inference
(def latest-db (tbh/load-csv shapes-file
                             conn
                             {:db.unique/identity #{:shape/id}}))
(d/schema @conn)
(d/datoms @conn :eavt)

; This would be better, but there's currently a bug in Datahike that
; mishandles cardinality-many tuples
(->> {:db.type/tuple {:shape/pt [:shape/pt-sequence :shape/pt-lat :shape/pt-lon]}
      :db.unique/identity #{:shape/id}}
     (tbh/load-csv shapes-file conn))

; But curiously, this works (or rather seems to)
; Further demo below time permiitting: it's not that useful, or I don't know the right syntax
(->> {:db.type/compositeTuple {:shape/pt [:shape/pt-sequence :shape/pt-lat :shape/pt-lon]}
      :db.unique/identity #{:shape/id}}
     (tbh/load-csv shapes-file conn))

; Doesn't quite make sense: demo purposes only
; Cardinality-one inference
(let [latlon [:shape/pt-lat :shape/pt-lon]]
  (->> {:db.type/tuple {:shape/pt [:shape/id :shape/pt-sequence]}
        :db.unique/identity #{:shape/pt}
        :db.type/compositeTuple {:shape/coordinates latlon}}
       (tbh/load-csv shapes-file conn)))
(d/schema @conn)
(d/datoms @conn :eavt)

; Doesn't work as one might expect
(def min-avg-max
  (d/q '[:find (min ?lat) (avg ?lat) (max ?lat)
         :where
         [_ :shape/pt-lat ?lat]]
       @conn))
(let [[min avg] min-avg-max]
  (d/q '[:find ?pt
         :in $ ?min ?avg
         :where
         [?shpt :shape/pt-lat ?lat]
         [?shpt :shape/pt ?pt]
         [(> ?lat ?min)]
         [(< ?lat ?avg)]]
       @conn min avg))


; Refs

; Gotcha: the "basic problem"
(def latest-db (tbh/load-csv (io/file data-dir "stops.csv")
                             conn
                             {:db.unique/identity #{:stop/id}
                              :db/index #{:stop/id}
                              :db.type/ref {:stop/parent_station :stop/id}}))
(d/schema @conn)
(d/datoms @conn :eavt)
(d/delete-database cfg)

(def cfg (d/create-database {}))
(def conn (d/connect cfg))
(def latest-db (tbh/load-csv (io/file data-dir "stops.csv")
                             conn
                             {:db.unique/identity #{:stop/id}
                              :db/index #{:stop/id}
                              :db.type/ref {:stop/parent_station :stop/id}}
                             {:parser-sample-size 40670}))
(d/schema @conn)
(d/datoms @conn :eavt)

; TODO How do I find entities WITHOUT a certain attribute???
; (To locate the parent station programmatically)
(d/q '[:find ?id ?parent
       :where
       [?parent :stop/id "de:12072:900245027"]
       [?child :stop/parent_station ?parent]
       [?child :stop/id ?id]]
     @conn)

; How refs are useful

; Finding coordinates of all parent stations, with refs
(d/q '[:find ?lat ?lon
       :where
       [?child :stop/parent_station ?parent]
       [?parent :stop/lat ?lat]
       [?parent :stop/lon ?lon]]
     @conn)

; Without refs
(d/q '[:find ?lat ?lon
       :where
       [?child :stop/parent_station ?parent-id]
       [?parent :stop/id ?parent-id]
       [?parent :stop/lat ?lat]
       [?parent :stop/lon ?lon]]
     @conn)

; Also useful for transactions

(d/schema @conn)
(d/datoms @conn :eavt)

; Finis demo DB 1
(d/delete-database cfg)


; Tuples

; Homogeneous tuples

(def cfg (d/create-database))
(def conn (d/connect cfg))

(tbh/load-csv "test/data/pokemon.csv" conn)
(d/datoms @conn :eavt)
(:abilities (d/schema @conn))

(d/q '[:find ?p
       :where
       [?p :abilities "Chlorophyll"]]
     @conn)

(d/delete-database cfg)

; Heterogeneous tuples

(def cfg (d/create-database))
(def conn (d/connect cfg))

(tbh/load-csv "test/data/311-service-requests-sample.csv"
              conn
              {}
              {:vector-open \(
               :vector-close \)})
(d/schema @conn)
(d/datoms @conn :eavt)

; Currently can't pass in custom parser like below
;:parser-fn {40 [:vector
;                (fn [v]
;                  (let [len (.length ^String v)
;                        re (re-pattern ", ")]
;                    (mapv (get parser/default-coercers :float32)
;                          (-> (subs v 1 (dec len))
;                              (str/split re)))))]}}

(d/delete-database cfg)
