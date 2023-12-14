(ns tablehike.demo
  (:require [clojure.java.io :as io]
            [datahike.api :as d]
            [tablehike.core :as tbh]))


; Note: API NOT STABLE... in fact, it WILL change

(def data-dir "data")

(def cfg (atom nil))

(defn cfg-get [] @cfg)

(defn cfg-reset []
  (reset! cfg (d/create-database {})))

(def conn (atom nil))

(defn conn-set []
  (reset! conn (d/connect @cfg)))

(defn conn-get [] @conn)

(defn db-init []
  (cfg-reset)
  (conn-set))

(defn db-delete []
  (d/delete-database @cfg))


; DEMO

; Cardinality
;
(def shapes-file (io/file data-dir "shapes.csv"))

; Cardinality-many inference
(db-init)
(tbh/load-csv shapes-file
              (conn-get)
              {:db.unique/identity #{:shape/id}})
; Check schema and view datoms
(d/schema @(conn-get))
(d/datoms @(conn-get) :eavt)

; NOTE run `delete-database` then recreate DB and establish connection
; between mutually conflicting invocations of load-csv
(db-delete)

; Doesn't quite make sense: demo purposes only
; Cardinality-one inference
(db-init)
(let [latlon [:shape/pt-lat :shape/pt-lon]]
  (->> {:db.type/tuple {:shape/pt [:shape/id :shape/pt-sequence]}
        :db.unique/identity #{:shape/pt}
        :db.type/compositeTuple {:shape/coordinates latlon}}
       (tbh/load-csv shapes-file (conn-get))))
(db-delete)

; NOTE not meant to be run: for the record only
; This would be better, but there's currently a bug in Datahike that
; mishandles cardinality-many tuples
(->> {:db.type/tuple {:shape/pt [:shape/pt-sequence :shape/pt-lat :shape/pt-lon]}
      :db.unique/identity #{:shape/id}}
     (tbh/load-csv shapes-file (conn-get)))

; But curiously, this seems to work
(db-init)
(->> {:db.type/compositeTuple {:shape/pt [:shape/pt-sequence :shape/pt-lat :shape/pt-lon]}
      :db.unique/identity #{:shape/id}}
     (tbh/load-csv shapes-file (conn-get)))

; query data just transacted by last example invocation
(def min-avg-max
  (first (d/q '[:find (min ?lat) (avg ?lat) (max ?lat)
                :where
                [_ :shape/pt-lat ?lat]]
              @(conn-get))))
(let [[min avg] min-avg-max]
  (d/q '[:find ?pt
         :in $ ?min ?avg
         :where
         [?shpt :shape/pt ?pt]
         [?shpt :shape/pt-lat ?pt-lat]
         [(nth ?pt 1) ?lat]
         [(> ?lat ?min)]
         [(< ?lat ?avg)]]
       @(conn-get) min avg))
(db-delete)


; Refs

; NOTE `:parser-sample-size` needed here to sample enough rows for correctly inferring type
(db-init)
(tbh/load-csv (io/file data-dir "stops.csv")
              (conn-get)
              {:db.unique/identity #{:stop/id}
               :db/index #{:stop/id}
               :db.type/ref {:stop/parent-station :stop/id}}
              {:parser-sample-size 40670})
; TODO How do I find entities WITHOUT a certain attribute???
; (To locate the parent station programmatically by identifying stations nil parent-station)
(d/q '[:find ?id
       :where
       [?parent :stop/id "de:12072:900245027"]
       [?child :stop/parent-station ?parent]
       [?child :stop/id ?id]]
     @(conn-get))

; How refs are useful

; Finding coordinates of all parent stations, with refs
(d/q '[:find ?lat ?lon
       :where
       [?child :stop/parent-station ?parent]
       [?parent :stop/lat ?lat]
       [?parent :stop/lon ?lon]]
     @(conn-get))
(db-delete)

; Without refs
(db-init)
(tbh/load-csv (io/file data-dir "stops.csv")
              (conn-get)
              {:db.unique/identity #{:stop/id}
               :db/index #{:stop/id}}
              {:parser-sample-size 40670})
(count (d/q '[:find ?lat ?lon
       :where
       [?child :stop/parent-station ?parent-id]
       [?parent :stop/id ?parent-id]
       [?parent :stop/lat ?lat]
       [?parent :stop/lon ?lon]]
     @(conn-get)))
(db-delete)

; Also useful for transactions, e.g.:
; 1. Creating nested entity from ref attribute specified as nested map with tempid
; 2. Reverse attribute name as shorthand (for created entity to be referenced by existing)
; (Not in demo)


; Tuples

; Homogeneous tuples
(db-init)
(tbh/load-csv (io/file data-dir "pokemon.csv") (conn-get))
(d/datoms @conn :eavt)
(:abilities (d/schema @(conn-get)))
(d/q '[:find ?p
       :where
       [?p :abilities "Chlorophyll"]]
     @(conn-get))
(db-delete)

; Heterogeneous tuples
(db-init)
(tbh/load-csv (io/file data-dir "311-service-requests-sample.csv")
              (conn-get)
              {}
              {:vector-open \(
               :vector-close \)})
(select-keys (d/schema @(conn-get)) [:Latitude :Longitude :Location])
(db-delete)

; Currently can't pass in custom parser like below
;:parser-fn {40 [:vector
;                (fn [v]
;                  (let [len (.length ^String v)
;                        re (re-pattern ", ")]
;                    (mapv (get parser/default-coercers :float32)
;                          (-> (subs v 1 (dec len))
;                              (str/split re)))))]}}
