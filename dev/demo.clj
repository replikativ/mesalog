(ns demo
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [config :refer [data-dir]]
            [criterium.core :as cr]
            [datahike.api :as d]
            [mesalog.api :as m]
            [mesalog.read :as m-read]
            [mesalog.parse.datetime :as dt]
            [mesalog.parse.parser :as parser]
            [mesalog.schema :as schema])
  (:import [java.time LocalDateTime]
           [java.time.format DateTimeFormatter]))


;; Note:
;; 1. Try in `dev` mode so that `criterium` is available
;; 2. API NOT STABLE

(def ^:private cfg (atom nil))

(def ^:private conn (atom nil))

(defn- conn-get [] @conn)

(defn- cfg-reset []
  (reset! cfg (d/create-database {})))

(defn- db-delete []
  (d/delete-database @cfg))

(defn- conn-reset []
  (reset! conn (d/connect @cfg)))

(defn- db-reset []
  (db-delete)
  (cfg-reset)
  (conn-reset))


;;;; DEMO

;;; Cardinality

(def ^:private shapes-file (io/file data-dir "shapes.csv"))

; Cardinality-many inference
(cfg-reset)
(conn-reset)
(m/load-csv shapes-file
              (conn-get)
              {}
              {:db.unique/identity #{:shape/id}})
; Check schema and view datoms
(d/schema @(conn-get))
(d/datoms @(conn-get) :eavt)

; NOTE run `db-reset` between mutually conflicting invocations of load-csv
; Doesn't quite make sense: demo purposes only
; Cardinality-one inference
(db-reset)
(let [latlon [:shape/pt-lat :shape/pt-lon]]
  (->> {:db.type/tuple {:shape/pt [:shape/id :shape/pt-sequence]}
        :db.unique/identity #{:shape/pt}
        :db.type/compositeTuple {:shape/coordinates latlon}}
       (m/load-csv shapes-file (conn-get) {})))
(d/schema @(conn-get))

; NOTE not meant to be run: for the record only
; This would be better, but there's currently a bug in Datahike that
; mishandles cardinality-many tuples
(->> {:db.type/tuple {:shape/pt [:shape/pt-sequence :shape/pt-lat :shape/pt-lon]}
      :db.unique/identity #{:shape/id}}
     (m/load-csv shapes-file (conn-get)))

; But curiously, this seems to work
(db-reset)
(->> {:db.type/compositeTuple {:shape/pt [:shape/pt-sequence :shape/pt-lat :shape/pt-lon]}
      :db.unique/identity #{:shape/id}}
     (m/load-csv shapes-file (conn-get) {}))
(d/schema @(conn-get))

; query data just transacted by last example invocation
(def ^:private min-avg-max
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


;;; Refs

; NOTE `:parser-sample-size` needed here to sample enough rows for correctly inferring type
(db-reset)
(m/load-csv (io/file data-dir "stops.csv")
              (conn-get)
              {}
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

;; PSA: How refs are useful
;; Can be skipped without loss of continuity

; Finding coordinates of all parent stations, with refs
(def ^:private all-parent-coordinates
  (d/q '[:find ?lat ?lon
         :where
         [?child :stop/parent-station ?parent]
         [?parent :stop/lat ?lat]
         [?parent :stop/lon ?lon]]
       @(conn-get)))
(count all-parent-coordinates)

; Without refs
(db-reset)
(m/load-csv (io/file data-dir "stops.csv")
              (conn-get)
              {}
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

; Also useful for transactions, e.g.:
; 1. Creating nested entity from ref attribute specified as nested map with tempid
; 2. Reverse attribute name as shorthand (for created entity to be referenced by existing)
; (Not in demo)


;;; Variable-length homogeneous vector-valued column interpreted as cardinality-many attribute

(db-reset)
(m/load-csv (io/file data-dir "pokemon.csv") (conn-get))
(d/datoms @(conn-get) :eavt)
(:abilities (d/schema @(conn-get)))
(d/q '[:find ?p
       :where
       [?p :abilities "Chlorophyll"]]
     @(conn-get))


;;; Vector detection and parsing

; Homogeneous tuples
(db-reset)
(m/load-csv (io/file data-dir "311-service-requests-sample.csv")
              (conn-get)
              {}
              {}
              {:vector-open-char \(
               :vector-close-char \)})
(d/schema @(conn-get))
(select-keys (d/schema @(conn-get)) [:Latitude :Longitude :Location])
(d/delete-database @cfg)


;;; Performance

; Extracted from `api/load-csv` for demo purposes only
(defn- infer-parsers-and-schema
  [filename conn parsers-desc schema-desc options]
  (let [colname->ident (parser/colname->ident-fn options)
        parsers (mapv (fn [p]
                        ;; temporary stopgap in lieu of proper interface
                        ;; between DB attr idents and col names
                        (update (->> (colname->ident (:column-name p))
                                     (assoc p :column-ident))
                                :parser-dtype
                                #(if (contains? dt/datetime-datatypes %)
                                   :db.type/instant
                                   %)))
                      (parser/infer-parsers filename parsers-desc options))
        schema-on-read (= (:schema-flexibility (:config @conn))
                          :read)
        existing-schema (d/schema @conn)
        schema (when (not schema-on-read)
                 (schema/build-schema
                  parsers
                  schema-desc
                  existing-schema
                  (d/reverse-schema @conn)
                  (m-read/csv->header-skipped-row-iter filename options)
                  options))]
    {:parsers parsers
     :schema schema}))

; Sample data file contains 2,000,000 rows and 41 columns
(defn- download-311-data-sample [filename]
  (let [wget-out (sh/sh "wget" "-O" filename "http://tinyurl.com/4ux2htkn")]
    (if (= (:exit wget-out) 0)
      (let [mv-out (sh/sh "mv" filename data-dir)]
        (if (= (:exit mv-out) 0)
          (println "311 data downloaded into " filename "in folder " data-dir)
          (println "error during `mv`: " (:err mv-out))))
      (println "error during `wget`: " (:err wget-out)))))


(db-reset)
(def ^:private filename-311 "311_service_requests_2010-present_sample.csv")
(download-311-data-sample filename-311)
(cr/with-progress-reporting
  (cr/quick-bench (infer-parsers-and-schema (io/file data-dir filename-311)
                                            (conn-get)
                                            {}
                                            {}
                                            {:vector-open-char \(
                                             :vector-close-char \)
                                             :num-rows 1000000})))
;; Evaluation count : 6 in 6 samples of 1 calls.
;;              Execution time mean : 44.948350 sec
;;     Execution time std-deviation : 1.216673 sec
;;    Execution time lower quantile : 44.055686 sec ( 2.5%)
;;    Execution time upper quantile : 46.498014 sec (97.5%)
;;                    Overhead used : 8.896033 ns

(cr/with-progress-reporting
  (cr/quick-bench (parser/infer-parsers (io/file data-dir filename-311)
                                        {}
                                        {:vector-open-char \(
                                         :vector-close-char \)
                                         :num-rows 1000000})))
;; Evaluation count : 6 in 6 samples of 1 calls.
;;              Execution time mean : 45.898523 sec
;;     Execution time std-deviation : 1.025800 sec
;;    Execution time lower quantile : 45.286529 sec ( 2.5%)
;;    Execution time upper quantile : 47.655087 sec (97.5%)
;;                    Overhead used : 8.896033 ns

(cr/with-progress-reporting
  (cr/quick-bench (parser/infer-parsers (io/file data-dir filename-311)
                                        {}
                                        {:num-rows 1000000})))
;; Evaluation count : 6 in 6 samples of 1 calls.
;;              Execution time mean : 44.093829 sec
;;     Execution time std-deviation : 1.122148 sec
;;    Execution time lower quantile : 43.370246 sec ( 2.5%)
;;    Execution time upper quantile : 46.017279 sec (97.5%)
;;                    Overhead used : 8.896033 ns


(comment
  (require '[tech.v3.dataset.io.csv :as csv])
  (import '[java.time LocalDateTime]
          '[java.util Locale])

  (let [dt-parse-fn #(->> (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss")
                          (LocalDateTime/parse %))]
    (cr/with-progress-reporting
      (cr/quick-bench (parser/infer-parsers (io/file data-dir filename-311)
                                            {1 [:db.type/instant dt-parse-fn]
                                             2 [:db.type/instant dt-parse-fn]}
                                            {:num-rows 1000000}))))
  ;; Evaluation count : 6 in 6 samples of 1 calls.
  ;;              Execution time mean : 49.237197 sec
  ;;     Execution time std-deviation : 1.098354 sec
  ;;    Execution time lower quantile : 48.330083 sec ( 2.5%)
  ;;    Execution time upper quantile : 51.042669 sec (97.5%)
  ;;                    Overhead used : 8.896033 ns


  (let [dt-parse-fn #(->> (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss")
                          (LocalDateTime/parse %))]
    (cr/with-progress-reporting
      (cr/quick-bench (parser/infer-parsers (io/file data-dir filename-311)
                                            {1 [:db.type/instant dt-parse-fn]
                                             2 [:db.type/instant dt-parse-fn]}
                                            {:num-rows 1000000}))))

  (let [dtype-parser-tuple [:local-date-time
                            (DateTimeFormatter/ofPattern "MM/dd/yyyy hh:mm:ss a"
                                                         (Locale. "ENGLISH"))]]
    (cr/with-progress-reporting
      (cr/quick-bench (csv/csv->dataset (io/file data-dir filename-311)
                                        {:num-rows 10000
                                         :parser-fn {"Created Date" dtype-parser-tuple
                                                     "Closed Date" dtype-parser-tuple
                                                     "Resolution Action Updated Date" dtype-parser-tuple}}))))
  ; Evaluation count : 6 in 6 samples of 1 calls.
  ;              Execution time mean : 188.888004 ms
  ;     Execution time std-deviation : 16.487412 ms
  ;    Execution time lower quantile : 166.057849 ms ( 2.5%)
  ;    Execution time upper quantile : 202.549871 ms (97.5%)
  ;                    Overhead used : 8.876847 ns

  (let [dtype-parser-tuple [:local-date-time
                            #(->> (DateTimeFormatter/ofPattern "MM/dd/yyyy hh:mm:ss a"
                                                               (Locale. "ENGLISH"))
                                  (LocalDateTime/parse %))]]
    (cr/with-progress-reporting
      (cr/quick-bench (parser/infer-parsers (io/file data-dir filename-311)
                                            {}
                                            {:num-rows 10000
                                             :parser-fn {"Created Date" dtype-parser-tuple
                                                         "Closed Date" dtype-parser-tuple
                                                         "Resolution Action Updated Date" dtype-parser-tuple}}))))


  ;; `csv->dataset` (and the library it belongs to) just doesn't seem to accommodate the following
  ; [:vector
  ;  #(let [len (.length ^String %)]
  ;     (map float
  ;          (-> (subs % 1 (- len 1))
  ;              (clj-str/split #","))))]
)
