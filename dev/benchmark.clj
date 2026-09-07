(ns benchmark
  (:require [charred.api :as charred]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [criterium.core :as cr]
            [datahike.api :as d]
            [flatland.ordered.set :as flos]
            [mesalog.api :as mlapi]
            [mesalog.config :refer [data-dir]]
            [mesalog.parse.parser :as mlparse]
            [mesalog.read :as mlread])
  (:import [java.math BigDecimal RoundingMode]))


(def cli-options
  [["-b" "--batching" "Include benchmark with batching (using default of 128,000 rows)"]
   ["-f" "--file FILE" "File path where benchmark results should be written"]
   ["-h" "--help" "Show help"]])

(def data-files ["boolean-or-not.csv" "levels.csv" "calendar-dates.csv"
                 "pokemon.csv" "trips.csv" "stops.csv"])

(def larger-data-files #{"stops.csv" "311-service-requests-sample-300k.csv"})

(defn- all-benchmarking-files [include-batching]
  (cond-> data-files
    include-batching (conj "311-service-requests-sample-300k.csv")))

;; Needs updating if `:tail-quantile` default value of 0.025 changes
(def ^:private tail-percentile 2.5)

(def point-estimate-stats #{:mean :variance :lower-q :upper-q})

(def ^:private core-output-col-keys
  [:commit-hash :data-filename :mean :variance :lower-q :upper-q :sample-count])

(def ^:private output-col-keys
  (let [core-keys-vec (into [] core-output-col-keys)]
    (vec (concat (subvec core-keys-vec 0 1)
                 [:timestamp :function]
                 (subvec core-keys-vec 1)
                 (map (fn [adj] [:outliers adj])
                      [:low-severe :low-mild :high-mild :high-severe])
                 [:outlier-variance]))))

(defn- output-col-key->str [k]
  (let [keyword->string #(string/replace (name %) "-" " ")]
    (cond
      (vector? k) (-> (name (nth k 1))
                      (str " outliers"))
      (= :lower-q k) (str tail-percentile "th percentile")
      (= :upper-q k) (str (- 100 tail-percentile)
                          "th percentile")
      (= :outlier-variance k) (str (keyword->string k) " %")
      :else (keyword->string k))))

(def core-output-header-cols
  (map output-col-key->str core-output-col-keys))

(def output-header
  (mapv output-col-key->str output-col-keys))

(defn write-header [writer]
  (charred/write-csv writer [output-header]))

(defn- round-to-precision [v precision]
  (let [bd (BigDecimal. (str v))]
    (.setScale bd precision RoundingMode/HALF_UP)))

(defn- data-file [filename]
  (io/file data-dir filename))

(defn- csv->header-row [input]
  (let [row-iter ^Iterator (mlread/csv->row-iter input {})]
    (mlread/row-iter->header-row row-iter {})))

(defn- new->old-column-indices [header]
  (let [colname->idx #(reduce-kv (fn [m i v] (assoc m v i))
                                 {}
                                 %)
        output-header-col-indices (colname->idx output-header)
        old-col-indices (colname->idx header)]
    (reduce (fn [m k]
              (assoc m (output-header-col-indices k) (old-col-indices k)))
            {}
            output-header)))

(defn- transform-row-format [r idx-map]
  (concat (map #(when-let [old-idx (idx-map %)]
                  (nth r old-idx))
               (range (.length output-header)))
          (map #(nth r %)
               (set/difference (into (flos/ordered-set)
                                     (range (count r)))
                               (into #{} (vals idx-map))))))

(defn ensure-current-output-format [filename]
  (let [iofile (io/file filename)
        header (csv->header-row iofile)]
    (when (not= header output-header)
      (if (set/subset? core-output-header-cols (set header))
        (let [new->old-indices (new->old-column-indices header)
              old-results (next (charred/read-csv iofile))]
          (with-open [w (io/writer iofile)]
            (write-header w)
            (->> (map #(transform-row-format % new->old-indices) old-results)
                 (charred/write-csv w))))
        (throw (ex-info "CSV header doesn't include minimal set of columns"
                        {:min core-output-header-cols
                         :actual header}))))))

(defn- run-benchmark
  ([data-file-path outfile f f-invocation bench-f-sym]
   (let [{:keys [out err exit]} (sh/sh "git" "rev-parse" "HEAD")]
     (when (not (zero? exit))
       (throw (Exception. (str "Error getting commit hash: " err))))
     (let [stats (cr/with-progress-reporting
                   ((case bench-f-sym
                      :full cr/benchmark*
                      :quick cr/quick-benchmark*)
                    f {}))]
       (with-open [w (io/writer outfile :append true)]
         (->> [(into [(subs (string/trim out) 0 8)
                      (.toString (java.time.Instant/now))
                      f-invocation
                      data-file-path]
                     (map (fn [k]
                            (if (vector? k)
                              (get-in stats k)
                              (let [stat (stats k)]
                                (cond
                                  (point-estimate-stats k) (-> (nth stat 0)
                                                               (round-to-precision 8))
                                  (= :sample-count k) stat
                                  :else (-> (* 100 stat)
                                            (round-to-precision 2))))))
                          (subvec output-col-keys 4)))]
              (charred/write-csv w))))))
  ([data-file-path outfile f f-invocation]
   (run-benchmark data-file-path outfile f f-invocation :full)))

;; mean, variance, lower & upper quantiles, outlier count, outlier variance
(defn benchmark-load-csv
  ([outfile include-batching]
   (let [io-outfile (io/file outfile)
         data-files (all-benchmarking-files include-batching)]
     (if (or (not (.exists io-outfile))
             (zero? (.length io-outfile)))
       (write-header outfile)
       (ensure-current-output-format outfile))
     (doseq [data-f data-files]
       (let [bench-fn (partial run-benchmark
                               data-f
                               outfile
                               (fn []
                                 (let [cfg (d/create-database {})
                                       conn (d/connect cfg)]
                                   (try
                                     (mlapi/load-csv (data-file data-f) conn)
                                     (finally
                                       (d/delete-database cfg)))))
                               '(mlapi/load-csv (data-file data-f) conn))]
         (if (contains? larger-data-files data-f)
           (bench-fn :quick)
           (bench-fn))))))
  ([outfile]
   (benchmark-load-csv outfile false)))

(defn usage [summary]
  (str "Usage: clj -M:dev -m benchmark [options]\n" "Options:\n" summary))

(defn error-message [errors]
  (str "The following errors occurred while parsing your command:\n"
       (string/join \newline errors)))

(defn exit [status message]
  (println message)
  (System/exit status))

(defn summarise-opts [args]
  (let [{:keys [options errors summary]} (parse-opts args cli-options)]
    (cond
      errors {:exit-message (error-message errors)}
      (:help options) {:exit-message (usage summary)
                       :ok? true}
      (:file options) (conj {:output-file (:file options)}
                            {:batching? (:batching options)})
      :else {:exit-message (usage summary)})))

(defn -main [& args]
  (let [{:keys [exit-message ok? output-file batching?]} (summarise-opts args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (benchmark-load-csv output-file batching?))))
