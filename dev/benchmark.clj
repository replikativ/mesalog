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


(def benchmarkable-fns
  #{#'mlapi/load-csv})

(def ^:private benchmarkable-fns-str
  (->> (map #(subs (str %) 2) benchmarkable-fns)
       (string/join ", ")))

(def cli-options
  [["-f" "--function FUNCTION"
    (str "Function to benchmark. Must be one of: " benchmarkable-fns-str)
    :parse-fn (fn [s]
                (some-> s symbol resolve))
    :validate [#(contains? benchmarkable-fns %)
               (str "Must be one of: " benchmarkable-fns-str)]]
   ["-d" "--data-file FILE" "Data file to benchmark (repeatable)"
    :multi true
    :default (flos/ordered-set)
    :update-fn conj]
   ["-b" "--batching" "Include large benchmark file with batching (using default of 128,000 rows)"]
   ["-o" "--output FILE" "File path to which benchmark results should be written"]
   ["-h" "--help" "Show help"]])

(def larger-data-filename "stops.csv")

(def data-filenames
  (into (flos/ordered-set)
        ["boolean-or-not.csv" "levels.csv" "calendar-dates.csv"
         "pokemon.csv" "trips.csv" larger-data-filename]))

(def batching-data-filename "311-service-requests-sample-300k.csv")

(def larger-data-files #{"stops.csv" batching-data-filename})

(def all-data-filenames
  (conj data-filenames batching-data-filename))

(defn- select-data-files [cli-data-files batching?]
  (let [files (if (empty? cli-data-files)
                data-filenames
                (-> (conj data-filenames batching-data-filename)
                    (set/intersection cli-data-files)))]
    (cond-> files
      batching? (conj batching-data-filename))))

(defn- file-path [filename]
  (io/file data-dir filename))

;; Needs updating if `:tail-quantile` default value of 0.025 changes
(def ^:private tail-percentile 2.5)

(def point-estimate-stats #{:mean :variance :lower-q :upper-q})

(def ^:private core-output-col-keys
  [:commit-hash :data-filename :mean :variance :lower-q :upper-q :sample-count])

;; mean, variance, lower & upper quantiles, outlier count, outlier variance
(def ^:private output-col-keys
  (vec (concat (subvec core-output-col-keys 0 1)
               [:timestamp :function]
               (subvec core-output-col-keys 1)
               (map (fn [adj] [:outliers adj])
                    [:low-severe :low-mild :high-mild :high-severe])
               [:outlier-variance]
               (map (fn [d] [:os-details d])
                    [:arch :available-processors :name :version])
               (map (fn [d] [:runtime-details d])
                    [:vm-name :vm-version :java-version :clojure-version-string]))))

(defn- vector-output-col-key->str [k1 k2]
  (case k1
    :outliers (str (name k2) " outliers")
    :os-details (case k2
                  :arch "architecture"
                  :available-processors "available processors"
                  (str "OS " (name k2)))
    :runtime-details (let [[t1 t2 t3] (string/split (name k2) #"-")]
                       (case t1
                         "vm" (str "JVM " t2)
                         (str (string/capitalize t1) " version")))))

(defn- output-col-key->str [k]
  (let [keyword->string #(string/replace (name %) "-" " ")]
    (cond
      (vector? k) (apply vector-output-col-key->str k)
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

(defn large-benchmark
  "Adapted from `criterium/benchmark*` and `criterium/run-benchmark`"
  [sample-count n-exec f gc-before-sample overhead]
  (cr/force-gc)
  (let [rt (Runtime/getRuntime)
        total-overhead (long (* (or overhead 0) 1e9 n-exec))
        _   (cr/progress "Sampling ...")
        _   (cr/debug
             "Running with\n sample-count" sample-count \newline
             "exec-count" n-exec \newline
             "overhead[s]" overhead \newline
             "total-overhead[ns]" total-overhead)
        final-gc-time (cr/final-gc)
        samples (cr/collect-samples sample-count n-exec f gc-before-sample)
        sample-times (->> samples
                          (map first)
                          (map #(- % total-overhead)))
        total (reduce + 0 sample-times)
        final-gc-result (cr/final-gc-warn total final-gc-time)]
    (cr/benchmark-stats {:execution-count n-exec
                         :sample-count sample-count
                         :samples sample-times
                         :results (map second samples)
                         :total-time (/ total 1e9)
                         :final-gc-time final-gc-time
                         :overhead overhead}
                        (select-keys cr/*default-quick-bench-opts*
                                     [:tail-quantile :bootstrap-size]))))

;; mean, variance, lower & upper quantiles, outlier count, outlier variance
(defn- run-benchmark
  [expr-fn expr-form data-file-path outfile]
  (let [{:keys [out err exit]} (sh/sh "git" "rev-parse" "HEAD")]
    (println data-file-path)
    (when (not (zero? exit))
      (throw (Exception. (str "Error getting commit hash: " err))))
    (let [benchmark-fn (fn []
                         (if (= batching-data-filename data-file-path)
                           (large-benchmark 6 1 expr-fn true nil)
                           (cr/benchmark*
                            expr-fn
                            (if (= larger-data-filename data-file-path)
                              cr/*default-quick-bench-opts*
                              {}))))
          stats (cr/with-progress-reporting (benchmark-fn))]
      (with-open [w (io/writer outfile :append true)]
        (->> [(into [(subs (string/trim out) 0 8)
                     (.toString (java.time.Instant/now))
                     expr-form
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

(defn run-benchmarks
  [expr-fn expr-form data-files outfile]
  (let [io-outfile (io/file outfile)]
    (println data-files)
    (if (or (not (.exists io-outfile))
            (zero? (.length io-outfile)))
      (write-header outfile)
      (ensure-current-output-format outfile))
    (doseq [data-f data-files]
      (run-benchmark (expr-fn data-f)
                     (expr-form data-f)
                     data-f
                     outfile))))

(defn usage [summary]
  (str "Usage: clj -M:dev -m benchmark [options]\n"
       "Options:\n" summary "\n"))

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
      (:output options) {:function (:function options)
                         :data-files (:data-file options)
                         :batching? (:batching options)
                         :output-file (:output options)}
      :else {:exit-message (usage summary)})))

; Note: `nil` return value advisable because `mutable-place` in Criterium
; holds a reference to the return value of the benchmarked expression, and garbage collection
; seems to be delayed, causing a heap memory error if a large value is returned instead.
(def benchmark-fn-exprs-and-forms
  {#'mlapi/load-csv
   {:expr-fn (fn [data-file]
               (fn []
                 (let [cfg (d/create-database {})
                       conn (d/connect cfg)]
                   (try
                     (mlapi/load-csv (file-path data-file) conn)
                     (finally
                       (d/delete-database cfg))))
                 nil))
    :expr-form (fn [data-file]
                 (list 'api/load-csv
                       (list 'file-path (symbol data-file))
                       'conn))}})

(defn -main [& args]
  (let [{:keys [exit-message ok? function data-files batching? output-file] :as summary}
        (summarise-opts args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (let [{:keys [expr-fn expr-form]} (benchmark-fn-exprs-and-forms function)
            data-files (select-data-files data-files batching?)]
        (run-benchmarks expr-fn expr-form data-files output-file)))))
