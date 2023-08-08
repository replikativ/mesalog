(ns tablehike.parse.datetime
  (:require [clojure.string :as s]
            [tablehike.parse.utils :as utils])
  (:import [java.time Instant LocalDate LocalDateTime LocalTime OffsetDateTime ZonedDateTime ZoneId]
           [java.time.format DateTimeFormatter DateTimeFormatterBuilder]
           [java.util Date Locale]))


(set! *warn-on-reflection* true)


;;Assuming that the string will have /,-. replaced with /space
(def ^{:doc "Local date parser patterns used to generate the local date
formatter."} local-date-parser-patterns
  ["yyyy MM dd"
   "yyyyMMdd"
   "MM dd yyyy"
   "dd MMM yyyy"
   "M d yyyy"
   "M d yy"
   "MMM dd yyyy"
   "MMM dd yy"
   "MMM d yyyy"])


(defn- date-preparse
  ^String [^String data]
  (.replaceAll data "[/,-. ]+" " "))


(def ^{:doc "DateTimeFormatter that runs through a set of options in order to
parse a wide variety of local date formats."
       :tag DateTimeFormatter}
  local-date-formatter
  (let [builder (DateTimeFormatterBuilder.)]
    (.parseCaseInsensitive builder)
    (doseq [pattern local-date-parser-patterns]
      (.appendOptional builder (DateTimeFormatter/ofPattern pattern)))
    (.appendOptional builder DateTimeFormatter/ISO_LOCAL_DATE)
    (.toFormatter builder Locale/ENGLISH)))


(defn parse-local-date
  "Convert a string into a local date attempting a wide variety of format types."
  ^LocalDate [^String str-data]
  (LocalDate/parse (date-preparse str-data) local-date-formatter))


(def ^{:doc "Parser patterns to parse a wide variety of time strings"}
  time-parser-patterns
  ["HH:mm:ss:SSS"
   "hh:mm:ss a"
   "HH:mm:ss"
   "h:mm:ss a"
   "H:mm:ss"
   "hh:mm a"
   "HH:mm"
   "HHmm"
   "h:mm a"
   "H:mm"])


(def ^{:doc "DateTimeFormatter built to help parse a wide variety of time strings"
       :tag DateTimeFormatter}
  local-time-formatter
  (let [builder (DateTimeFormatterBuilder.)]
    (.parseCaseInsensitive builder)
    (doseq [pattern time-parser-patterns]
      (.appendOptional builder (DateTimeFormatter/ofPattern pattern)))
    (.appendOptional builder DateTimeFormatter/ISO_LOCAL_TIME)
    (.toFormatter builder Locale/ENGLISH)))


(defn- local-time-preparse
  ^String [^String data]
  (.replaceAll data "[._]" ":"))


(defn parse-local-time
  "Convert a string into a local time attempting a wide variety of
  possible parser patterns."
  ^LocalTime [^String str-data]
  (LocalTime/parse (local-time-preparse str-data) local-time-formatter))


(defn parse-local-date-time
  "Parse a local-date-time by first splitting the string and then separately
  parsing the local-date portion and the local-time portions."
  ^LocalDateTime [^String str-data]
  (let [split-data (s/split str-data #"[ T]+")]
    (cond
      (== 2 (count split-data))
      (let [local-date (parse-local-date (first split-data))
            local-time (parse-local-time (second split-data))]
        (LocalDateTime/of local-date local-time))
      (== 3 (count split-data))
      (let [local-date (parse-local-date (first split-data))
            local-time (parse-local-time (str (split-data 1) " " (split-data 2)))]
        (LocalDateTime/of local-date local-time))
      :else
      (throw (Exception. (format "Failed to parse \"%s\" as a LocalDateTime"
                                 str-data))))))


(def datetime-datatypes
  #{:instant :local-date :local-date-time :zoned-date-time :offset-date-time})


(defn datetime->date [dt]
  (-> (if (instance? OffsetDateTime dt)
        (.toInstant ^OffsetDateTime dt)
        (let [zone-id (ZoneId/systemDefault)
              dt (condp instance? dt
                   LocalDate      (.atStartOfDay ^LocalDate dt zone-id)
                   LocalDateTime  (.atZone ^LocalDateTime dt zone-id)
                   ZonedDateTime  dt)]
          (.toInstant ^ZonedDateTime dt)))
      Date/from))


(def ^{:doc "Map of datetime datatype to generalized parse fn."}
  datatype->general-parse-fn-map
  (->> {:local-date parse-local-date
        :local-date-time parse-local-date-time
        :instant #(Instant/parse ^String %)
        :offset-date-time #(OffsetDateTime/parse ^String %)
        :zoned-date-time #(ZonedDateTime/parse ^String %)}
       (into {} (map (fn [[k v]]
                       [k (utils/make-safe-parse-fn (comp datetime->date v))])))))


(defn datetime-formatter-parse-fn
  "Given a datatype and a formatter return a function that attempts to
  parse that specific datatype, then convert into a java.util.Date."
  [dtype formatter]
  (let [formatter-parser (case dtype
                           :local-date #(LocalDate/parse % formatter)
                           :local-date-time #(LocalDateTime/parse % formatter)
                           :zoned-date-time #(ZonedDateTime/parse % formatter)
                           :offset-date-time #(OffsetDateTime/parse % formatter))]
    (comp datetime->date formatter-parser)))
