(ns tablehike.parse.datetime
  (:require [tech.v3.dataset.io.column-parsers :refer [parse-failure]])
  (:import [java.time LocalDate LocalDateTime OffsetDateTime ZonedDateTime ZoneId]
           [java.util Date]))


(def datetime-datatypes
  #{:instant :local-date :packed-instant :packed-local-date
    :local-date-time :zoned-date-time :offset-date-time})


(defn datetime->date [dt]
  (let [zone-id (ZoneId/systemDefault)
        dt (condp instance? dt
             LocalDate      (.atStartOfDay dt zone-id)
             LocalDateTime  (.atZone dt zone-id)
             ZonedDateTime  dt
             OffsetDateTime dt)]
    (Date/from (.toInstant dt))))


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


(defn update-datetime-coercers [coercers]
  (let [coercers (-> (apply dissoc
                            coercers
                            #{:duration :local-time :packed-duration :packed-local-time})
                     (assoc :offset-date-time #(OffsetDateTime/parse ^String %)))]
    (merge coercers
           (->> (select-keys coercers datetime-datatypes)
                (map (fn [[k v]] [k (fn [dt]
                                      (let [parsed-value (v dt)]
                                        (if (identical? parse-failure parsed-value)
                                          parse-failure
                                          (datetime->date parsed-value))))]))
                (into {})))))
