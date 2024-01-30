(ns tablehike.parser-test
  (:require [charred.api :as charred]
            [clojure.java.io :as io]
            [clojure.set :as clj-set]
            [clojure.string :as string]
            [clojure.test :refer [deftest testing is use-fixtures]]
            [tablecloth.api :as tc]
            [tablehike.parse.parser :as parser])
  (:import [java.util UUID]))


(def ^:private data-folder "data")
(def ^:private booleans-file (io/file data-folder "boolean-or-not.csv"))
(def ^:private agencies-file (io/file data-folder "agencies.csv"))
(def ^:private shapes-file (io/file data-folder "shapes.csv"))
(def ^:private shapes-modified-file (io/file data-folder "shapes-modified.csv"))
(def ^:private stops-file (io/file data-folder "stops-sample.csv"))
(def ^:private nyc-311-file (io/file data-folder "311-service-requests-sample.csv"))


(deftest empty-file
  (testing "parser inference on empty input CSV"
    (is (-> (parser/infer-parsers (io/file data-folder "empty.csv"))
            count
            (= 0)))))


(deftest long-and-string-inference
  (testing "data types correctly inferred for long- and string-valued columns"
    (doseq [p (parser/infer-parsers agencies-file)]
      (is (= (:parser-dtype p)
             (if (= (:column-name p) "agency/id")
               :db.type/long
               :db.type/string))))))


(deftest boolean-inference
  (testing (str "type inference for columns with values meeting and "
                "not meeting boolean parsing criteria")
    (let [expected-col-dtypes {"char" :db.type/string
                               "word" :db.type/string
                               "bool" :db.type/boolean
                               "boolstr" :db.type/boolean
                               "boolean" :db.type/boolean}]
      (doseq [p (subvec (parser/infer-parsers booleans-file) 1)]
        (is (= (:parser-dtype p)
               (get expected-col-dtypes (:column-name p))))))))


(deftest numeric-type-inference
  (testing "type inference for numeric-valued columns"
    (let [expected-col-dtypes {"shape/id" :db.type/long
                               "shape/pt-lat" :db.type/double
                               "shape/pt-lon" :db.type/double
                               "shape/pt-sequence" :db.type/long}]
      (doseq [p (parser/infer-parsers shapes-file)]
        (is (= (:parser-dtype p)
               (get expected-col-dtypes (:column-name p))))))))


(deftest uuid-inference
  (testing "UUID type inference"
    (let [uuids (repeatedly 5 #(UUID/randomUUID))
          test-fname "uuid.csv"
          test-file (io/file test-fname)]
      (-> {:uuids (repeatedly 5 #(UUID/randomUUID))}
          tc/dataset
          (tc/write! test-fname))
      (try (let [parsers (parser/infer-parsers (io/file test-fname))
                 parser (first parsers)]
             (is (= (count parsers) 1))
             (is (= (:column-name parser) "uuids"))
             (is (= (:parser-dtype parser) :db.type/uuid))
             (is (= (count (set (:missing-indexes parser)))
                    0)))
           (finally (.delete test-file))))))


(deftest parser-sample-size-and-num-rows
  (let [test-fn (fn [opts]
                  (is (every? #(= (:parser-dtype %) :db.type/boolean)
                              (-> (parser/infer-parsers (io/file booleans-file)
                                                        {}
                                                        opts)
                                  (subvec 1 3)))))]
  (testing "`:parser-sample-size` option has the expected effect when `:num-rows` is unspecified"
    (test-fn {:parser-sample-size 6}))
  (testing "`:parser-sample-size` option has the expected effect when specified `:num-rows` is lower"
    (test-fn {:parser-sample-size 12
              :num-rows 6}))
  (testing "`:parser-sample-size` option has the expected effect when specified `:num-rows` is higher"
    (test-fn {:parser-sample-size 6
              :num-rows 12}))
  (testing "`:num-rows` option has the expected effect when `:parser-sample-size` unspecified"
    (test-fn {:num-rows 6}))))


(deftest col-types-specified
  (let [test-fn (fn [col-id-fn coltypes]
                  (doseq [p (parser/infer-parsers stops-file coltypes)]
                    (when-let [expected (get coltypes (col-id-fn p))]
                      (is (and (= (:parser-dtype p) expected)
                               (= (count (set (:failed-indexes p))) 0))))))]
  (testing "parsers/types (keyword, number, long) specified using column index are applied"
    (test-fn :column-idx
             {0 :db.type/keyword
              4 :db.type/number
              5 :db.type/number
              6 :db.type/long}))
  (testing "parsers/types (string, float, bigint) specified using column name are applied"
    (test-fn :column-name
             {"stop/id" :db.type/string
              "stop/lat" :db.type/float
              "stop/lon" :db.type/float
              "stop/location-type" :db.type/bigint}))
  (testing "parsers/types (keyword, number, long) specified using keywordised name are applied"
    (test-fn #(keyword (:column-name %))
             {:stop/id :db.type/keyword
              :stop/lat :db.type/number
              :stop/lon :db.type/number
              :stop/location-type :db.type/long}))))


(deftest types-and-parsers-specified-in-vector
  (testing "column types specified in a vector are applied"
    (let [types [:db.type/long
                 :db.type/float
                 :db.type/float
                 :db.type/bigint]
          parsers (parser/infer-parsers shapes-file types)]
    (doseq [i (range (count types))]
      (is (= (:parser-dtype (nth parsers i))
             (nth types i))))))
  (testing "column types and custom parsers specified in a vector are applied"
    (let [parse-fn #(-> (.setScale (bigdec %) 3 java.math.RoundingMode/HALF_EVEN)
                        float)
          dtype-parser [:db.type/float parse-fn]
          types [:db.type/long dtype-parser dtype-parser :db.type/bigint]
          parsers (parser/infer-parsers shapes-file types)]
      (doseq [i [1 2]]
        (is (= ((:parse-fn (nth parsers i))
                13.631534)
               (float (bigdec 13.632))))))))


(deftest specified-dtype-and-fn-used
  (testing (str "parser has data type and function corresponding to the specified "
                "column data type (only `:db.type/symbol` is tested)")
    (let [colname "word"
          parser (->> {colname :db.type/symbol}
                      (parser/infer-parsers booleans-file)
                      (filter #(= (:column-name %) colname))
                      first)]
      (is (symbol? ((:parse-fn parser) "true"))))))


(deftest missing-col-dtype-and-fn
  (let [parsers (parser/infer-parsers (io/file data-folder "routes.csv"))
        empty-cols #{3 5 6 7}
        nonempty-cols (-> (set (range (count parsers)))
                          (clj-set/difference empty-cols))]
    (testing "a column has only empty/missing values => it has nil data type and parse function"
      (doseq [i empty-cols]
        (is (nil? (-> (nth parsers i)
                      :parser-dtype)))
        (is (nil? (-> (nth parsers i)
                      :parse-fn)))))
    (testing "a column has nil data type and parse function => it has only empty/missing values"
      (doseq [i nonempty-cols]
        (is (some? (-> (nth parsers i)
                       :parser-dtype)))
        (is (some? (-> (nth parsers i)
                       :parse-fn)))))))


(deftest missing-indexes
  (testing "column parsers correctly report indexes of rows containing missing values"
    (let [max-row (- (count (charred/read-csv agencies-file))
                     2)]
      (is (= (-> (last (parser/infer-parsers agencies-file))
                 :missing-indexes
                 set)
             (clj-set/difference (set (range (+ max-row 1)))
                                 #{(- max-row 2)}))))))


(deftest user-spec-types-failed-indexes-and-vals
  (let [parsers (->> {"char" :db.type/boolean
                      "word" :db.type/boolean}
                     (parser/infer-parsers booleans-file)
                     (filter #(contains? #{"char" "word"}
                                         (:column-name %))))
        test-indexes (fn [p expected]
                       (= (set (:failed-indexes p)) expected))
        test-vals (fn [p expected]
                    (= (:failed-values p) expected))]
    (testing "type (boolean) specified using column name are applied"
      (is (every? #(= (:parser-dtype %)
                      :db.type/boolean)
                  parsers)))
    (testing (str "for columns with user-specified types/parsers, values that cannot "
                  "be parsed and their corresponding row indexes are correctly reported")
      (doseq [p parsers]
        (case (:column-name p)
          "char" (is (and (test-indexes p #{8 9})
                          (test-vals p ["A" "z"])))
          "word" (is (and (test-indexes p #{6 7 8 9})
                          (test-vals p ["yep" "not" "pos" "neg"]))))))))


(deftest include-cols
  (let [colnames #{"Created Date" "Closed Date" "Descriptor"}]
    (testing "all columns to be included, specified by index, and no others, are parsed"
      (let [cols #{1 2 6}]
        (is (= (->> (parser/infer-parsers nyc-311-file
                                          {}
                                          {:include-cols cols})
                    (into #{} (map :column-idx)))
               cols))))
    (testing "all columns to be included, specified by name, and no others, are parsed"
      (is (= (->> (parser/infer-parsers nyc-311-file
                                        {}
                                        {:include-cols colnames})
                  (into #{} (map :column-name)))
             colnames)))
    (testing (str "all columns to be included, specified by name keywordised using "
                  "default method, and no others, are parsed")
      (let [cols #{:Created-Date :Closed-Date :Descriptor}]
        (is (= (->> (parser/infer-parsers nyc-311-file
                                          {}
                                          {:include-cols cols})
                    (into #{} (map :column-name)))
               colnames))))
    (testing (str "all columns to be included, specified by name keywordised using "
                  "function given in `:colname->ident` option, and no others, are parsed")
      (let [colname->ident #(-> (string/replace % #"\s+" "")
                                keyword)
            include-cols (into #{} (map colname->ident) colnames)]
        (is (= (->> (parser/infer-parsers nyc-311-file
                                          {}
                                          {:include-cols include-cols
                                           :colname->ident colname->ident})
                    (into #{} (map :column-name)))
               colnames))))))


(deftest local-datetime
  (let [dt :local-date-time
        opts {:include-cols #{1 2}}]
    (testing "`:local-date-time` type correctly inferred"
      (is (every? #(= :local-date-time (:parser-dtype %))
                  (parser/infer-parsers nyc-311-file {} opts))))
    (testing "`:local-date-time` type correctly applied when specified"
      (doseq [p (parser/infer-parsers nyc-311-file
                                      {1 dt
                                       2 dt}
                                      opts)]
        (is (= (:parser-dtype p) dt))
        (is (= (count (set (:failed-indexes p)))
               0))))))


(deftest vector-valued-col-type-inference
  (let [vector-str "(40.561217790392675,-74.10280072030771)"
        vector-val [40.561217790392675 -74.10280072030771]
        colname "Location"
        include-cols {:include-cols #{colname}}
        options (merge include-cols
                       {:vector-open-char \(
                        :vector-close-char \)})
        tokenise (fn [s]
                   (let [len (count s)]
                     (-> (subs s 1 (- len 1))
                         (string/split #","))))
        double-dt :db.type/double
        test-vector-parser-inference
        (fn [parser parser-dt expected-val]
          (let [elmt-parsers (:field-parser-data parser)]
            (is (= (:parser-dtype parser) :vector))
            (is (some? elmt-parsers))
            (is (= (count elmt-parsers) 2))
            (is (every? #(= (:parser-dtype %) parser-dt)
                        elmt-parsers))
            (is (= (mapv #((:parse-fn %1) %2)
                         elmt-parsers
                         (tokenise vector-str))
                   expected-val))))]
    (testing "vector-valued column with `db.type/tuple` type specified and custom parser"
      (let [parse-fn (fn [s]
                       (map #(Double/parseDouble %)
                            (tokenise s)))
            schema {colname [:db.type/tuple parse-fn]}
            parser (-> (parser/infer-parsers nyc-311-file schema include-cols)
                       first)]
        (is (= (:parser-dtype parser) :db.type/tuple))
        (is (= ((:parse-fn parser) vector-str)
               vector-val))))
    (testing "Type inference for vector-valued (as opposed to scalar-valued) column"
      (-> (first (parser/infer-parsers nyc-311-file {} options))
          (test-vector-parser-inference double-dt vector-val)))
    (testing "User-specified vector element types handled correctly"
      (-> (first (parser/infer-parsers nyc-311-file
                                       {colname [[double-dt double-dt]]}
                                       options))
          (test-vector-parser-inference double-dt vector-val)))
    (testing "User-specified vector element types and custom parsers handled correctly"
      (let [dt :db.type/bigint
            pfn #(int (Double/parseDouble %))
            exp-val (map pfn (tokenise vector-str))]
        (-> (first (parser/infer-parsers nyc-311-file
                                         {colname [[dt dt]
                                                   [pfn pfn]]}
                                         options))
            (test-vector-parser-inference dt exp-val))))))


(deftest headerless-columns
  (let [expected-types {2 :db.type/double
                        3 :db.type/long}
        subvec-range (range 2 4)
        test-fn (fn [opts]
                  (let [parsers (parser/infer-parsers shapes-modified-file {} opts)
                        idx->colname (parser/col-idx->col-name-fn opts)]
                    (doseq [i subvec-range]
                      (let [p (nth parsers i)]
                        (is (= (:column-name p)
                               (idx->colname i)))
                        (is (= (:parser-dtype p)
                               (expected-types i)))))))]
    (testing (str "default column names are used if `:idx->colname` option not specified, "
                  "and type inference is handled as usual")
      (test-fn {}))
    (testing (str "expected column names are used if `:idx->colname` option specified, "
                  "and type inference is handled as usual")
      (test-fn {:idx->colname {2 "shape/pt-lon"}}))))


(deftest no-header-row
  (testing "Parser inference works without header row"
    (let [options {:header-row? false}
          parsers (-> (io/file data-folder "routes-no-header.csv")
                      (parser/infer-parsers {} options))
          idx->colname (parser/col-idx->col-name-fn {})
          expected-dtypes {0 :db.type/string
                           1 :db.type/long
                           2 :db.type/string
                           3 nil
                           4 :db.type/long
                           5 nil
                           6 nil
                           7 nil}]
      (doseq [i (range (count parsers))]
        (let [p (nth parsers i)]
          (is (= (:column-name p) (idx->colname i)))
          (is (= (:parser-dtype p) (expected-dtypes i))))))))
