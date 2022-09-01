(ns build
  (:refer-clojure :exclude [test])
  (:require [borkdude.gh-release-artifact :as gh]
            [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd])
  (:import (clojure.lang ExceptionInfo)))

(def lib 'io.replikativ/datahike-csv-loader)
(def version (format "0.1.%s" (b/git-count-revs nil)))
(def current-commit (gh/current-commit))
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (b/write-pom {:class-dir class-dir
                :src-pom "./template/pom.xml"
                :lib lib
                :version version
                :basis basis
                :src-dirs ["src"]})
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))

(defn install
  [_]
  (clean nil)
  (jar nil)
  (b/install {:basis (b/create-basis {})
              :lib lib
              :version version
              :jar-file jar-file
              :class-dir class-dir}))

(defn release [_]
  (-> (try (gh/overwrite-asset {:org "replikativ"
                                :repo (name lib)
                                :tag version
                                :commit current-commit
                                :file jar-file
                                :content-type "application/java-archive"})
           (catch ExceptionInfo e
             (assoc (ex-data e) :failure? true)))
      :url
      println))

(defn deploy "Don't forget to set CLOJARS_USERNAME and CLOJARS_PASSWORD env vars." [_]
  (dd/deploy {:installer :remote
              :artifact jar-file
              :pom-file (b/pom-path {:lib lib :class-dir class-dir})}))
