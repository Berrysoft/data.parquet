(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'berrysoft/data.parquet)
(def version (format "0.1.0-%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn generate [_]
  (b/javac {:src-dirs ["src-java"]
            :class-dir class-dir
            :javac-opts ["-h" "target/jni"]}))

(def cargo-command
  ["cargo" "build"])

(defn- jar-opt [rel]
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})
  (generate nil)
  (b/process {:command-args
              (if rel
                (conj cargo-command "--release")
                cargo-command)})
  (b/compile-clj {:basis basis
                  :src-dirs ["src"]
                  :class-dir class-dir}))

(defn jar-debug [_]
  (jar-opt false))

(defn jar-release [_]
  (jar-opt true))
