(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'com.berrysoft/berrysoft.data.parquet)
(def version (format "0.1.0-%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn generate [_]
  (b/javac {:src-dirs ["src-java"]
            :class-dir class-dir
            :basis basis
            :javac-opts ["-h" "target/jni"]}))

(def cargo-command
  ["cargo" "build"])

(defn cargo-compile [rel]
  (b/process {:command-args
              (if rel
                (conj cargo-command "--release")
                cargo-command)}))

(defn- compile-opt [rel]
  (generate nil)
  (cargo-compile rel))

(defn compile-debug [_]
  (compile-opt false))

(defn compile-release [_]
  (compile-opt true))

(defn jar [_]
  (generate nil)
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :scm {:tag (str "v" version)}
                :basis basis
                :src-dirs ["src"]})
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))
