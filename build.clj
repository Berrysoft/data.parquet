(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.java.io :as io]))

(def lib 'io.github.berrysoft/berrysoft.data.parquet)
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

(defn- native-folder [rel]
  (str
   (.getCanonicalPath (io/as-file b/*project-root*))
   "/target/"
   (if rel "release" "debug")))

(defn cargo-compile [rel]
  (b/process {:command-args
              (if rel
                (conj cargo-command "--release")
                cargo-command)})
  (println "Compiled native lib to" (native-folder rel))
  (println "Set this path to LD_LIBRARY_PATH or java.library.path to allow java find the native library."))

(defn- compile-opt [rel]
  (generate nil)
  (cargo-compile rel))

(defn compile [_]
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

(defn install [_]
  (jar nil)
  (b/install {:basis basis
              :lib lib
              :version version
              :jar-file jar-file
              :class-dir class-dir}))
