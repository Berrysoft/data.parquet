(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'clojure-jni/clojure-jni.core)
(def version (format "0.1.0-%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn generate [_]
  (b/javac {:src-dirs ["src-java"]
            :class-dir class-dir
            :javac-opts ["-h" "target/jni"]}))

(defn jar [_]
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})
  (generate nil)
  (b/process {:command-args ["cargo" "build"]})
  (b/compile-clj {:basis basis
                  :src-dirs ["src"]
                  :class-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))
