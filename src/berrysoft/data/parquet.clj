(ns berrysoft.data.parquet
  (:gen-class))

(import berrysoft.data.ParquetNative)
(import berrysoft.data.ParquetColumnSeq)

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defprotocol IParquetFile
  (getColumns [this])
  (getColumn [this k]))

(deftype ParquetFile [reader ^:volatile-mutable cols]
  IParquetFile
  (getColumns [_this]
    (map #(keyword %) (ParquetNative/getColumns reader)))
  (getColumn [_this k]
    (let [col (ParquetNative/getColumn reader (name k))]
      (set! cols (conj cols col))
      (flatten (map #(seq %) (ParquetColumnSeq. col)))))

  java.lang.AutoCloseable
  (close [_this]
    (ParquetNative/closeReader reader)
    (doseq [col cols]
      (ParquetNative/closeColumn col)))

  clojure.lang.Associative
  (containsKey [this k]
    (.contains (.getColumns this) k))
  (entryAt [this k]
    (.valAt this k))

  clojure.lang.IFn
  (invoke [this k]
    (.valAt this k))

  clojure.lang.ILookup
  (valAt [this k]
    (.getColumn this k))

  clojure.lang.IMapIterable
  (keyIterator [this]
    (.iterator (.getColumns this)))
  (valIterator [this]
    (.iterator (map #(.valAt this %) (.getColumns this))))

  clojure.lang.Seqable
  (seq [this]
    (map #(clojure.lang.MapEntry. % (.valAt this %)) (.getColumns this)))

  java.lang.Iterable
  (iterator [this]
    (.iterator (.seq this)))

  java.lang.Object
  (toString [this]
    (str (.getColumns this))))

(defn open-parquet [path]
  (ParquetFile. (ParquetNative/openReader path) []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defprotocol IParquetWriter
  (add [this row]))

(deftype ParquetWriter [writer]
  IParquetWriter
  (add [_this row]
    (ParquetNative/writeRow writer row))

  java.lang.AutoCloseable
  (close [_this]
    (ParquetNative/closeWriter writer)))

(defn- java-map [m]
  (let [jmap (java.util.HashMap.)]
    (doseq [[key value] m]
      (.put jmap (name key) value))
    jmap))

(defn- java-class-map [m]
  (let [jmap (java.util.HashMap.)]
    (doseq [[key value] m]
      (.put
       jmap (name key)
       (cond
         (seq? value) (class (first value))
         (vector? value) (class (first value))
         :else (class value))))
    jmap))

(defn save-parquet [path data]
  (let [seq-data (if (map? data) [data] data)
        f (first seq-data)]
    (assert (map? f))
    (with-open [writer (ParquetWriter. (ParquetNative/openWriter path (java-class-map f)))]
      (doseq [row seq-data]
        (.add writer (java-map row))))))
