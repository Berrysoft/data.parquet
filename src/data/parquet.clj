(ns data.parquet
  (:gen-class))

(import data.ParquetNative)
(import data.ParquetColumnIterator)

(deftype ParquetColumn [reader key]
  clojure.lang.IFn
  (invoke [this]
    (.seq this))

  clojure.lang.Seqable
  (seq [_this]
    (with-open [col (ParquetColumnIterator. (ParquetNative/getColumn reader (name key)))]
      (flatten (map #(seq %) (iterator-seq col))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defprotocol IParquetFile
  (getColumns [this])
  (getColumn [this k]))

(deftype ParquetFile [reader]
  IParquetFile
  (getColumns [_this]
    (map #(keyword %) (ParquetNative/getColumns reader)))
  (getColumn [_this k]
    (ParquetColumn. reader k))

  java.io.Closeable
  (close [_this]
    (ParquetNative/closeReader reader))

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
  (ParquetFile. (ParquetNative/openReader path)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defprotocol IParquetWriter
  (add [this row]))

(deftype ParquetWriter [writer]
  IParquetWriter
  (add [_this row]
    (ParquetNative/writeRow writer row))

  java.io.Closeable
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
      (.put jmap (name key) (class value)))
    jmap))

(defn save-parquet [path data]
  (let [f (first data)]
    (with-open [writer (ParquetWriter. (ParquetNative/openWriter path (java-class-map f)))]
      (doseq [row data]
        (.add writer (java-map row))))))
