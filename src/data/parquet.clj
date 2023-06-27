(ns data.parquet
  (:gen-class))

(import data.ParquetNative)

(defprotocol IParquetFile
  (getColumns [this])
  (getColumn [this k])
  (close [this]))

(deftype ParquetFile [reader]
  IParquetFile
  (getColumns [this]
    (map #(keyword %) (ParquetNative/getColumns reader)))
  (getColumn [this k]
    (flatten (map #(seq %) (ParquetNative/getColumn reader (name k)))))
  (close [this]
    (ParquetNative/close reader))

  clojure.lang.Associative
  (containsKey [this k]
    (.contains (.getColumns this) k))

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

  java.lang.Object
  (toString [this]
    (str (.getColumns this))))

(defn open-parquet [path]
  (ParquetFile. (ParquetNative/open path)))
