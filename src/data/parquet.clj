(ns data.parquet
  (:gen-class))

(import data.ParquetNative)

(defn hello
  []
  (ParquetNative/hello))
