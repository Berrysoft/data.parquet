(ns data.parquet
  (:gen-class))

(import data.ParquetNative)

(defn open
  [path]
  (ParquetNative/open path))
