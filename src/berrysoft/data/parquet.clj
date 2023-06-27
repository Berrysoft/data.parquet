(ns berrysoft.data.parquet
  (:gen-class))

(import berrysoft.data.ParquetNative)

(defn hello
  []
  (ParquetNative/hello))
