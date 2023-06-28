(ns data.parquet-test.utils)

(import java.nio.file.Files)
(import java.nio.file.Paths)

(deftype TempFile [path]
  java.io.Closeable
  (close [_this]
    (Files/deleteIfExists path)))

(defn temp-file [path]
  (TempFile. (Paths/get path (into-array String []))))
