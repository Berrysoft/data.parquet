# berrysoft.data.parquet

This is an experimental package for Clojure.
It is used for Parquet IO, and the backend is written by Rust through JNI.

I choose to use Rust because I'm not able to understand the designation of Arrow package for Java. Fortunately I can understand the Parquet package for Rust easily.

## Usage
``` clojure
(require '[berrysoft.data.parquet :as pq])

;; Write 3 rows with keys :a :b :c.
(pq/save-parquet
 "test.parquet"
 [{:a 1 :b true :c [0.1]}
  {:a [2 3]
   :b [true false]
   :c [0.2 0.3]}])

;; Load the file lazily.
;; The file should be closed to free the allocated memory.
(with-open [f (pq/open-parquet "test.parquet")]
  (assert (= [:a :b :c] (keys f)))
  ;; The column is loaded as a lazy seq.
  ;; You need (seq) to evaluate it.
  (assert (= [1 2 3] (seq (:a f))))
  (assert (= [true true false] (seq (:b f))))
  (assert (= [0.1 0.2 0.3] (seq (:c f)))))
```
