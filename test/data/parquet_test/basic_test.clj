(ns data.parquet-test.basic-test
  (:require [data.parquet :as pq]
            [clojure.test :refer [deftest is testing]]
            [data.parquet-test.utils :as utils]))

(def basic-path "test.pq")

(deftest basic-io
  (testing "Test save and open."
    (with-open [_tf (utils/temp-file basic-path)]
      (pq/save-parquet
       basic-path
       [{:a 1 :b true :c 0.1}
        {:a 2 :b true :c 0.2}
        {:a 3 :b false :c 0.3}])
      (with-open [f (pq/open-parquet basic-path)]
        (is (= [:a :b :c] (keys f)))
        (is (= [1 2 3] (seq (:a f))))
        (is (= [true true false] (seq (:b f))))
        (is (= [0.1 0.2 0.3] (seq (:c f))))))))

(def batch-path "test.batch.pq")

(deftest batch-io
  (testing "Test batched save and open."
    (with-open [_tf (utils/temp-file batch-path)]
      (pq/save-parquet
       batch-path
       {:a [1 2 3]
        :b [true true false]
        :c [0.1 0.2 0.3]})
      (with-open [f (pq/open-parquet batch-path)]
        (is (= [:a :b :c] (keys f)))
        (is (= [1 2 3] (seq (:a f))))
        (is (= [true true false] (seq (:b f))))
        (is (= [0.1 0.2 0.3] (seq (:c f))))))))
