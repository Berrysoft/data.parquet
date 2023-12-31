(ns berrysoft.data.parquet-test.basic-test
  (:require [berrysoft.data.parquet :as pq]
            [clojure.test :refer [deftest is testing]]
            [berrysoft.data.parquet-test.utils :as utils]))

(def basic-path "test.pq")

(deftest basic-io
  (testing "Test IO."
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

(def skey-path "test.skey.pq")

(deftest skey-io
  (testing "Test string key IO."
    (with-open [_tf (utils/temp-file skey-path)]
      (pq/save-parquet
       skey-path
       {"a" [1 2 3]
        "b" [true true false]
        "c" [0.1 0.2 0.3]})
      (with-open [f (pq/open-parquet skey-path)]
        (is (= [:a :b :c] (keys f)))
        (is (= [1 2 3] (seq (:a f))))
        (is (= [true true false] (seq (:b f))))
        (is (= [0.1 0.2 0.3] (seq (:c f))))))))

(def batch-path "test.batch.pq")

(deftest batch-io
  (testing "Test batched IO."
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

(def mbatch-path "test.mbatch.pq")

(deftest mbatch-io
  (testing "Test mixed batched IO."
    (with-open [_tf (utils/temp-file mbatch-path)]
      (pq/save-parquet
       mbatch-path
       [{:a 1 :b true :c [0.1]}
        {:a [2 3]
         :b [true false]
         :c [0.2 0.3]}])
      (with-open [f (pq/open-parquet mbatch-path)]
        (is (= [:a :b :c] (keys f)))
        (is (= [1 2 3] (seq (:a f))))
        (is (= [true true false] (seq (:b f))))
        (is (= [0.1 0.2 0.3] (seq (:c f))))))))
