(ns berrysoft.data.parquet-test.rand-test
  (:require [berrysoft.data.parquet :as pq]
            [clojure.test :refer [deftest is testing]]
            [berrysoft.data.parquet-test.utils :as utils]))

(def rand-path "rand.pq")

(def rand-data-a (take 100000 (repeatedly #(rand 10))))

(def rand-data-b (take 100000 (repeatedly #(int (rand 10)))))

(deftest rand-io
  (testing "Test random data IO."
    (with-open [_tf (utils/temp-file rand-path)]
      (time
       (pq/save-parquet
        rand-path
        {:a rand-data-a
         :b rand-data-b}))
      (with-open [f (pq/open-parquet rand-path)]
        (is (= [:a :b] (keys f)))
        (is (= rand-data-a (seq (:a f))))
        (is (= rand-data-b (seq (:b f))))))))
