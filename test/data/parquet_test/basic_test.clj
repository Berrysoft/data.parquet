(ns data.parquet-test.basic-test
  (:require [data.parquet :as pq]
            [clojure.test :refer [deftest is testing]]
            [data.parquet-test.utils :as utils]))

(deftest basic-io
  (testing "Test save and open."
    (with-open [_tf (utils/temp-file "test.pq")]
      (pq/save-parquet
       "test.pq"
       [{:a 1 :b true :c 0.1}
        {:a 2 :b true :c 0.2}
        {:a 3 :b false :c 0.3}])
      (with-open [f (pq/open-parquet "test.pq")]
        (is (= [:a :b :c] (keys f)))
        (is (= [1 2 3] (seq (:a f))))
        (is (= [true true false] (seq (:b f))))
        (is (= [0.1 0.2 0.3] (seq (:c f))))))))
