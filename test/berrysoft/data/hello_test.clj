(ns berrysoft.data.hello-test
  (:require [berrysoft.data.parquet :as pq]
            [clojure.test :refer [deftest is testing]]))

(deftest hello-test
  (testing "Test basic JNI."
    (is (= (pq/hello) "Hello world!"))))
