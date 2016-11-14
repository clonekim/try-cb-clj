(ns try-cb-clj.core-test
  (:require [clojure.test :refer :all]
            [try-cb-clj.core :refer :all]))



(def cluster1 (connect "couchbase://localhost"))

(def ec (open-bucket cluster1 "ecommerce"))


(defn user-add [doc]
  (async-bucket [bc ec]
    (-> (counter bc "user::id" 1 1)
      (to-map)
      (to-flat (fn [id]
                 (let [user-id (str "user::" id)]
                   (insert! bc user-id (assoc doc :user_id user-id)))))
      (single!))))

(defn user-all []
  (query ec "select * from ecommerce"))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 0 1))))
