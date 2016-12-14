Try Couchbase
===============

Simpliy do use couchbase

## Usage

```clojure

(:require [try-cb-clj.core :refer :all])

;; connect server
(def cluster (connect "couchbase://localhost"))

;; open bucket
(def bucket (open-bucket cluster "myblog"))

;; open bucket with password
(def bucket (open-bucket cluster "myblog" "*****"))

;; insert
(insert! bucket "blog-id" {:title "Hello first blogging!"
                           :content "Hi there bla bla bla..."
                           :create_on (java.util.Date.)})
;; and returns
{:value  {:title "Hello first blogging!"
          :content "Hi there bla bla bla..."
          :create_on 149409102932}
 :id "blog-id"
 :cas 12309203920}

;;insert array
(insert! bucket "items" [ 1 2 3 4 5])

;;and get as array type
(get-as-array bucket "items")

;; counter
(counter bucket "order::id" 1 1)

;; get counter
(get! bucket "order::id" :long) 
;; or
(get-as-long bucket "order::id")

;; upsert/replace
(upsert! bucket "abcd" {:items [1 2 3 4 5]})

(replace! bucket "abcd" {})

;; delete
(delete! bucket "abcd")

;; exist
(.exists bucket "abcd")

;; get
(get! bucket "abcd")


;; query
(query bucket "select * from myblog")

;; and returns
({:myblog  {:name "kim"}}
           {:myblog  {:name "kim", :email "clonekim@gmail.com"}}
           {:myblog  {:user_id "user::1", :name "kim", :gender "male"}})

;; query with metrics
(query bucket "select * from myblog" {:with-metrics true})

;; and returns with metrics
{:requestId "2dbaae07-0877-45de-89b5-8a2b334921c6",
  :errors [],
  :status "success",
  :metrics {:executionTime "26.509444ms",
             :resultCount 3,
             :resultSize 583,
             :elapsedTime "26.536883ms"},
 :results ({:myblog  {:name "kim"}}
           {:myblog  {:name "kim", :email "clonekim@gmail.com"}}
           {:myblog  {:user_id "user::1", :name "kim", :gender "male"}})}

```

## Async Bucket

```clojure
(defn user-add [doc]
  (async-bucket [bc bucket]
    (-> (counter bc "user::id" 1 1)
      (to-map)
      (to-flat (fn [id]
                 (let [user-id (str "user::" id)]
                   (insert! bc user-id (assoc doc :user_id user-id)))))
      (single!))))


(user-add {:name "kim" :gender "male"})
;; and returns
{:value {:user_id "user::1"
         :name "kim"
         :gender "male"}
 :cas "1479118072773672960"
 :id "user::1"}



;; if you need to query to be blocked
(let [result (async-bucket [bc bucket]
              (-> (query bc "select * from users limit 1" {:block true})))]

   (println result))


;; else
(let [result (async-bucket [bc bucket]
              (-> (query bc "select * from users limit 1")
                  (to-map)
                  (to-flat (fn [x]
                           ;; your code here))]

   (println result)) ;; rx.Observable




;; find and insert or get
;; or You can use upsert method
(async-bucket [bc bucket]
  (-> (.exists bc "blog12")
      (to-flat (fn [found]
                (if-not found
                   (insert! bc "blog12" {:test "ok"})
	               (get! bc "blog12"))))
      (single!)))

;;and returns blog12

```


## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
