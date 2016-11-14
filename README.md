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

;; insert
(insert! bucket "blog-id" {:title "Hello first blogging!"
                           :content "Hi there bla bla bla..."
                           :create_on (java.util.Date.)})
;; and returns
{:value 
        {:title "Hello first blogging!"
         :content "Hi there bla bla bla..."
         :create_on 149409102932}
 :id "blog-id" :cas 12309203920}


;; update/replace
(update! bucket "abcd" {:items [1 2 3 4 5]})

;; delete
(delete! bucket "abcd")

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
{:value {:user_id "user::1", :name "kim", :gender "male"}, :cas "1479118072773672960", :id "user::1"}


;; query
(query bucket "select * from myblog")

;; and returns

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

## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
