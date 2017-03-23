# Try Couchbase



## Usage

```clojure
(:require [try-cb-clj.core :refer :all])
```

## 1. Connect to server

**connect to localhost**

```clojure
(def cluster (connect "couchbase://localhost"))
```

**connect to multiple server**

```clojure
(def cluster (connect "couchbase://10.1.1.1,10.1.1.2"))
```

## 2. open bucket


```clojure
(def bucket (open-bucket cluster "myblog"))
```

**open bucket with password**

```clojure
(def bucket (open-bucket cluster "myblog" "your password"))
```

## 3. Manuplate Document

### Insert

**insert map**

```clojure
;; blog-id is document id that must be a unique name from your bucket
;; if omit document id then uuid will be used instead of
(insert! bucket "blog-id" {:title "Hello first blogging!"
                           :content "Hi there bla bla bla..."
                           :create_on (java.util.Date.)})
```

and returns values
it has always id, cas, value

```clojure
{:value  {:title "Hello first blogging!"
          :content "Hi there bla bla bla..."
          :create_on 149409102932}
 :id "blog-id"
 :cas 12309203920}
```

**insert array**

```clojure
(insert! bucket "items" [ 1 2 3 4 5])
```

### getting a document


with document id can get a document,
if document not found you get a nill

```clojure
(get! "blog-id")
```

do not want to dealing with nil?
you can find out

```clojure
(.exists bucket "abcd")
true
```

if document found
and returns

```clojure
{:value  {:title "Hello first blogging!"
          :content "Hi there bla bla bla..."
          :create_on 149409102932}
 :id "blog-id"
 :cas 12309203920}
```

document like array or long types
you must use get-as-[type]
for example

```clojure

(get-as-array bucket "items")

(get-as-long bucket "order::id")
```


### Upsert/Replace

```clojure
(upsert! bucket "abcd" {:items [1 2 3 4 5]})

(replace! bucket "abcd" {})

```

### Remove

return true of false

```clojure
(remove! bucket "abcd")
```

### 4. Atomic operation

**creating counter**

```clojure

;; create counter
(counter bucket "order::id" 1 1)
```

and returns

```clojure
{:value 1
 :id "order::id"
 :cas 12323232 }
```

**get counter**

```clojure
(get-as-long bucket "order::id")
```


### 5. Query

**simple query**

```clojure
(query bucket ["select * from myblog"])
```

**also supports parameters**

```clojure
(query bucket ["select * from myblog where user_id=$1 and email=$2" "abc" "clonekim@gmail.com"])
```

```clojure
(query bucket ["select * from myblog where user_id=$user_id and age=$age"  {:user-id "abc" :age 12}])
```
query returns

```clojure
({:myblog  {:name "kim"}}
           {:myblog  {:name "kim", :email "clonekim@gmail.com"}}
           {:myblog  {:user_id "user::1", :name "kim", :gender "male"}})
```

**support metrics**

it's very helpfull to pagination

```clojure
(query bucket ["select * from myblog"] {:with-metrics true})
```

and returns with metrics

```clojure
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

 single! or first! , last!
you feel async query is much like sync operation

```clojure
(defn user-add [doc]
  (async-bucket [bc bucket]
    (-> (counter bc "user::id" 1 1)
      (to-map)
      (to-flat (fn [id]
                 (let [user-id (str "user::" id)]
                   (insert! bc user-id (assoc doc :user_id user-id)))))
      (single!))))`

(user-add {:name "kim" :gender "male"})
```

and returns

```clojure

{:value {:user_id "user::1"
         :name "kim"
         :gender "male"}
 :cas "1479118072773672960"
 :id "user::1"}
```


you can run async query to be blocked explicitly

```clojure
(let [result (async-bucket [bc bucket]
              (-> (query bc ["select * from users limit 1"] {:block true})))]

   (println result))
```
can also 

```clojure
(let [result (async-bucket [bc bucket]
              (-> (query bc ["select * from users limit 1"])
                  (to-map)
                  (to-flat (fn [x]
                           ;; your code here))]

   (println result)) ;; rx.Observable
```



find and insert or get
or You can use upsert method

```clojure
(async-bucket [bc bucket]
  (-> (.exists bc "blog12")
      (to-flat (fn [found]
                (if-not found
                   (insert! bc "blog12" {:test "ok"})
	               (get! bc "blog12"))))
      (single!)))

```


## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
