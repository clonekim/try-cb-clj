(ns try-cb-clj.core

  (:require [clojure.tools.logging :as log])

  (:import [com.couchbase.client.java CouchbaseCluster Bucket]
           [com.couchbase.client.java.query 
            N1qlQuery 
            N1qlQueryResult 
            AsyncN1qlQueryResult 
            AsyncN1qlQueryRow]
           [com.couchbase.client.java.document
            JsonDocument
            JsonArrayDocument
            JsonLongDocument
            JsonBooleanDocument]
           [com.couchbase.client.java.auth ClassicAuthenticator]
           [com.couchbase.client.java.env DefaultCouchbaseEnvironment]
           [com.couchbase.client.java.document.json JsonObject JsonArray JsonNull]
           [rx Observer Observable Subscriber]
           [java.util.concurrent TimeUnit]))


(defn connect [^String str]
  (CouchbaseCluster/fromConnectionString str))


(defn disconnect [^CouchbaseCluster cluster]
  (.disconnect cluster))


(defn open-bucket
  ([^Bucket bucket]
   (when bucket
     (-> bucket
         (.bucketManager)
         (.createN1qlPrimaryIndex true false))
     bucket))

  ([^CouchbaseCluster cluster name]
   (open-bucket (.openBucket cluster name)))

  ([^CouchbaseCluster cluster name password]
   (open-bucket
    (-> cluster
        (.authenticate (-> (ClassicAuthenticator.)
                           (.bucket name password)))
        (.openBucket name password)))))



(defprotocol ClojureToJava
  "클로저 자료형을 자바자료형으로
  변환한다"
  (->java [o]))

(extend-protocol ClojureToJava
  clojure.lang.IPersistentMap
  (->java [o]
    (reduce (fn [jo [k v]]
              (.put jo (name k) (->java v)))
      (JsonObject/empty) o))


  clojure.lang.IPersistentCollection
  (->java [o]
    (reduce (fn [arry v]
              (.add arry (->java v)))
      (JsonArray/empty) o))


  java.util.Date
  (->java [o] (.getTime o))


  java.lang.String
  (->java [o]
    (let [s (-> o .trim)]
      (if (= 0 (.length s))
        JsonNull/INSTANCE
        s)))


  java.lang.Object
  (->java [o] o)

  nil
  (->java [o] JsonNull/INSTANCE))

(defn to-java [o]
  (->java o))


(defprotocol JavaToClojure
  "프로토콜 정의
  자바객체를 클로저 자료형으로 변환"
  (->clj [o]))

(extend-protocol JavaToClojure
  
  AsyncN1qlQueryRow
  (->clj [o]
    (->clj (.value o)))

  JsonDocument
  (->clj [o]
    {:value (->clj (.content o))
     :cas (str (.cas o))
     :id (.id o)})

  JsonLongDocument
  (->clj [o]
    {:value (.content o)
     :cas (str (.cas o))
     :id (.id o)})

  JsonArrayDocument
  (->clj [o]
    {:value (->clj (.content o))
     :cas (str (.cas o))
     :id (.id o)})

  JsonArray
  (->clj [o]
    (vec (map ->clj (.toList o))))

  JsonObject
  (->clj [o]>
    (reduce (fn [m k]
              (assoc m (keyword k) (->clj (.get o k))))
      {} (.getNames o)))


  java.util.Map
  (->clj [o]
    (reduce (fn [m [^String k v]]
              (assoc m (keyword k) (->clj v)))
            {} (.entrySet o)))

  java.util.List
  (->clj [o]
    (vec (map ->clj o)))


  java.lang.Object
  (->clj [o] o)

  nil
  (->clj [o] nil))

(defn to-clj [o]
  (->clj o))


(declare to-map
         to-flat
         single!
         first!)


(defprotocol IBucket
  "프로토콜 정의
  버킷에서 사용할 도큐먼트를 생성, 가져오기"
  (create-doc [this id cas])
  (get-doc    [this bucket args]))


(defprotocol IQuery
  "Defines Protocol
  convert query rows to sequence"
  (simple-query [this args]))


(defprotocol IMetric
  "Define Protocol
   get metrics infomations"
  (get-metrics [this]))



(extend-protocol IQuery
  rx.Observable
  (simple-query [this args]
    (let [is-block? (:block args false)
          with-metric? (:with-metric args false)
          rows (if is-block?
                 (for [i (-> this
                             (to-flat (fn [x] (.rows x)))
                             (.timeout 1 TimeUnit/SECONDS)
                             (.toBlocking)
                             (.getIterator)
                             (iterator-seq))]
                   (to-clj (.value i)))

                 (-> this
                     (to-flat (fn [x] (.rows x)))))]

      (if with-metric?
        (assoc (get-metrics this) :results rows)
        (if (and is-block? (= 1 (count rows)))
          (first rows)
          rows))))


  com.couchbase.client.java.query.N1qlQueryResult
  (simple-query [this args]
    (let [with-metric? (:with-metric args false)
          rows (for [x (.allRows this)]
                 (to-clj (.value x)))]

      (if with-metric?
        (assoc (get-metrics this) :results rows)
        (if (= 1 (count rows))
          (first rows)
          rows)))))


(extend-protocol IMetric

  rx.Observable
  (get-metrics [this]
    {:resultId (-> (to-map this (fn [x] (.requestId x)))
                   (single!))
     :errors (-> (to-flat this (fn [x] (.errors x)))
                 (single! []))
     :status (-> (to-flat this (fn [x] (.status x)))
                 (single!))
     :metrics (-> (to-flat this (fn [x] (.info x)))
                  (single!)
                  (.asJsonObject)
                  (to-clj))})

  com.couchbase.client.java.query.N1qlQueryResult
  (get-metrics [this]
    {:requestId (.requestId this)
     :errors (to-clj (.errors this))
     :status (.status this)
     :metrics (to-clj (.asJsonObject (.info this)))}))


(extend-protocol IBucket

  java.lang.Object
  (get-doc [this bucket args]
    (let [t (first args)]
      (case t
        :long (to-clj (.get bucket this JsonLongDocument))
        :array (to-clj (.get bucket this JsonArrayDocument))
        (to-clj (.get bucket this)))))


  clojure.lang.IPersistentMap
  (create-doc [this id cas]
    (let [content (to-java this)]
      (if (nil? cas)
        (JsonDocument/create id content)
        (JsonDocument/create id content cas))))


  clojure.lang.IPersistentVector
  (create-doc [this id cas]
    (let [content (to-java this)]
      (if (nil? cas)
        (JsonArrayDocument/create id content)
        (JsonArrayDocument/create id content cas))))


  java.lang.Long
  (create-doc [this id cas]
    (let [content (to-java this)]
      (if (nil? cas)
        (JsonLongDocument/create id content)
        (JsonLongDocument/create id content cas)))))



(defmacro async-bucket [binding & body]
  "couchbase 매크로
   async bucket를 사용하게 함
   예) (async-bucket [bc *bucket*]
         (-> (counter bc \"user::id\" 1 1)
             (to-map)))"

  `(let [~(first binding) (.async ~(second binding))]
     (do ~@body)))


;;; couchbase 메서드

(defn insert!

  ([bucket doc]
   (insert! bucket (.toString (java.util.UUID/randomUUID)) doc))

  ([bucket id doc]
   (->> (create-doc doc id nil)
        (.insert bucket)
        to-clj)))


(defn upsert! [bucket id doc]
  (->> (create-doc doc id nil)
    (.upsert bucket)
    to-clj))


(defn replace!
  ([bucket id doc]
   (->> (create-doc doc id nil)
     (.replace bucket)
     to-clj))

  ([bucket id doc cas]
   (->> (create-doc doc id cas)
     (.replace bucket)
     to-clj)))


(defn get! [bucket doc & args]
  "JsonLongDocument로 저장된 경우
   예) (get! *bucket* \"hello\" :long)
   없을 경우 JsonDocument로 가져온다"
  (get-doc doc bucket args))


(defn get-as-long [bucket doc]
  (get-doc doc bucket '(:long)))


(defn get-as-array [bucket doc]
  (get-doc doc bucket '(:array)))


(defn remove! [bucket id]
  (do
    (.remove bucket id)
    true))


(defn counter [bucket id a b]
  (-> (.counter bucket id a b)))


(defn to-map
  ([^Observable ob]
   (-> ob
     (.map (reify rx.functions.Func1
             (call [this doc]
               (-> doc
                   (to-clj)))))))


  ([^Observable ob caller]
   (-> ob
     (.map (reify rx.functions.Func1
             (call [this doc]
               (caller doc)))))))


(defn to-flat [^Observable ob caller]
  (-> ob
    (.flatMap (reify rx.functions.Func1
                (call [this doc]
                  (caller doc))))))



(defn single!
  ([^Observable ob val]
   (-> ob
     (.timeout 1 TimeUnit/SECONDS)
     (.toBlocking)
     (.singleOrDefault val)
     to-clj))

  ([^Observable ob]
   (single! ob nil)))



(defn first!
  ([^Observable ob]
   (-> ob
     (.timeout 1 TimeUnit/SECONDS)
     (.toBlocking)
     (.first)
     to-clj)))



(defn query
  ([bucket str & args]
   (let [result (->> (N1qlQuery/simple str)
                     (.query bucket))]
     (simple-query result (if-not (nil? args)
                            (first args)
                            {:with-metric false})))))


(defn subscribe
  ([^Observable ob]
   (.subscribe ob))


  ([^Observable ob & args]
   (let [l (apply hash-map args)
         on-completed (:on-completed l)
         on-error (:on-error l)
         on-next (:on-next l)]

     (.subscribe ob
       (proxy [Subscriber] []
         (onCompleted []
           (log/debug "completed!...")
           (when (fn? on-completed)
             (on-completed)))

         (onError [throwable]
           (log/error "error ..." throwable)
           (if (fn? on-error)
             (on-error throwable)
             (throw throwable)))

         (onNext [o]
           (log/debug "next ..." o)
           (when (fn? on-next)
             (on-next o))))))))
