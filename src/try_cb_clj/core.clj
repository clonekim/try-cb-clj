(ns try-cb-clj.core

  (:require [clojure.tools.logging :as log])

  (:import [com.couchbase.client.java CouchbaseCluster Bucket]
           [com.couchbase.client.java.query N1qlQuery N1qlMetrics]
           [com.couchbase.client.java.document
            JsonDocument
            JsonArrayDocument
            JsonLongDocument
            JsonBooleanDocument
            StringDocument]
           [com.couchbase.client.java.env DefaultCouchbaseEnvironment]
           [com.couchbase.client.java.document.json JsonObject JsonArray JsonNull]
           [rx Observer Observable Subscriber]
           [java.util.concurrent TimeUnit]))


(defn connect [^String str]
  (log/info "Connecting..." str)
  (CouchbaseCluster/fromConnectionString str))


(defn disconnect [^CouchbaseCluster cluster]
  (log/info "Disconnecting..." str)
  (.disconnect cluster))


(defn open-bucket [^CouchbaseCluster cluster name]
  (log/info "Openning bucket.." name)
  (let [bucket (.openBucket cluster name)]
    (when bucket
      (log/info "Builing Primary Index for..." name)
      (-> bucket
        (.bucketManager)
        (.createN1qlPrimaryIndex true false)))
    bucket))


;;; 프로토콜 정의
;;; 클로저에서 자바로

(defprotocol ClojureToJava
  (->java [o]))

(extend-protocol ClojureToJava
  clojure.lang.IPersistentMap
  (->java [o]
    (reduce (fn [jo [k v]]
              (.put jo (name k) (->java v)))
      (JsonObject/empty) o))


  clojure.lang.IPersistentVector
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

(defn- to-java [o]
  (->java o))


;;; 프로토콜 정의
;;; 자바에서 클로저로

(defprotocol JavaToClojure
  (->clj [o]))

(extend-protocol JavaToClojure
  JsonDocument
  (->clj [o]
    {:value (->clj (.content o))
     :cas (str (.cas o))
     :id (.id o)})

  JsonLongDocument
  (->clj [o]
    {:value (.content o)
     :cas (.cas o)
     :id (.id o)})

  JsonArrayDocument
  (->clj [o]
    {:value (->clj (.content o))
     :cas (.cas o)
     :id (.id o)})

  JsonArray
  (->clj [o]
    (vec (map ->clj (.toList o))))

  JsonObject
  (->clj [o]>
    (reduce (fn [m k]
              (assoc m (keyword k) (->clj (.get o k))))
      {} (.getNames o)))

  java.lang.Object
  (->clj [o] o)

  nil
  (->clj [o] nil))

(defn- to-clj [o]
  (->clj o))


;;; 프로토콜 정의
;;; Clojure -> Bucket


(defprotocol IBucket
  (create-doc [this id cas])
  (get-doc    [this bucket args]))


(extend-protocol IBucket

  ;; get 추상화
  java.lang.Object
  (get-doc [this bucket args]
    (let [t (first args)]
      (case t
        :long (to-clj (.get bucket this JsonLongDocument))
        :array (to-clj (.get bucket this JsonArrayDocument))
        (to-clj (.get bucket this)))))


  ;; create document
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


;;; couchbase 매크로
;;; async bucket

(defmacro async-bucket [binding & body]
  ;; (async-bucket [bc *bucket*]
  ;;   (-> (counter bc "user::id" 1 1)
  ;;        (to-map)
  ;;        ...
  `(let [~(first binding) (.async ~(second binding))]
     (do ~@body)))


;;; couchbase 메서드

(defn insert! [bucket id doc]
  (->> (create-doc doc id nil)
    (.insert bucket)
    to-clj))


(defn update! [bucket id doc]
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
  ;; JsonLongDocument로 저장된 경우
  ;; 예) (get! *bucket* "hello2" :long)
  ;; 없을 경우 JsonDocument로 가져온다
  (get-doc doc bucket args))


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
               (.content doc))))))


  ([^Observable ob caller]
   (-> ob
     (.map (reify rx.functions.Func1
             (call [this doc]
               (caller doc)))))))


(defn flat [^Observable ob caller]
  (-> ob
    (.flatMap (reify rx.functions.Func1
                (call [this doc]
                  (caller doc))))))



(defn single!
  ([^Observable ob]
   (-> ob
     (.timeout 1 TimeUnit/SECONDS)
     (.toBlocking)
     (.single))))



(defn first!
  ([^Observable ob]
   (-> ob
     (.timeout 1 TimeUnit/SECONDS)
     (.toBlocking)
     (.first))))


(defn query
  ([bucket str]
   (let [result (->> (N1qlQuery/simple str)
                  (.query bucket))]

     {:requestId (.requestId result)
      :errors (to-clj (.errors result))
      :status (.status result)
      :metrics (to-clj (.asJsonObject
                         (.info result)))
      :results (for [x (.allRows result)]
                 (to-clj (.value x)))})))


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
