(ns try-cb-clj.core

  (:import [com.couchbase.client.java CouchbaseCluster]
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
           [java.util.concurrent TimeUnit])))



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
    (reduce (fn [ja v]
              (.add ja (->java v)))
      (JsonArray/empty) o))

  java.util.Date
  (->java [o] (.getTime o))

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
    (->clj
      {:content (->clj (.content o))
       :cas (str (.cas o))
       :id (.id o)}))

  JsonObject
  (->clj [o]>
    (reduce (fn [m k]
              (assoc m (keyword k) (->clj (.get o k))))
      {} (.getNames o)))

  JsonArray
  (->clj [o]
    (vec (map ->clj (.toList o))))

  JsonLongDocument
  (->clj [o]
    {:content (.content o)
     :cas (.cas o)
     :id (.id o)})

  JsonArrayDocument
  (->clj [o]
    {:content (->clj (.content o))
     :cas (.cas o)
     :id (.id o)})

  java.lang.Object
  (->clj [o] o)

  nil
  (->clj [o] nil))

(defn- to-clj [o]
  (->clj o))


;;; 프로토콜 정의
;;; Clojure -> Bucket


(defprotocol IBucket
  (->get    [this bucket args])
  (->insert [this bucket id]))


(extend-protocol IBucket

  ;; get 추상화
  java.lang.Object
  (->get [this bucket args]
    (let [t (first args)]
      (case t
        :long (to-clj (.get bucket this JsonLongDocument))
        :array (to-clj (.get bucket this JsonArrayDocument))
        (to-clj (.get bucket this)))))


  ;; insert 추상화
  clojure.lang.IPersistentMap
  (->insert [this bucket id]
    (->>
      (to-java this)
      (JsonDocument/create id)
      (.insert bucket)
      to-clj))


  clojure.lang.IPersistentVector
  (->insert [this bucket id]
    (->>
      (to-java this)
      (JsonArrayDocument/create id)
      (.insert bucket)
      to-clj))


  java.lang.Long
  (->insert [this bucket id]
    (->>
      (to-java this)
      (JsonLongDocument/create id)
      (.insert bucket)
      to-clj)))


;;; couchbase 매크로
;;; async bucket

(defmacro async-bucket [binding & body]
  ;; (async-bucket [bc *bucket*]
  ;;   (-> (counter bc "user::id" 1 1)
  ;;        (to-map)
  ;;        (to-flat #(
  `(let [~(first binding) (.async ~(second binding))]
     (do ~@body)))


;;; couchbase 메서드

(defn insert! [bucket id doc]
  ;; 벡터, 맵, 숫자를 저장할 수 있다
  (->insert doc bucket id))


(defn upsert! [bucket id doc]
  (->upsert doc bucket id))


(defn get! [bucket doc & args]
  ;; JsonLongDocument로 저장된 경우
  ;; 예) (get! *bucket* "hello2" :long)
  ;; 없을 경우 JsonDocument로 가져온다
  (->get doc bucket args))

(defn remove! [bucket id]
  (do
    (.remove bucket id)
    true))


(defn counter [bucket id a b]
  (-> (.counter bucket id a b)))


(defn to-map [^Observable ob caller]
  (-> ob
    (.map (reify rx.functions.Func1
            (call [this doc]
              (caller doc))))))


(defn flat [^Observable ob caller]
  (-> ob
    (.flatMap (reify rx.functions.Func1
                (call [this doc]
                  (caller doc))))))
