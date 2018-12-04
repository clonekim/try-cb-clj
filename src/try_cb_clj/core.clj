(ns try-cb-clj.core
  (:require [clojure.tools.logging :as log])

  (:import [com.couchbase.client.java CouchbaseCluster Bucket PersistTo ReplicateTo]
           [com.couchbase.client.java.query N1qlQuery N1qlQueryResult N1qlQueryRow N1qlParams AsyncN1qlQueryResult AsyncN1qlQueryRow]
           [com.couchbase.client.java.query.consistency ScanConsistency]
           [com.couchbase.client.java.document Document JsonDocument JsonArrayDocument JsonLongDocument JsonBooleanDocument]
           [com.couchbase.client.java.auth ClassicAuthenticator]
           [com.couchbase.client.java.env DefaultCouchbaseEnvironment]
           [com.couchbase.client.java.document.json JsonObject JsonArray JsonNull]
           [rx Observer Observable Subscriber]
           [java.util.concurrent TimeUnit]))


(declare to-map
         to-flat
         single!
         first!
         to-clj
         to-java)

(defonce ^:private default-durability (atom {:persist-to (PersistTo/valueOf "MASTER")
                                             :replicate-to (ReplicateTo/valueOf "NONE")
                                             :scan-consistency (ScanConsistency/valueOf "NOT_BOUNDED")}))

(defn set-default-durability [{:keys [persist-to replicate-to scan-consistency]}]

  (reset! default-durability {:persist-to (PersistTo/valueOf persist-to)
                              :replicate-to (ReplicateTo/valueOf replicate-to)
                              :scan-consistency (ScanConsistency/valueOf scan-consistency)}))


(defn connect [^String str]
  "connect to server
   examples
   couchbase://localhost or
   couchbase://10.1.0.1,10.1.0.2"
  (CouchbaseCluster/fromConnectionString str))


(defn disconnect [^CouchbaseCluster cluster]
  (.disconnect cluster))


(defn open-bucket
  ([^CouchbaseCluster cluster name]
   (-> cluster
       (.openBucket name)))

  ([^CouchbaseCluster cluster name password]
   (-> cluster
       (.openBucket name password)))

  ([^CouchbaseCluster cluster bucketname username password]
   (doto
       (-> cluster
           (.authenticate username password)
           (.openBucket bucketname)))))


(defn get-info [bucket]
  "http://docs.couchbase.com/sdk-api/couchbase-java-client-2.4.2/com/couchbase/client/java/bucket/BucketInfo.html"
  (-> bucket
      (.bucketManager)
      (.info)
      (.raw)
      to-clj))

(defn create-n1qlprimary-index [^Bucket bucket]
  (when bucket
    (-> bucket
        (.bucketManager)
        (.createN1qlPrimaryIndex true false))))


(defprotocol ClojureToJava
  "클로저 자료형을 자바자료형으로
  변환한다"
  (->java [o]))

(extend-protocol ClojureToJava
  clojure.lang.IPersistentMap
  (->java [o]
    (reduce (fn [jo [k v]]
              (.put jo (.replace (name k) "-" "_") (->java v)))
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
        JsonNull/INSTANCE s)))


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

  N1qlQueryRow
  (->clj [o]
    (->clj (.value o)))

  Document
  (->clj [o]
    (let [expiry (.expiry o)
          token  (.mutationToken o)
          meta (cond-> {:id  (.id o)
                        :cas (str (.cas o))
                        :value (->clj (.content o))}

                 (some? token) (assoc :token token)
                 (< 0 expiry)  (assoc :expiry expiry))]
      meta))

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





(defprotocol IBucket
  "프로토콜 정의
  버킷에서 사용할 도큐먼트를 생성, 가져오기"
  (create-doc [this id cas])
  (get-doc    [this bucket as-type]))


(defprotocol IQuery
  (simple-query [this] [this args]))


(defprotocol IMetric
  (get-metrics [this]))



(extend-protocol IQuery
  Observable
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
                   (to-clj i))

                 (-> this
                     (to-flat (fn [x] (.rows x)))))]

      (if with-metric?
        (assoc (get-metrics this) :results rows)
        rows)))


  N1qlQueryResult
  (simple-query [this args]

    (let [with-metric? (:with-metric args false)
          rows (for [i (.allRows this)]
                 (to-clj i))]

      (if with-metric?
        (assoc (get-metrics this) :results rows)
        rows))))


(extend-protocol IMetric

  Observable
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

  N1qlQueryResult
  (get-metrics [this]
    {:requestId (.requestId this)
     :errors (to-clj (.errors this))
     :status (.status this)
     :metrics (to-clj (.asJsonObject (.info this)))}))


(extend-protocol IBucket
  java.lang.Object
  (get-doc [this bucket {:keys [as timeout locktime] :or {timeout (.kvTimeout (.environment bucket))}}]
    "timeout is followed by Couchbase Environment, kvTimout (default 2500)
     locktime is max 30(seconds)"
    (let [lock? (pos? (if (number? locktime) locktime 0))]
      (if-let [klass (case as
                       :long JsonLongDocument
                       :array JsonArrayDocument
                       nil)]
        
        (to-clj
         (if lock?
           (.getAndLock bucket this locktime klass)
           (.get bucket this klass timeout TimeUnit/MILLISECONDS)))
          
        (to-clj 
         (if lock?
           (.getAndLock bucket this locktime )
           (.get bucket this timeout TimeUnit/MILLISECONDS))))))


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
   (-> bucket
       (.insert (create-doc doc id nil) (:persist-to @default-durability) (:replicate-to @default-durability))
       to-clj)))


(defn upsert! [bucket id doc]
  (-> bucket
      (.upsert (create-doc doc id nil) (:persist-to @default-durability) (:replicate-to @default-durability))
      to-clj))


(defn replace!
  ([bucket id doc]
   (-> bucket
       (.replace (create-doc doc id nil) (:persist-to @default-durability) (:replicate-to @default-durability))
       to-clj))

  ([bucket id doc cas]
   (-> bucket
       (.replace (create-doc doc id cas) (:persist-to @default-durability) (:replicate-to @default-durability))
        to-clj)))



(defn get! [bucket doc-id & args]
  "JsonLongDocument로 저장된 경우
   예) (get! *bucket* \"hello\" :long)
   없을 경우 JsonDocument로 가져온다"
  (get-doc doc-id bucket (or (first args) {})))



(defn get-as-long [bucket doc-id]
  (get-doc doc-id bucket {:as :long}))


(defn get-as-array [bucket doc-id]
  (get-doc doc-id bucket {:as :array}))


(defn remove! [bucket id]
  (if (.exists bucket id)
    (do
      (.remove bucket id (:persist-to @default-durability) (:replicate-to @default-durability))
      true)
    false))


(defn counter
  ([bucket id delta inital]
   (-> (.counter bucket id delta inital)
       (to-clj)))

  ([bucket id delta]
   (-> (.counter bucket id delta)
       (to-clj))))


(defn to-map
  ([^Observable ob]
   (-> ob
       (.map (reify rx.functions.Func1
               (call [this doc]
                 (to-clj doc))))))


  ([^Observable ob caller]
   (-> ob
       (.map (reify rx.functions.Func1
               (call [this doc]
                 (caller (to-clj doc))))))))



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

(defn last!
  ([^Observable ob]
   (-> ob
       (.timeout 1 TimeUnit/SECONDS)
       (.toBlocking)
       (.last)
       to-clj)))


(defn query [bucket [str & params] & [{:keys [with-metric block] :or {with-metric false block false}}]]
  (let [is-map? (map? (first params))
        n1ql-param (-> (N1qlParams/build)
                     (.consistency (:scan-consistency @default-durability)))
        result (->> (if (nil? params)
                      (N1qlQuery/simple str n1ql-param)
                      (N1qlQuery/parameterized str (if is-map? (to-java (first params)) (to-java params)) n1ql-param))
                    (.query bucket))]
    (simple-query result {:block block
                          :with-metric with-metric})))


(defn subscribe
  "TODO"
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
