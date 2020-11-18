(ns francesco.util
  (:require [clojure.string :as s]))

(defn assocnn
  "Like `assoc` if value is not nil, otherwise return `m`."
  [m k v & kvs]
  (let [m (if v
            (assoc m k v)
            m)]
    (if kvs
      (recur m (first kvs) (second kvs) (nnext kvs))
      m)))

(defn ->prop
  "Convert a clojure map to something compatible with Java property
  maps."
  [obj]
  (cond (keyword? obj) (name obj)
        (map? obj) (->> obj
                        (map (juxt (comp name key)
                                   (comp ->prop val)))
                        (into {}))
        (coll? obj) (->> obj
                         (map ->prop)
                         (s/join ","))
        :else (str obj)))

(defn as-properties [map]
  (doto (java.util.Properties.)
    (.putAll (francesco.util/->prop map))))

(defn consumer-record->map
  "Convert a `org.apache.kafka.clients.consumer.ConsumerRecord` into a
  Clojure map."
  [cr]
  {:topic (.topic cr)
   :partition (.partition cr)
   :offset (.offset cr)
   :key (.key cr)
   :value (.value cr)
   :timestamp (.timestamp cr)
   :headers (.headers cr)})
