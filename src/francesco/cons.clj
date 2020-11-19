(ns francesco.cons
  (:require [clojure.core.async :as a]
            [franzy.clients.codec :as codec]
            [franzy.clients.consumer.protocols :as cprot]
            [franzy.clients.consumer.defaults :refer [make-default-consumer-options]]
            [franzy.clients.consumer.callbacks :refer [consumer-rebalance-listener]]
            [franzy.serialization.deserializers :as des]
            [franzy.clients.consumer.client :as cli]
            [francesco.util :refer :all])
  (:import [franzy.clients.consumer.types ConsumerRecord]
           [org.apache.kafka.clients.consumer KafkaConsumer]))


(def edn-deserializer (des/edn-deserializer))

(defn consumer-record->map
  "Convert a `org.apache.kafka.clients.consumer.ConsumerRecord` (not
  Franzy's one) into a Clojure map."
  [cr]
  {:topic (.topic cr)
   :partition (.partition cr)
   :offset (.offset cr)
   :key (.key cr)
   :value (.value cr)
   :timestamp (.timestamp cr)
   :headers (.headers cr)})

(defn make-consumer [config & [key-deserializer value-deserializer options]]
  (let [options (if (:rebalance-listener-callback options)
                  (update options :rebalance-listener-callback
                          (fn [cb]
                            (if (fn? cb)
                              (consumer-rebalance-listener cb)
                              cb)))
                  options)]
    (-> (->prop config)
        (KafkaConsumer. (or key-deserializer edn-deserializer)
                        (or value-deserializer edn-deserializer))
        (cli/->FranzConsumer (make-default-consumer-options options)))))

(defn consumer-assign [consumer topic-partitions]
  (locking consumer
    (cprot/assign-partitions! consumer topic-partitions)
    consumer))

(defn consumer-assignments [consumer]
  (locking consumer
    (cprot/assigned-partitions consumer)))

(defn consumer-subscribe [consumer topics]
  (locking consumer
    (cprot/subscribe-to-partitions! consumer topics)
    consumer))

(defn consumer-subscriptions [consumer]
  (locking consumer
    (cprot/partition-subscriptions consumer)))

(defn consumer-clear-subscriptions [consumer]
  (locking consumer
    (cprot/clear-subscriptions! consumer)))

(defn consumer-seek [consumer topic-partition offset]
  (locking consumer
    (case offset
      :beginning
      (cprot/seek-to-beginning-offset! consumer [topic-partition])

      :end
      (cprot/seek-to-end-offset! consumer [topic-partition])

      (cprot/seek-to-offset! consumer topic-partition offset)))
  consumer)

(defn consumer-position [consumer topic-partition]
  (locking consumer
    (cprot/next-offset consumer topic-partition)))

(defn consumer-receive [consumer & args]
  (locking consumer
    (->> (apply cprot/poll! consumer args)
         seq
         (map #(assoc % :consumer consumer))
         doall)))

(defn consumer-commit [consumer & [offsets]]
  (locking consumer
    (if offsets
      (cprot/commit-offsets-sync! consumer offsets)
      (cprot/commit-offsets-sync! consumer))))

(defn consumer-message [& {:keys [topic partition offset key value]}]
  (franzy.clients.consumer.types/->ConsumerRecord
   topic partition offset key value))

(defn message-topic-partition [msg]
  (select-keys msg [:topic :partition]))

(defn message-cursor [msg]
  (select-keys msg [:topic :partition :offset]))

(defn cursor-next [cursor]
  (update cursor :offset inc))

(defn message-offset [msg]
  (:offset msg))

(defn message-commit [msg]
  (let [offsets {(message-topic-partition msg)
                 {:offset (inc (:offset msg))}}]
    (consumer-commit (:consumer msg) offsets)))

(defn consumer-message? [obj]
  (instance? ConsumerRecord obj))

(defn consumer-close [consumer]
  (locking consumer
    (.close consumer)))

(defmacro with-consumer
  "Execute `body` binding `consumer` to a Kafka consumer.  Upon exit of
  the code block, the `consumer` is automatically closed."
  [[consumer config & rest] & body]
  `(with-open [~consumer (make-consumer ~config ~@rest)]
     ~@body))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn make-consumer-channel
  "Make an async channel associated with a Kafka consumer.  Each get
  returns a message from a Kafka queue.  When the channel is closed,
  so is the underlying Kafka consumer.  Return a core.async channel."
  [consumer & chan-args]
  (let [c (apply a/chan chan-args)]
    (a/go
      (try
        (loop []
          (doseq [msg (consumer-receive consumer)]
            (a/>! c msg))
          (recur))
        (finally
          (consumer-close consumer)
          (a/close! c))))
    c))

(defmacro with-consumer-channel
  "Execute `body` binding `channel` to an async channel which, in turn,
  is fed from a Kafka consumer.  Upon exit of the code block, the
  `channel` (and thus the consumer) is automatically closed."
  [[channel config & {:keys [topic partition offset] :as args}] & body]
  (let [consumer (gensym "consumer")]
    `(with-consumer [~consumer ~config ~@((juxt :key-deserializer :value-deserializer :options) args)]
       ~(if partition
          `(consumer-assign ~consumer [~(select-keys args [:topic :partition])])
          `(consumer-subscribe ~consumer [~topic]))
       ~@(when (and partition offset)
           `((consumer-seek c ~(select-keys args [:topic :partition]) ~offset)))
       (let [~channel (make-consumer-channel ~consumer)]
         (try
           (do ~@body)
           (finally
             (a/close! ~channel)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn message-range
  "Query the Kafka queue of `consumer` and return the message range
  associated to `topic-partition`.  Return a vector of two elements:
  the beginning offset and the end offset."
  [consumer topic-partition]
  (locking consumer
    (let [c (.consumer consumer)
          topic-partition (codec/map->topic-partition topic-partition)]
      [(->> (.beginningOffsets c [topic-partition])
            first val)
       (->> (.endOffsets c [topic-partition])
            first val)])))

(defn- partition-info->map
  "Convert a partition info `pi` to a Clojure map."
  [pi]
  {:topic (.topic pi)
   :leader (.leader pi)
   :partition (.partition pi)
   :replicas (.replicas pi)})

(defn partitions-for
  "Query the Kafka queue of `consumer` and return the list of partitions
  associated to `topic`.  Return a list of maps."
  [consumer topic]
  (locking consumer
    (->> (.partitionsFor (.consumer consumer) topic)
         (map partition-info->map)
         doall)))
