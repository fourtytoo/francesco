(ns francesco.prod
  (:require [clojure.core.async :as a]
            [franzy.clients.producer.client :as producer]
            [franzy.clients.producer.protocols :as pprot]))


(defn assocnn
  "Like `assoc` if value is not nil, otherwise return `m`."
  [m k v & kvs]
  (let [m (if v
            (assoc m k v)
            m)]
    (if kvs
      (recur m (first kvs) (second kvs) (nnext kvs))
      m)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmacro with-producer
  "Execute `body` binding `producer` to a Kafka producer.  The
  `producer` is automatically closed on exit of the code block."
  [[producer config & make-producer-options] & body]
  `(with-open [~producer (producer/make-producer ~config ~@make-producer-options)]
     ~@body))

(defn producer-send [producer topic key value & {:keys [partition]}]
  (pprot/send-sync! producer
                    (-> {:key key
                         :value value
                         :topic topic}
                        (assocnn :partition partition))))

(defn producer-close
  "Close a Kafka `producer` avoiding race conditions."
  [producer]
  (locking producer
    (.close producer)))

(defn make-producer-channel
  "Make an async channel associated with a Kafka producer.  What is put
  into the channel is forwarded to a Kafka queue.  When the channel is
  closed, so is the underlying Kafka producer.  The `key-fn` is used
  to extract the key from the messages put in the channel; this
  function is applied to every message going through the channel.
  Return a core.async channel."
  [producer topic key-fn]
  (let [c (a/chan)]
    (a/go (try
            (loop []
              (if-some [msg (a/<! c)]
                (do
                  (producer-send producer topic (key-fn msg) msg)
                  (recur))))
            (finally
              (producer-close producer)
              (a/close! c))))
    c))

(defmacro with-producer-channel
  "Execute `body` binding `channel` to a Kafka queue.  See
  `make-producer-channel` for further details.  The `channel` is
  automatically closed, upon exit of the code block."
  [[channel config topic key-fn & make-producer-options] & body]
  `(with-producer [producer# ~config ~@ make-producer-options]
     (let [~channel (make-producer-channel producer# ~topic ~key-fn)]
       (try
         (do ~@body)
         (finally
           (a/close! ~channel))))))

