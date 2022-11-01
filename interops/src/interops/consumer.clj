(ns interops.consumer
  (:gen-class)
  (:import
   org.apache.kafka.clients.consumer.KafkaConsumer
   (java.time Duration)
   [org.apache.kafka.common.serialization StringDeserializer]))

(defn build-consumer
  "Create the consumer instance to consume
from the provided kafka topic name"
  [bootstrap-server]
  (let [consumer-props
        {"bootstrap.servers",  bootstrap-server
         "group.id",           "a"
         "key.deserializer",   StringDeserializer
         "value.deserializer", StringDeserializer
         "auto.offset.reset",  "earliest"
         "enable.auto.commit", "true"}]
    (KafkaConsumer. consumer-props)
    ))

(defn consumer-subscribe
  [topic]
  (with-open [consumer (build-consumer "host.docker.internal:9091")]
    (.subscribe consumer [topic])
    (while true
      (let [records (.poll consumer (Duration/ofMillis 100))]
        (doseq [record records]
          (println "Valor: " (.value record)))))))

(defn -main []
  (consumer-subscribe "name1"))
