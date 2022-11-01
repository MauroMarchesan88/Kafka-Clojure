(ns interops.core
  (:gen-class)
  (:import
   [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
   [org.apache.kafka.common.serialization StringSerializer]))

(defn build-producer
  ;"Create the kafka producer to send on messages received"
  [bootstrap-server]
  (let [producer-props {"value.serializer"  StringSerializer
                        "key.serializer"    StringSerializer
                        "bootstrap.servers" bootstrap-server}]
    (KafkaProducer. producer-props)))

(defn produce [msg]
  (with-open  [producera (build-producer "host.docker.internal:9091")]
    (.send producera (ProducerRecord. "name1" msg))))

(defn -main [msg]
  (produce msg))
