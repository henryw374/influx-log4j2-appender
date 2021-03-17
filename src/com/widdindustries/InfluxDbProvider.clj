(ns com.widdindustries.InfluxDbProvider
  (:require [com.widdindustries.influx-data :as influx-data])
  (:import [org.apache.logging.log4j.core.appender.nosql
            NoSqlProvider DefaultNoSqlObject NoSqlConnection NoSqlObject
            NoSqlAppender]
           [org.apache.logging.log4j.status StatusLogger]
           [org.influxdb InfluxDB InfluxDBFactory InfluxDB$ConsistencyLevel]
           [org.influxdb.dto BatchPoints Point]
           (org.apache.logging.log4j.core.layout MessageLayout)
           (java.util.concurrent TimeUnit)
           (org.apache.logging.log4j.message MapMessage)))

(defn point [msg]
  (->
    (Point/measurement (influx-data/measurement msg))
    (.time (System/currentTimeMillis) TimeUnit/MILLISECONDS)
    (.fields (influx-data/fields msg))
    (.tag (influx-data/tags msg))
    (.build)))

(defn insert [^InfluxDB influx ^MapMessage o]
  (def o o)
  (let [msg (.unwrap o)]
    (when (influx-data/influx-point? msg)
      (.write influx ^Point (point msg)))))

(defrecord Connection [influx]
  NoSqlConnection
  (createObject [_] (DefaultNoSqlObject.))
  (createList [_ length] (make-array DefaultNoSqlObject length))
  (insertObject [_ obj]
    (insert influx obj))
  (close [_])
  (isClosed [_] false))

(def default-config
  ; Flush every 2000 Points, at least every 10 seconds
  {:max-points     2000
   :flush-duration 10
   :flush-unit     TimeUnit/SECONDS})

(defn influx-connection [{:keys [create-db? database-name influx-url max-points flush-duration flush-unit]}]
  (let [influx (InfluxDBFactory/connect influx-url)]
    (when create-db?
      (.createDatabase influx database-name))
    (.setDatabase influx database-name)
    (.enableBatch influx max-points, flush-duration, flush-unit)))

(defrecord Provider [connection]
  NoSqlProvider
  (getConnection [_]
    connection))

(defn provider [config]
  (Provider.
    (Connection.
      (influx-connection
        (merge default-config config)))))

(defn appender [influx-config buffer-size]
  (-> (NoSqlAppender/newBuilder)
      (.setName (:influx-url influx-config))
      (.setProvider (provider influx-config))
      (.setBufferSize buffer-size)
      (.setLayout (MessageLayout.))
      (.build)))


