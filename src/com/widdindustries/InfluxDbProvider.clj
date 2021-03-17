(ns com.widdindustries.InfluxDbProvider
  (:import [
            org.apache.logging.log4j.core.appender.nosql
            NoSqlProvider DefaultNoSqlObject NoSqlConnection NoSqlObject
            NoSqlAppender]
           [org.apache.logging.log4j.status StatusLogger]
           [org.influxdb InfluxDB InfluxDBFactory InfluxDB$ConsistencyLevel]
           [org.influxdb.dto BatchPoints]
           (org.apache.logging.log4j.core.layout MessageLayout)))

(defn insert [o]
  (def o o )
  (.unwrap o)
  ;(sc.api/spy)
  
  )

(defn connection [influx-url]
  (reify NoSqlConnection
    (createObject [_] (DefaultNoSqlObject.))
    (createList [_ length] (make-array DefaultNoSqlObject length))
    (insertObject [_ obj]
      (insert obj))
    (close [_])
    (isClosed [_] false)
    ))

(defn provider [influx-url]
  (reify NoSqlProvider
    (getConnection [_]
      (connection influx-url)
      )))

(defn appender [influx-url buffer-size]
  (-> (NoSqlAppender/newBuilder)
      (.setName influx-url)
      (.setProvider (provider influx-url))
      (.setBufferSize buffer-size)
      (.setLayout (MessageLayout.))
      (.build)))



#_(defn no-sql-appender [builder appender-name]
  (-> builder
      (.newAppender appender-name, "NoSql")
      (.add )))


