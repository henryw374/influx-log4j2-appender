(ns com.widdindustries.influx-data)

(defn influx-point [measurement tags fields]
  {"measurement" measurement
   "tags"        tags
   "fields"      fields})

(defn influx-point? [p]
  (and
    (get p "measurement")
    (get p "tags")
    (get p "fields")))

(defn measurement [p] (get p "measurement"))
(defn tags [p] (get p "tags"))
(defn fields [p] (get p "fields"))
