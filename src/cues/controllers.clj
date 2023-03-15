(ns cues.controllers
  "Provides a global controller cache.

  Controllers ensure that blocking tailers will unblock when an
  appender writes to a blocking queue. Each queue has one and only one
  corresponding controller. Once a controller has been bound to a
  queue on disk, it should never be removed for the lifetime of the
  app, unless the queue has been deleted on disk, in which case, the
  controller must be purged from the cache."
  (:require [cues.util :as util])
  (:import java.nio.file.Paths))

(def ^:private cache
  (atom {}))

(defn- components
  [path]
  (->> (into-array String [])
       (Paths/get path)
       (.iterator)
       (iterator-seq)
       (map str)))

(defn lookup
  [path xtor]
  (let [c (components path)]
    (letfn [(f [cache]
              (if-not (get-in cache c)
                (assoc-in cache c (xtor))
                cache))]
      (get-in (swap! cache f) c))))

(defn purge
  [path]
  (->> path
       (components)
       (swap! cache util/dissoc-in)))
