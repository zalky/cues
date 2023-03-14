(ns cues.controllers
  "Provides a global controller cache.

  Controllers ensure that blocking tailers will unblock when an
  appender writes to a blocking queue. Each queue has one and only one
  corresponding controller. Once a controller has been bound to a
  queue on disk, it should never be removed for the lifetime of the
  app, unless the queue has been deleted on disk, in which case, the
  controller must be purged from the cache.")

(def cache
  (atom {}))

(defn lookup
  [id xtor]
  (letfn [(lookup* [c]
            (if-not (get c id)
              (assoc c id (xtor))
              c))]
    (get (swap! cache lookup*) id)))

(defn purge
  [id]
  (swap! cache dissoc id))
