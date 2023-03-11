(ns cues.controller-cache
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
  [k xtor]
  (letfn [(lookup* [c]
            (if-not (get c k)
              (assoc c k (xtor))
              c))]
    (get (swap! cache lookup*) k)))

(defn purge-controller
  [k]
  (swap! cache dissoc k))
