(ns cues.controller-cache
  "We require a global controller cache to ensure that any instance of a
  blocking tailers will successfully unblock when any instance of a
  blocking appender writes to any instance of a blocking queue, that
  are all bound to the same path on disk, once a controller has been
  bound to a path, it should never be removed for the lifetime of the
  app, unless the actual queue files on disk have been deleted.")

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
