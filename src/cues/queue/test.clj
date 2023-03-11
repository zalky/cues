(ns cues.queue.test
  "Provides Cues test fixtures."
  (:require [cinch.core :as util]
            [cues.queue :as q]
            [taoensso.timbre :as log]))

(defn with-warn
  [f]
  (log/with-level :warn
    (f)))

(defn index-from-1
  "For testing purposes only."
  [tailer]
  (q/with-tailer [t-1 (:queue tailer)]
    (let [i   (q/index* tailer)
          _   (q/to-start t-1)
          i-1 (q/index* t-1)]
      (inc (- i i-1)))))

(defn last-index-from-1
  "For testing purposes only."
  [queue]
  (q/with-tailer [t-1 queue]
    (let [i   (q/last-index* queue)
          _   (q/to-start t-1)
          i-1 (q/index* t-1)]
      (inc (- i i-1)))))

(defn last-read-index-from-1
  "For testing purposes only."
  [tailer]
  (q/with-tailer [t-1 (:queue tailer)]
    (let [i   (q/last-read-index* tailer)
          _   (q/to-start t-1)
          i-1 (q/index* t-1)]
      (inc (- i i-1)))))

(defn to-index-from-1
  "For testing purposes only."
  [tailer i]
  (if (pos? i)
    (q/with-tailer [t-1 (:queue tailer)]
      (let [_   (q/to-start t-1)
            i-1 (q/index* t-1)]
        (->> (+ i (dec i-1))
             (q/to-index* tailer))))
    (q/to-start tailer)))

(defn written-index-from-1
  "For testing purposes only."
  [queue i]
  (q/with-tailer [t-1 queue]
    (let [_   (q/to-start t-1)
          i-1 (q/index* t-1)]
      (inc (- i i-1)))))

(defn with-deterministic-meta
  "A number of cues.queue functions are rebound to make queue indices
  easier to work with in unit tests. While ChronicleQueue indicies are
  deterministic, they have a complex relationship to the roll cycles
  of the data on disk. Queue indices are guaranteed to increase
  monotonically, but not always continguously, and in general code
  that tries to predict future indices should be avoided. However,
  under the narrow constraints of the unit tests and test queue
  configurations, these new bindings will start all indices at 1 and
  then increase continguously. Just beware that this does NOT hold in
  general, and you should never rebind these methods outside of unit
  tests."
  [f]
  (binding [q/last-read-index  last-read-index-from-1
            q/last-index       last-index-from-1
            q/to-index         to-index-from-1
            q/index            index-from-1
            q/written-index    written-index-from-1
            q/add-attempt-hash (fn [_ msg] msg)]
    (f)))

(defmacro with-graph-impl-and-delete
  [[sym :as binding] & body]
  `(let ~binding
     (let [~sym (-> ~sym
                    (util/assoc-nil :error-queue ::error)
                    (update :queue-opts util/assoc-nil ::q/default {:queue-meta #{:q/t}})
                    (q/graph-impl)
                    (q/start-graph!))]
       (try
         ~@body
         (finally
           (q/delete-graph-queues! ~sym true))))))

(defmacro with-graph-and-delete
  [[sym config] & body]
  `(let ~[sym config]
     (with-graph-impl-and-delete
       [~sym (q/parse-graph ~sym)]
       ~@body)))

(defn simplify-exceptions
  [messages]
  (map #(update %
                :err/cause
                select-keys
                [:cause])
       messages))
