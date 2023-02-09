(ns cues.queue.test
  (:require [cues.queue :as q]
            [cues.util :as util]
            [taoensso.timbre :as log]))

(defn with-warn
  [f]
  (log/with-level :warn
    (f)))

(defn last-read-index-from-1
  "For testing purposes only."
  [tailer]
  (q/with-tailer [t-1 (:queue tailer)]
    (let [i   (q/last-read-index* tailer)
          _   (q/to-start t-1)
          i-1 (q/index t-1)]
      (inc (- i i-1)))))

(defn to-index-from-1
  "For testing purposes only."
  [tailer i]
  (q/with-tailer [t-1 (:queue tailer)]
    (let [_   (q/to-start t-1)
          i-1 (q/index t-1)]
      (->> (+ i (dec i-1))
           (q/to-index* tailer)))))

(defn written-index-from-1
  "For testing purposes only."
  [queue i]
  (q/with-tailer [t-1 queue]
    (let [_   (q/to-start t-1)
          i-1 (q/index t-1)]
      (inc (- i i-1)))))

(defn with-deterministic-meta
  "The order that messages are placed on outbound fork queues is
  non-deterministic, therefore we can't test timestamps here."
  [f]
  (binding [q/last-read-index last-read-index-from-1
            q/to-index        to-index-from-1
            q/timestamp       (fn [_ msg] msg)
            q/written-index   written-index-from-1]
    (f)))

(defn delete-graph-queues!
  "Careful with this function!"
  [g]
  (doseq [q (vals (:queues g))]
    (q/delete-queue! q true)))

(defmacro with-graph-and-delete
  [[sym :as binding] & body]
  `(let ~binding
     (let [~sym (-> ~sym
                    (util/assoc-nil :error-queue ::error)
                    (q/graph-impl)
                    (q/start-graph!))]
       (try
         ~@body
         (finally
           (-> ~sym
               (q/stop-graph!)
               (q/close-graph!)
               (delete-graph-queues!)))))))

(defn simplify-exceptions
  [messages]
  (map #(update %
                :err/cause
                select-keys
                [:cause])
       messages))
