(ns cues.repl
  (:require [cues.queue :as q]
            [runway.core :as run]
            [taoensso.timbre :as log]))

(defn system
  "Returns the full system, or specific dependencies."
  ([]
   run/system)
  ([dependencies]
   (select-keys (system) dependencies)))

(defn graph
  []
  (:graph (system)))

(defn restart!
  "Restarts system without spamming repl."
  []
  (run/restart)
  nil)

(defn stop!
  "Stop system without spamming repl."
  []
  (run/stop)
  nil)

(defn start!
  "Start system without spamming repl."
  []
  (run/start)
  nil)

(defn delete-queues-and-restart!
  "Restarts system without spamming repl."
  ([]
   (-> (graph)
       (:queues)
       (keys)
       (delete-queues-and-restart!)))
  ([ids]
   (let [queues (-> (graph)
                    (:queues)
                    (select-keys ids)
                    (vals))]
     (stop!)
     (log/info "Deleting all queues and restarting...")
     (doseq [q queues]
       (q/delete-queue! q true))
     (start!))
   nil))


