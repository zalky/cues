(ns cues.repl)

(in-ns 'user)

(require '[cues.build :as build]
         '[cues.deps :as deps]
         '[cues.queue :as q]
         '[runway.core :as run]
         '[taoensso.timbre :as log])

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

(defn delete-graph-and-restart!
  []
  (let [g (graph)]
    (stop!)
    (log/info "Deleting system graph and restarting...")
    (q/delete-graph-queues! g)
    (start!))
  nil)
