(ns cues.build
  (:require [com.stuartsierra.component :as component]
            [cues.queue :as q]
            [runway.core :as run]

            ;; Loaded for use
            [cues.log]))

(defmethod q/processor ::processor
  [{:keys [system]} {msg :in}]
  {:out (assoc-in msg [:q/topics ::doc :processed] true)})

(defmethod q/processor ::doc-store
  [{{db :db} :opts}
   {{{{id  :id
       :as doc} ::doc} :q/topics
     :as               msg} :in}]
  (swap! db update id merge doc))

(defn graph
  [db]
  {:source     ::s1
   :tx-queue   ::tx
   :queue-path "data/example"
   :processors [{:id ::s1}
                {:id  ::processor
                 :in  {::s1 :in}
                 :out {::tx :out}}
                {:id     ::doc-store
                 :topics ::doc
                 :in     {::tx :in}
                 :opts   {:db db}}]})

(defonce db
  (atom {}))

(defrecord GraphExample []
  component/Lifecycle
  (start [c]
    (->> (graph db)
         (q/parse-graph)
         (q/graph)
         (q/start-graph!)
         (merge c)))

  (stop [c]
    (->> c
         (q/stop-graph!)
         (q/close-graph!))
    (GraphExample.)))

(def components
  {:graph [->GraphExample]})

(def dependencies
  {})

(defn test-system
  []
  (run/assemble-system components dependencies))
