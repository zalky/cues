(ns cues.build
  (:require [com.stuartsierra.component :as component]
            [cues.queue :as q]
            [runway.core :as run]

            ;; Loaded for use
            [cues.log]))

(defmethod q/processor ::processor
  [_ {msg :in}]
  {:out (update msg :x inc)})

(defmethod q/processor ::doc-store
  [{{db :db} :opts} {msg :in}]
  (swap! db assoc (:x msg) (dissoc msg :q/meta)))

(defn graph-spec
  [db]
  {:source         ::s1
   :queue-opts-all {:queue-meta {:tx-queue ::tx}
                    :queue-path "data/example"}
   :processors     [{:id ::s1}
                    {:id  ::processor
                     :in  {::s1 :in}
                     :out {::tx :out}}
                    {:id   ::doc-store
                     :in   {::tx :in}
                     :opts {:db db}}]})

(defonce db
  (atom {}))

(defrecord GraphExample []
  component/Lifecycle
  (start [c]
    (->> (graph-spec db)
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
