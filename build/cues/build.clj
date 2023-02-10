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
  {:source     ::s1
   :queue-opts {::q/default {:queue-path "data/example"
                             :queue-meta #{:q/t :q/time}}
                ::tx        {:queue-meta #{:q/t :q/time :tx/t}}}
   :processors [{:id ::s1}
                {:id  ::processor
                 :in  {:in ::s1}
                 :out {:out ::tx}}
                {:id   ::doc-store
                 :in   {:in ::tx}
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
