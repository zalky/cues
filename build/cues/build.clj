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
  {:source      ::source
   :error-queue ::error
   :queue-opts  {::source {:queue-meta #{:tx/t}}}
   :processors  [{:id ::source}
                 {:id  ::processor
                  :in  {:in ::source}
                  :out {:out ::tx}}
                 {:id   ::doc-store
                  :in   {:in ::tx}
                  :opts {:db db}}]})

(defonce db
  (atom {}))

(defrecord GraphExample []
  component/Lifecycle
  (start [component]
    (->> (graph-spec db)
         (q/graph)
         (q/start-graph!)
         (merge component)))

  (stop [component]
    (->> component
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
