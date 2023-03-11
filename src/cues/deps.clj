(ns cues.deps
  "Extends com.stuartsierra.dependency for disconnected graphs."
  (:require [cinch.core :as util]
            [cinch.spec :as s*]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [com.stuartsierra.dependency :as dep]))

(s/def ::step
  (s*/non-conformer
   (s/or :k qualified-keyword?
         :s string?)))

(s/def ::steps
  (s/or :dag   ::dag
        :pipe  ::pipeline
        :steps ::set-of-steps
        :step  ::step))

(s/def ::set-of-steps
  (s*/conform-to
    (s/coll-of (s/nilable ::steps) :kind set?)
    (partial remove nil?)))

(s/def ::dag
  (s/map-of ::steps ::steps :conform-keys true))

(s/def ::pipeline
  (s*/conform-to
    (s/coll-of (s/nilable ::steps) :kind vector?)
    (partial remove nil?)))

(defn no-dependents?
  [g node]
  (empty? (dep/immediate-dependents g node)))

(defn no-dependencies?
  [g node]
  (empty? (dep/immediate-dependencies g node)))

(defn out-nodes
  [g]
  (->> g
       (dep/nodes)
       (filter (partial no-dependents? g))
       (set)))

(defn in-nodes
  [g]
  (->> g
       (dep/nodes)
       (filter (partial no-dependencies? g))
       (set)))

(defprotocol DisconnectedDependencyGraphUpdate
  (add-node [g node]
    "Adds a disconnected node to the dependency graph."))

(defrecord DisconnectedDependencyGraph [nodes deps]
  dep/DependencyGraph
  (immediate-dependencies [_ node]
    (dep/immediate-dependencies deps node))
  
  (immediate-dependents [_ node]
    (dep/immediate-dependents deps node))
  
  (transitive-dependencies [_ node]
    (dep/transitive-dependencies deps node))
  
  (transitive-dependencies-set [_ node-set]
    (dep/transitive-dependencies-set deps node-set))
  
  (transitive-dependents [_ node]
    (dep/transitive-dependents deps node))
  
  (transitive-dependents-set [_ node-set]
    (dep/transitive-dependents-set deps node-set))
  
  (nodes [g]
    (:nodes g))

  dep/DependencyGraphUpdate
  (depend [g node dep]
    (-> g
        (update :deps dep/depend node dep)
        (update :nodes util/conjs node dep)))

  (remove-edge [g node dep]
    (update g :deps dep/remove-edge node dep))
  
  (remove-all [g node]
    (-> g
        (update :deps dep/remove-all node)
        (update :nodes disj node)))
  
  (remove-node [g node]
    (update g :deps dep/remove-node node))

  DisconnectedDependencyGraphUpdate
  (add-node [g node]
    (update g :nodes util/conjs node)))

(defn disconnected-graph
  ([]
   (disconnected-graph #{}))
  ([nodes]
   (disconnected-graph nodes (dep/graph)))
  ([nodes deps]
   (DisconnectedDependencyGraph. nodes deps)))

(defn merge-graphs
  ([] (disconnected-graph))
  ([g] g)
  ([{n1 :nodes d1 :deps}
    {n2 :nodes d2 :deps}]
   (->> (merge-with (partial merge-with set/union) d1 d2)
        (disconnected-graph (set/union n1 n2)))))

(defn depend-graphs
  [{n1 :nodes :as g1}
   {n2 :nodes :as g2}]
  (let [in   (in-nodes g1)
        out  (out-nodes g2)
        deps (for [o out
                   i in]
               [i o])]
    (reduce
     (fn [g [i o]] (dep/depend g i o))
     (merge-graphs g1 g2)
     deps)))

(declare graph*)

(defn dag-graph
  [form]
  (reduce-kv
   (fn [g form-1 form-2]
     (let [g1 (graph* form-1)
           g2 (graph* form-2)]
      (->> (depend-graphs g2 g1)
           (merge-graphs g))))
   (disconnected-graph)
   form))

(defn pipe-graph
  [form]
  (loop [g               (disconnected-graph)
         [g-next & more] (->> form
                              (remove nil?)
                              (map graph*))]
    (if g-next
      (-> g-next
          (depend-graphs g)
          (recur more))
      g)))

(defn steps-graph
  [form]
  (->> form
       (remove nil?)
       (map graph*)
       (reduce merge-graphs)))

(defn step-graph
  [form]
  (disconnected-graph #{form}))

(defn- graph*
  [form]
  (let [[t subform] form]
    (case t
      :dag   (dag-graph subform)
      :pipe  (pipe-graph subform)
      :step  (step-graph subform)
      :steps (steps-graph subform))))

(defn graph
  [expr]
  (graph* (s*/parse ::steps expr)))

(defn transitive-dependencies+
  [graph node]
  (-> graph
      (dep/transitive-dependencies node)
      (conj node)))

(defn transitive-dependents+
  [graph node]
  (-> graph
      (dep/transitive-dependents node)
      (conj node)))

(defn two-hops
  [deps node]
  (->> node
       (dep/immediate-dependencies deps)
       (mapcat #(dep/immediate-dependencies deps %))
       (not-empty)
       (set)))

;; Re-bind com.stuartsierra.dependency API

(def immediate-dependencies      dep/immediate-dependencies)
(def immediate-dependents        dep/immediate-dependents)
(def transitive-dependencies     dep/transitive-dependencies)
(def transitive-dependencies-set dep/transitive-dependencies-set)
(def transitive-dependents       dep/transitive-dependents)
(def transitive-dependents-set   dep/transitive-dependents-set )
(def nodes                       dep/nodes)

(def depend                      dep/depend)
(def remove-edge                 dep/remove-edge)
(def remove-all                  dep/remove-all)
(def remove-node                 dep/remove-node)

(def depends?                    dep/depends?)
(def dependent?                  dep/dependent?)
(def topo-sort                   dep/topo-sort)
(def topo-comparator             dep/topo-comparator)
