(ns cues.deps-test
  (:require [cues.deps :as deps]
            [clojure.test :as t :refer [is]]
            [com.stuartsierra.dependency :as sdeps]))

(defn g
  [nodes dependencies dependents]
  (->> dependents
       (sdeps/->MapDependencyGraph dependencies)
       (deps/disconnected-graph nodes)))

(t/deftest graph-test
  (is (= (deps/graph [])
         (deps/graph #{})
         (deps/graph {})
         (g #{} {} {})))
  (is (= (deps/graph {[] []})
         (g #{} {} {})))
  (is (= (deps/graph {[nil] []})
         (g #{} {} {})))
  (is (= (deps/graph #{nil [nil]})
         (g #{} {} {})))
  (is (= (deps/graph #{::A ::B})
         (deps/graph #{::A ::B nil})
         (deps/graph #{[::A nil] ::B nil})
         (deps/graph #{::A #{::B}})
         (g #{::A ::B} {} {})))
  (is (= (deps/graph #{::A {::B ::C}})
         (g
           #{::A ::B ::C}
           {::C #{::B}}
           {::B #{::C}})))
  (is (= (deps/graph [::A ::B ::C ::D])
         (deps/graph [::A [::B [::C [::D]]]])
         (deps/graph [::A {::B [::C #{::D}]}])
         (g
           #{::A ::B ::C ::D}
           {::B #{::A} ::C #{::B} ::D #{::C}}
           {::A #{::B} ::B #{::C} ::C #{::D}})))
  (is (= (deps/graph {::A ::B})
         (deps/graph [::A ::B])
         (g #{::A ::B} {::B #{::A}} {::A #{::B}})))
  (is (= (deps/graph {::A [::B ::C]})
         (g
           #{::A ::B ::C}
           {::B #{::A} ::C #{::B}}
           {::A #{::B} ::B #{::C}})))
  (is (= (deps/graph {#{::A ::D} [::B ::C]
                      ::B        [::E]})
         (g
           #{::A ::B ::C ::D ::E}
           {::B #{::D ::A}
            ::E #{::B}
            ::C #{::B}}
           {::A #{::B}
            ::D #{::B}
            ::B #{::C ::E}})))
  (is (= (deps/graph [#{::A ::B}
                      #{[::C ::D ::E]
                        [::F ::G ::H]}])
         (g
           #{::B ::C ::D ::G ::E ::F ::A ::H}
           {::D #{::C}
            ::E #{::D}
            ::G #{::F}
            ::H #{::G}
            ::C #{::B ::A}
            ::F #{::B ::A}}
           {::C #{::D}
            ::D #{::E}
            ::F #{::G}
            ::G #{::H}
            ::B #{::C ::F}
            ::A #{::C ::F}})))
  (let [g       (deps/graph [#{::A ::B}
                             #{[::C ::D ::E]
                               [::F ::G ::H]}])
        compare (deps/topo-comparator g)]
    ;; The clojure.tools.namespace.dependency/topo-comparator
    ;; implementation chooses a 2-way comparator over a 3-way one,
    ;; maybe for performance reasons. This results in a correct sort,
    ;; but a non-deterministic order for certain types of
    ;; graphs. Therefore, here instead of checking the order of the
    ;; sort, these tests assert comparisons where the result is known
    ;; to be deterministic.
    (is (= [::C ::D ::E] (sort compare [::E ::D ::C])))
    (is (= [::F ::G ::H] (sort compare [::H ::G ::F])))
    (is (every? #{-1} (for [x [::A ::B]
                            y [::C ::D ::E]]
                        (compare x y))))
    (is (every? #{1} (for [x [::C ::D ::E]
                           y [::A ::B]]
                       (compare x y))))
    (is (every? #{-1} (for [x [::A ::B]
                            y [::F ::G ::H]]
                        (compare x y))))
    (is (every? #{1} (for [x [::F ::G ::H]
                           y [::A ::B]]
                       (compare x y))))))

(t/deftest in-and-out-node-test
  (let [g (deps/graph #{})]
    (is (= (deps/out-nodes g)
           #{}))
    (is (= (deps/in-nodes g)
           #{})))
  (let [g (deps/graph {::A        #{::B ::C}
                       #{::D ::C} #{::E}})]
    (is (= (deps/out-nodes g)
           #{::B ::E}))
    (is (= (deps/in-nodes g)
           #{::A ::D})))
  (let [g (deps/graph [::A #{::B ::C} #{::D ::E}])]
    (is (= (deps/out-nodes g)
           #{::D ::E}))
    (is (= (deps/in-nodes g)
           #{::A}))))
