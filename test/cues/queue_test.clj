(ns cues.queue-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [is]]
            [cues.queue :as q]
            [cues.test :as qt]
            [cues.util :as cutil]
            [taoensso.timbre :as log]

            ;; Configure logging for tests.
            [cues.log]))

(t/use-fixtures :each
  (t/join-fixtures
   [qt/with-test-indices qt/with-warn]))

(defn done?
  ([done]
   (done? done 10000))
  ([done ms]
   (deref done ms false)))

(t/deftest parse-processor-impl-test
  (let [parse (partial s/conform ::q/processor-impl)]
    (is (= (parse {:id  ::processor
                   :fn  identity
                   :out ::out})
           (parse {:id  ::processor
                   :fn  identity
                   :out [::out]})
           [::q/source
            {:id  ::processor
             :fn  identity
             :out ::out}]))
    (is (= (parse {:id ::processor
                   :fn identity
                   :in ::in})
           (parse {:id ::processor
                   :fn identity
                   :in [::in]})
           [::q/sink
            {:id ::processor
             :fn identity
             :in ::in}]))
    (is (= (parse {:id  ::processor
                   :fn  identity
                   :in  ::in
                   :out ::out})
           (parse {:id  ::processor
                   :fn  identity
                   :in  [::in]
                   :out [::out]})
           [::q/join
            {:id  ::processor
             :fn  identity
             :in  ::in
             :out ::out}]))
    (is (= (parse {:id  ::processor
                   :fn  identity
                   :in  ::in
                   :out [::out-1 ::out-2]})
           [::q/join-fork
            {:id  ::processor
             :fn  identity
             :in  ::in
             :out [::out-1 ::out-2]}]))))

(defmethod q/processor ::map-reduce
  [{{r-fn     :reduce-fn
     m-fn     :map-fn
     bindings :to
     :or      {m-fn identity}} :opts} msgs]
  (let [result {:x (->> (vals msgs)
                        (map (comp m-fn :x))
                        (reduce r-fn))}]
    (zipmap bindings (repeat result))))

(defmethod q/processor ::done
  [{{:keys [done x-n]} :opts
    system             :system} {{x :x} :in}]
  (when (and (= x x-n) done)
    (deliver done (or system true))))

(t/deftest graph-system-test
  (let [done (promise)]
    (qt/with-graph-and-delete
      [g {:id         ::graph
          :processors [{:id ::s1}
                       {:id   ::done
                        :in   {:in ::s1}
                        :opts {:done done
                               :x-n  1}}]
          :system     {:component :running}}]
      (q/send! g ::s1 {:x 1})
      (is (done? done))
      (is (= @done {:component :running})))))

(t/deftest graph-source-sink-test
  (let [done (promise)]
    (qt/with-graph-and-delete
      [g {:id         ::graph
          :processors [{:id ::s1}
                       {:id   ::done
                        :in   {:in ::s1}
                        :opts {:done done
                               :x-n  3}}]}]
      (q/send! g ::s1 {:x 1})
      (q/send! g ::s1 {:x 2})
      (q/send! g ::s1 {:x 3})
      (is (done? done))
      (is (= (q/all-graph-messages g)
             {::qt/error []
              ::s1       [{:x 1 :q/meta {:q/queue {::s1 {:q/t 1}}}}
                          {:x 2 :q/meta {:q/queue {::s1 {:q/t 2}}}}
                          {:x 3 :q/meta {:q/queue {::s1 {:q/t 3}}}}]})))))

(t/deftest graph-pipe-test
  (let [done (promise)]
    (qt/with-graph-and-delete
      [g {:id         ::graph
          :processors [{:id ::s1}
                       {:id   ::map-reduce
                        :in   {:in ::s1}
                        :out  {:out ::q1}
                        :opts {:map-fn inc
                               :to     [:out]}}
                       {:id   ::done
                        :in   {:in ::q1}
                        :opts {:done done
                               :x-n  4}}]
          :queue-opts {::s1 {:queue-meta #{:q/t :tx/t}}}}]
      (q/send! g ::s1 {:x 1})
      (q/send! g ::s1 {:x 2})
      (q/send! g ::s1 {:x 3})
      (is (done? done))
      (is (= (q/all-graph-messages g)
             {::qt/error []
              ::s1       [{:x 1 :q/meta {:tx/t 1 :q/queue {::s1 {:q/t 1}}}}
                          {:x 2 :q/meta {:tx/t 2 :q/queue {::s1 {:q/t 2}}}}
                          {:x 3 :q/meta {:tx/t 3 :q/queue {::s1 {:q/t 3}}}}]
              ::q1       [{:x 2 :q/meta {:tx/t 1 :q/queue {::s1 {:q/t 1}
                                                           ::q1 {:q/t 1}}}}
                          {:x 3 :q/meta {:tx/t 2 :q/queue {::s1 {:q/t 2}
                                                           ::q1 {:q/t 2}}}}
                          {:x 4 :q/meta {:tx/t 3 :q/queue {::s1 {:q/t 3}
                                                           ::q1 {:q/t 3}}}}]})))))

(t/deftest graph-alts-test
  (let [d1   (promise)
        d2   (promise)
        d3   (promise)
        done (promise)]
    (qt/with-graph-and-delete
      [g {:id         ::graph
          :processors [{:id ::s1}
                       {:id ::s2}
                       {:id   ::alts
                        :fn   ::map-reduce
                        :in   {:q1 ::s1
                               :q2 ::s2}
                        :out  {:out ::q1}
                        :opts {:alts   true
                               :map-fn (fn [x]
                                         (case x
                                           1 (deliver d1 true)
                                           2 (deliver d2 true)
                                           3 (deliver d3 true)
                                           true)
                                         (inc x))
                               :to     [:out]}}
                       {:id   ::done
                        :in   {:in ::q1}
                        :opts {:done done
                               :x-n  4}}]}]
      (q/send! g ::s1 {:x 1})
      (is (done? d1))
      (q/send! g ::s2 {:x 2})
      (is (done? d2))
      (q/send! g ::s2 {:x 3})
      (is (done? d3))
      (q/send! g ::s1 {:x 4})
      (is (done? done))
      (is (= (q/all-graph-messages g)
             {::qt/error []
              ::s1       [{:x 1 :q/meta {:q/queue {::s1 {:q/t 1}}}}
                          {:x 4 :q/meta {:q/queue {::s1 {:q/t 2}}}}]
              ::s2       [{:x 2 :q/meta {:q/queue {::s2 {:q/t 1}}}}
                          {:x 3 :q/meta {:q/queue {::s2 {:q/t 2}}}}]
              ::q1       [{:x 2 :q/meta {:q/queue {::s1 {:q/t 1}
                                                   ::q1 {:q/t 1}}}}
                          {:x 3 :q/meta {:q/queue {::s2 {:q/t 1}
                                                   ::q1 {:q/t 2}}}}
                          {:x 4 :q/meta {:q/queue {::s2 {:q/t 2}
                                                   ::q1 {:q/t 3}}}}
                          {:x 5 :q/meta {:q/queue {::s1 {:q/t 2}
                                                   ::q1 {:q/t 4}}}}]})))))

(defn- preserve-meta
  [in out]
  (assoc out :q/meta (:q/meta in)))

(defmethod q/processor ::imperative
  [{{:keys [a]} :appenders} {msg :in}]
  (->> {:x (inc (:x msg))}
       (preserve-meta msg)
       (q/write a))
  nil)

(t/deftest graph-imperative-test
  (let [done (promise)]
    (qt/with-graph-and-delete
      [g {:id         ::graph
          :processors [{:id ::s1}
                       {:id        ::imperative
                        :in        {:in ::s1}
                        :appenders {:a ::q1}}
                       {:id   ::done
                        :in   {:in ::q1}
                        :opts {:done done
                               :x-n  4}}]
          :queue-opts {::s1 {:queue-meta #{:q/t :tx/t}}}}]
      (q/send! g ::s1 {:x 1})
      (q/send! g ::s1 {:x 2})
      (q/send! g ::s1 {:x 3})
      (is (done? done))
      (is (= (q/all-graph-messages g)
             {::qt/error []
              ::s1       [{:x 1 :q/meta {:tx/t 1 :q/queue {::s1 {:q/t 1}}}}
                          {:x 2 :q/meta {:tx/t 2 :q/queue {::s1 {:q/t 2}}}}
                          {:x 3 :q/meta {:tx/t 3 :q/queue {::s1 {:q/t 3}}}}]
              ::q1       [{:x 2 :q/meta {:tx/t 1 :q/queue {::s1 {:q/t 1}
                                                           ::q1 {:q/t 1}}}}
                          {:x 3 :q/meta {:tx/t 2 :q/queue {::s1 {:q/t 2}
                                                           ::q1 {:q/t 2}}}}
                          {:x 4 :q/meta {:tx/t 3 :q/queue {::s1 {:q/t 3}
                                                           ::q1 {:q/t 3}}}}]})))))

(t/deftest graph-join-test
  (let [done (promise)]
    (qt/with-graph-and-delete
      [g {:id         ::graph
          :processors [{:id ::s1}
                       {:id ::s2}
                       {:id   ::map-reduce
                        :in   {:in-1 ::s1
                               :in-2 ::s2}
                        :out  {:out ::q1}
                        :opts {:reduce-fn +
                               :map-fn    inc
                               :to        [:out]}}
                       {:id   ::done
                        :in   {:in ::q1}
                        :opts {:done done
                               :x-n  9}}]
          :queue-opts {::q1 {:queue-meta #{:q/t :tx/t}}}}]
      (q/send! g ::s1 {:x 1})
      (q/send! g ::s1 {:x 3})
      (q/send! g ::s2 {:x 2})
      (q/send! g ::s2 {:x 4})
      (is (done? done))
      (is (= (q/all-graph-messages g)
             {::qt/error []
              ::s1       [{:x 1 :q/meta {:q/queue {::s1 {:q/t 1}}}}
                          {:x 3 :q/meta {:q/queue {::s1 {:q/t 2}}}}]
              ::s2       [{:x 2 :q/meta {:q/queue {::s2 {:q/t 1}}}}
                          {:x 4 :q/meta {:q/queue {::s2 {:q/t 2}}}}]
              ::q1       [{:x 5 :q/meta {:tx/t    1
                                         :q/queue {::s1 {:q/t 1}
                                                   ::s2 {:q/t 1}
                                                   ::q1 {:q/t 1}}}}
                          {:x 9 :q/meta {:tx/t    2
                                         :q/queue {::s1 {:q/t 2}
                                                   ::s2 {:q/t 2}
                                                   ::q1 {:q/t 2}}}}]})))))

(t/deftest graph-join-fork-test
  (let [q1-done (promise)
        q2-done (promise)]
    (qt/with-graph-and-delete
      [g {:id         ::graph
          :processors [{:id ::s1}
                       {:id ::s2}
                       {:id   ::map-reduce
                        :opts {:reduce-fn +
                               :to        [:out-1 :out-2]}
                        :in   {:in-1 ::s1
                               :in-2 ::s2}
                        :out  {:out-1 ::q1
                               :out-2 ::q2}}
                       {:id   ::k1
                        :fn   ::done
                        :in   {:in ::q1}
                        :opts {:done q1-done
                               :x-n  7}}
                       {:id   ::k2
                        :fn   ::done
                        :in   {:in ::q2}
                        :opts {:done q2-done
                               :x-n  7}}]
          :queue-opts {::s1 {:queue-meta #{:q/t :tx/t}}}}]
      (q/send! g ::s1 {:x 1})
      (q/send! g ::s1 {:x 3})
      (q/send! g ::s2 {:x 2})
      (q/send! g ::s2 {:x 4})
      (is (and (done? q1-done)
               (done? q2-done)))
      (is (= (q/all-graph-messages g)
             {::qt/error []
              ::s1       [{:x 1 :q/meta {:tx/t 1 :q/queue {::s1 {:q/t 1}}}}
                          {:x 3 :q/meta {:tx/t 2 :q/queue {::s1 {:q/t 2}}}}]
              ::s2       [{:x 2 :q/meta {:q/queue {::s2 {:q/t 1}}}}
                          {:x 4 :q/meta {:q/queue {::s2 {:q/t 2}}}}]
              ::q1       [{:x 3 :q/meta {:tx/t    1
                                         :q/queue {::s1 {:q/t 1}
                                                   ::s2 {:q/t 1}
                                                   ::q1 {:q/t 1}}}}
                          {:x 7 :q/meta {:tx/t    2
                                         :q/queue {::s1 {:q/t 2}
                                                   ::s2 {:q/t 2}
                                                   ::q1 {:q/t 2}}}}]
              ::q2       [{:x 3 :q/meta {:tx/t    1
                                         :q/queue {::s1 {:q/t 1}
                                                   ::s2 {:q/t 1}
                                                   ::q2 {:q/t 1}}}}
                          {:x 7 :q/meta {:tx/t    2
                                         :q/queue {::s1 {:q/t 2}
                                                   ::s2 {:q/t 2}
                                                   ::q2 {:q/t 2}}}}]})))))

(defmethod q/processor ::join-fork-conditional
  [_ msgs]
  (let [n   (transduce (map :x) + (vals msgs))
        msg {:x n}]
    {:even (when (even? n) msg)
     :odd  (when (odd? n) msg)}))

(t/deftest graph-join-fork-conditional-test
  (let [q1-done (promise)
        q2-done (promise)]
    (qt/with-graph-and-delete
      [g {:id         ::graph
          :processors [{:id ::s1}
                       {:id ::s2}
                       {:id  ::join-fork-conditional
                        :in  {:in-1 ::s1
                              :in-2 ::s2}
                        :out {:even ::q1
                              :odd  ::q2}}
                       {:id   ::k1
                        :fn   ::done
                        :in   {:in ::q1}
                        :opts {:done q1-done
                               :x-n  2}}
                       {:id   ::k2
                        :fn   ::done
                        :in   {:in ::q2}
                        :opts {:done q2-done
                               :x-n  5}}]
          :queue-opts {::s1 {:queue-meta #{:q/t :tx/t}}}}]
      (q/send! g ::s1 {:x 1})
      (q/send! g ::s1 {:x 2})
      (q/send! g ::s2 {:x 1})
      (q/send! g ::s2 {:x 3})
      (is (and (done? q1-done)
               (done? q2-done)))
      (is (= (q/all-graph-messages g)
             {::qt/error []
              ::s1       [{:x 1 :q/meta {:tx/t 1 :q/queue {::s1 {:q/t 1}}}}
                          {:x 2 :q/meta {:tx/t 2 :q/queue {::s1 {:q/t 2}}}}]
              ::s2       [{:x 1 :q/meta {:q/queue {::s2 {:q/t 1}}}}
                          {:x 3 :q/meta {:q/queue {::s2 {:q/t 2}}}}]
              ::q1       [{:x 2 :q/meta {:tx/t    1
                                         :q/queue {::s1 {:q/t 1}
                                                   ::s2 {:q/t 1}
                                                   ::q1 {:q/t 1}}}}]
              ::q2       [{:x 5 :q/meta {:tx/t    2
                                         :q/queue {::s1 {:q/t 2}
                                                   ::s2 {:q/t 2}
                                                   ::q2 {:q/t 1}}}}]})))))

(defmethod q/processor ::pipe-error
  [_ {msg :in}]
  (if (even? (:x msg))
    {:out msg}
    (throw (Exception. "Oops"))))

(t/deftest graph-error-test
  ;; Suppress exception logging, just testing messaging
  (log/with-level :fatal
    (let [done (promise)]
      (qt/with-graph-and-delete
        [g {:id         ::graph
            :processors [{:id ::s1}
                         {:id  ::pipe-error
                          :in  {:in ::s1}
                          :out {:out ::q1}}
                         {:id   ::done
                          :in   {:in ::q1}
                          :opts {:done done
                                 :x-n  4}}]}]
        (q/send! g ::s1 {:x 1})
        (q/send! g ::s1 {:x 2})
        (q/send! g ::s1 {:x 3})
        (q/send! g ::s1 {:x 4})
        (is (done? done))
        (is (= (-> (q/all-graph-messages g)
                   (update ::qt/error qt/simplify-exceptions))
               {::qt/error [{:q/type            :q.type.err/processor
                             :err/cause         {:cause "Oops"}
                             :err.proc/config   {:id         ::pipe-error
                                                 :in         ::s1
                                                 :out        ::q1
                                                 :queue-opts {:queue-meta #{:q/t}}
                                                 :strategy   ::q/exactly-once}
                             :err.proc/messages {::s1 {:x      1
                                                       :q/meta {:q/queue {::s1 {:q/t 1}}}}}
                             :q/meta            {:q/queue {::qt/error {:q/t 1}}}}
                            {:q/type            :q.type.err/processor
                             :err/cause         {:cause "Oops"}
                             :err.proc/config   {:id         ::pipe-error
                                                 :in         ::s1
                                                 :out        ::q1
                                                 :queue-opts {:queue-meta #{:q/t}}
                                                 :strategy   ::q/exactly-once}
                             :err.proc/messages {::s1 {:x      3
                                                       :q/meta {:q/queue {::s1 {:q/t 3}}}}}
                             :q/meta            {:q/queue {::qt/error {:q/t 2}}}}]
                ::s1       [{:x 1 :q/meta {:q/queue {::s1 {:q/t 1}}}}
                            {:x 2 :q/meta {:q/queue {::s1 {:q/t 2}}}}
                            {:x 3 :q/meta {:q/queue {::s1 {:q/t 3}}}}
                            {:x 4 :q/meta {:q/queue {::s1 {:q/t 4}}}}]
                ::q1       [{:x 2 :q/meta {:q/queue {::s1 {:q/t 2}
                                                     ::q1 {:q/t 1}}}}
                            {:x 4 :q/meta {:q/queue {::s1 {:q/t 4}
                                                     ::q1 {:q/t 2}}}}]}))))))

(t/deftest filter-messages-test
  (let [msg {::q {:q/topics {::t1 ::message
                             ::t2 ::message}}}]
    (is (= (#'q/topics-filter nil nil)))
    (is (= (#'q/topics-filter nil {})))
    (is (= (#'q/topics-filter [] nil)))
    (is (= (#'q/topics-filter [::t1 ::t2] msg)
           msg))
    (is (= (#'q/topics-filter [::t1] msg)
           {::q {:q/topics {::t1 ::message}}}))
    (is (= (#'q/topics-filter [::other] msg)
           nil))
    (is (= (#'q/topics-filter [::t1 ::t2]
                              {::q1 {:q/topics {::t1 ::message
                                                ::t2 ::message}}
                               ::q2 {:q/topics {::t1 ::message
                                                ::t2 ::message}}})
           {::q1 {:q/topics {::t1 ::message
                             ::t2 ::message}}
            ::q2 {:q/topics {::t1 ::message
                             ::t2 ::message}}}))
    (is (= (#'q/topics-filter [::t1]
                              {::q1 {:q/topics {::t1 ::message
                                                ::t2 ::message}}
                               ::q2 {:q/topics {::t1 ::message
                                                ::t2 ::message}}})
           {::q1 {:q/topics {::t1 ::message}}
            ::q2 {:q/topics {::t1 ::message}}}))
    (is (= (#'q/topics-filter [::t2]
                              {::q1 {:q/topics {::t2 ::message}}
                               ::q2 {:q/topics {::t1 ::message}}})
           {::q1 {:q/topics {::t2 ::message}}}))
    (is (= (#'q/topics-filter [::t1]
                              {::q1 {:q/topics {::t2 ::message}}
                               ::q2 {:q/topics {::t1 ::message}}})
           {::q2 {:q/topics {::t1 ::message}}}))
    (is (= (#'q/topics-filter [::t2]
                              {::q1 {:q/topics {::t1 ::message}}
                               ::q2 {:q/topics {::t1 ::message}}})))))

(defmethod q/processor ::processor-a
  [{:keys [system]} {msg :in}]
  {:out (assoc msg
               :processed true
               :system system)})

(t/deftest processor-fn-test
  (let [config  {:id     ::processor-a
                 :topics [::doc]
                 :in     {:in         ::q1
                          :in-ignored ::q2}
                 :out    {:out ::tx}}
        f       (#'q/processor->fn config)
        process {:config config
                 :system {:component true}}]
    (is (fn? f))
    (is (nil? (f process nil)))
    (is (nil? (f process {})))
    (is (nil? (f process {::q-other {}})))
    (is (nil? (f process {::q-other {:q/topics {}}})))
    (is (nil? (f process {::q-other {:q/topics {::other ::message}}})))
    (is (nil? (f process {::q1 {:q/topics {::other ::message}}})))
    (is (= (f process {::q1 {:q/topics {::doc ::message}}})
           {::tx {:q/topics  {::doc ::message}
                  :processed true
                  :system    {:component true}}}))))

(defn- process-b
  [doc]
  (assoc doc ::processed true))

(defmethod q/processor ::processor-b
  [_ {msg :in
      _   :in-ignored}]
  {:out (->> process-b
             (partial cutil/map-vals)
             (update msg :q/topics))})

(defmethod q/processor ::doc-store
  [{{db   :db
     done :done} :opts}
   {{{{id  :system/uuid
       :as doc} ::doc} :q/topics
     :as               msg} :in}]
  (swap! db update id merge doc)
  (deliver done true))

(defn message
  [topics]
  {:q/type   :q.type.tx/command
   :q/topics topics})

(t/deftest graph-filters-test
  (let [done (promise)
        db   (atom {})]
    (qt/with-graph-and-delete
      [g {:id         ::graph
          :queue-opts {::tx {:queue-meta #{:q/t :tx/t}}}
          :processors [{:id ::s1}
                       {:id ::s2}
                       {:id    ::processor-b
                        :types :q.type.tx/command
                        :in    {:in         ::s1
                                :in-ignored ::s2}
                        :out   {:out ::tx}}
                       {:id     ::doc-store
                        :topics ::doc
                        :in     {:in ::tx}
                        :opts   {:db   db
                                 :done done}}]}]
      (q/send! g ::s1 (message {::other {:system/uuid 1 :text "no dice"}}))
      (q/send! g ::s2 (message {::other {:system/uuid 2 :text "no dice"}}))
      (q/send! g ::s1 (message {::doc {:system/uuid 3 :text "text"}}))
      (q/send! g ::s2 (message {::other {:system/uuid 4 :text "no dice"}}))
      (is (done? done))
      (is (= @db
             {3 {:system/uuid 3
                 ::processed  true
                 :text        "text"}}))
      (is (= (q/all-graph-messages g)
             {::qt/error []
              ::s1       [{:q/type   :q.type.tx/command
                           :q/topics {::other {:system/uuid 1 :text "no dice"}}
                           :q/meta   {:q/queue {::s1 {:q/t 1}}}}
                          {:q/type   :q.type.tx/command
                           :q/topics {::doc {:system/uuid 3 :text "text"}}
                           :q/meta   {:q/queue {::s1 {:q/t 2}}}}]
              ::s2       [{:q/type   :q.type.tx/command
                           :q/topics {::other {:system/uuid 2 :text "no dice"}}
                           :q/meta   {:q/queue {::s2 {:q/t 1}}}}
                          {:q/type   :q.type.tx/command
                           :q/topics {::other {:system/uuid 4 :text "no dice"}}
                           :q/meta   {:q/queue {::s2 {:q/t 2}}}}]
              ::tx       [{:q/type   :q.type.tx/command
                           :q/topics {::other {:system/uuid 1 :text "no dice"
                                               ::processed  true}}
                           :q/meta   {:q/queue {::s1 {:q/t 1}
                                                ::s2 {:q/t 1}
                                                ::tx {:q/t 1}}
                                      :tx/t    1}}
                          {:q/type   :q.type.tx/command
                           :q/topics {::doc {:system/uuid 3 :text "text"
                                             ::processed  true}}
                           :q/meta   {:q/queue {::s1 {:q/t 2}
                                                ::s2 {:q/t 2}
                                                ::tx {:q/t 2}}
                                      :tx/t    2}}]})))))

(defmethod q/processor ::interruptible
  [{{:keys [done throw?]} :opts} {msg :in}]
  (try
    (when throw? (q/throw-interrupt!))
    (finally
      (when done
        (deliver done true))))
  {:out msg})

(t/deftest exactly-once-test
  ;; First test throws an uncaught interrupt exception. Using
  ;; the :default message semantics the message is delivered when the
  ;; graph is restarted.
  (let [done (promise)]
    (let [g (->> {:id         ::graph
                  :processors [{:id ::s1}
                               {:id   ::interruptible
                                :in   {:in ::s1}
                                :out  {:out ::tx}
                                :opts {:throw? true
                                       :done   done}}]}
                 (q/graph)
                 (q/start-graph!))]
      (q/send! g ::s1 {:x 1})
      (q/send! g ::s1 {:x 2})
      (is (done? done))
      (is (= (q/all-graph-messages g)
             {::s1 [{:x 1} {:x 2}]
              ::tx []}))
      (q/stop-graph! g)))

  (let [done (promise)]
    (let [g (->> {:id         ::graph
                  :processors [{:id ::s1}
                               {:id  ::interruptible
                                :in  {:in ::s1}
                                :out {:out ::tx}}
                               {:id   ::done
                                :in   {:in ::tx}
                                :opts {:done done
                                       :x-n  2}}]}
                 (q/graph)
                 (q/start-graph!))]
      (is (done? done))
      (is (= (q/all-graph-messages g)
             {::s1 [{:x 1} {:x 2}]
              ::tx [{:x 1} {:x 2}]}))
      (q/close-and-delete-graph! g true))))

(defmethod q/processor ::done-counter
  [{{:keys [done counter n]} :opts} _]
  (when (= n (swap! counter inc))
    (deliver done true)))

(def stress-fixtures
  (t/join-fixtures [qt/with-warn]))

(defn stress-test
  [n]
  (stress-fixtures
   (fn []
     (let [q1-done (promise)
           q2-done (promise)
           timeout (max (/ n 10) 1000)]
       (qt/with-graph-and-delete
         [g {:id         ::graph
             :queue-opts {::q/default {:queue-meta false}}
             :processors [{:id ::s1}
                          {:id ::s2}
                          {:id   ::map-reduce
                           :in   {:in-1 ::s1
                                  :in-2 ::s2}
                           :out  {:out-1 ::q1
                                  :out-2 ::q2}
                           :opts {:reduce-fn +
                                  :to        [:out-1 :out-2]}}
                          {:id   ::k1
                           :fn   ::done-counter
                           :in   {:in ::q1}
                           :opts {:done    q1-done
                                  :counter (atom 0)
                                  :n       n}}
                          {:id   ::k2
                           :fn   ::done-counter
                           :in   {:in ::q2}
                           :opts {:done    q2-done
                                  :counter (atom 0)
                                  :n       n}}]}]
         {:success? (time
                      (do
                        (time
                          (dotimes [n n]
                            (q/send! g ::s1 {:x n})
                            (q/send! g ::s2 {:x n})))
                        (and (done? q1-done timeout)
                             (done? q2-done timeout))))
          :counts   (->> g
                         (:queues)
                         (cutil/map-vals (comp count q/all-messages)))})))))

(comment
  ;; This measures a single round-trip read + write, with
  ;; blocking. The messages being serialized on these queues are
  ;; generally between 114-117 Bytes in size.

  (b/quick-bench
   (q/write a {:x 1}))

  "Evaluation count : 865068 in 6 samples of 144178 calls.
               Execution time mean : 690.932746 ns
      Execution time std-deviation : 7.290161 ns
     Execution time lower quantile : 683.505105 ns ( 2.5%)
     Execution time upper quantile : 698.417843 ns (97.5%)
                     Overhead used : 2.041010 ns"

  (b/quick-bench
   (do (q/write a {:x 1})
       (q/read!! t)))
  "Evaluation count : 252834 in 6 samples of 42139 calls.
               Execution time mean : 2.389696 µs
      Execution time std-deviation : 64.305722 ns
     Execution time lower quantile : 2.340823 µs ( 2.5%)
     Execution time upper quantile : 2.466880 µs (97.5%)
                     Overhead used : 2.035620 ns"

  (qt/stress-test 1000000)
  "Elapsed time: 5766.9555 msecs"
  "Elapsed time: 32252.300833 msecs"
  {:success? true
   :counts   {:cues.test/error    0
              :cues.queue-test/s1 1000000
              :cues.queue-test/s2 1000000
              :cues.queue-test/q1 1000000
              :cues.queue-test/q2 1000000}})
