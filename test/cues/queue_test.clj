(ns cues.queue-test
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [is]]
            [cues.error :as err]
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
   (done? done 1000))
  ([done ms]
   (deref done ms false)))

(defn throw-interrupt!
  []
  (throw (InterruptedException. "Interrupted processor")))

(t/deftest parse-processor-impl-test
  (let [parse #(first (s/conform ::q/processor %))]
    (is (= (parse {:id ::source})
           ::q/source))
    (is (= (parse {:id ::processor
                   :in {:in ::in}})
           ::q/sink))
    (is (= (parse {:id  ::processor
                   :in  {:in ::in}
                   :out {:out ::out}})
           ::q/join))
    (is (= (parse {:id  ::processor
                   :in  {:in-1 ::in-1
                         :in-2 ::in-2}
                   :out {:out-1 ::out-1}})
           ::q/join))
    (is (= (parse {:id  ::processor
                   :in  {:in ::in}
                   :out {:out-1 ::out-1
                         :out-2 ::out-2}})
           ::q/join-fork))
    (is (= (parse {:id  ::processor
                   :in  {:in-1 ::in-1
                         :in-2 ::in-2}
                   :out {:out-1 ::out-1
                         :out-2 ::out-2}})
           ::q/join-fork))
    (is (= (parse {:id        ::processor
                   :in        {:in ::in}
                   :out       {:out-1 ::out-1}
                   :appenders {:a ::appenders}})
           ::q/imperative))
    (is (= (parse {:id        ::processor
                   :in        {:in ::in}
                   :out       {:out-1 ::out-1}
                   :tailers   {:t ::tailer}})
           ::q/imperative))
    (is (= (parse {:id        ::processor
                   :in        {:in ::in}
                   :out       {:out-1 ::out-1}
                   :tailers   {:t ::tailer}
                   :appenders {:a ::appenders}})
           ::q/imperative))))

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

(defmethod q/processor ::done-counter
  [{{:keys [done counter n]} :opts} _]
  (when (= n (swap! counter inc))
    (deliver done true)))

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
                        :alts {:q1 ::s1
                               :q2 ::s2}
                        :out  {:out ::q1}
                        :opts {:map-fn (fn [x]
                                         (case x
                                           1 (deliver d1 true)
                                           2 (deliver d2 true)
                                           3 (deliver d3 true)
                                           true)
                                         (inc x))
                               :to     [:out]}}
                       {:id   ::done-counter
                        :in   {:in ::q1}
                        :opts {:done    done
                               :counter (atom 0)
                               :n       4}}]}]
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
    (let [done        (promise)
          done-errors (promise)]
      (qt/with-graph-and-delete
        [g {:id         ::graph
            :processors [{:id ::s1}
                         {:id  ::pipe-error
                          :in  {:in ::s1}
                          :out {:out ::q1}}
                         {:id   ::d1
                          :fn   ::done
                          :in   {:in ::q1}
                          :opts {:done done
                                 :x-n  4}}
                         {:id   ::d2
                          :fn   ::done-counter
                          :in   {:in ::qt/error}
                          :opts {:done    done-errors
                                 :counter (atom 0)
                                 :n       2}}]}]
        (q/send! g ::s1 {:x 1})
        (q/send! g ::s1 {:x 2})
        (q/send! g ::s1 {:x 3})
        (q/send! g ::s1 {:x 4})
        (is (done? done))
        (is (done? done-errors))
        (is (= (-> (q/all-graph-messages g)
                   (update ::qt/error qt/simplify-exceptions))
               {::qt/error [{:q/type            :q.type.err/processor
                             :err/cause         {:cause "Oops"}
                             :err.proc/config   {:id       ::pipe-error
                                                 :in       {:in ::s1}
                                                 :out      {:out ::q1}
                                                 :strategy ::q/exactly-once}
                             :err.proc/messages {::s1 {:x      1
                                                       :q/meta {:q/queue {::s1 {:q/t 1}}}}}
                             :q/meta            {:q/queue {::qt/error {:q/t 1}}}}
                            {:q/type            :q.type.err/processor
                             :err/cause         {:cause "Oops"}
                             :err.proc/config   {:id       ::pipe-error
                                                 :in       {:in ::s1}
                                                 :out      {:out ::q1}
                                                 :strategy ::q/exactly-once}
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
           {::q {:q/topics {::t1 ::message
                            ::t2 ::message}}}))
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
           {::q1 {:q/topics {::t1 ::message
                             ::t2 ::message}}
            ::q2 {:q/topics {::t1 ::message
                             ::t2 ::message}}}))
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
                               ::q2 {:q/topics {::t1 ::message}}})
           nil))))

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
        f       (#'q/get-processor-fn {:config config})
        process {:config config
                 :system {:component true}}]
    (is (fn? f))
    (is (nil? (f process nil)))
    (is (nil? (f process {})))
    (is (nil? (f process {::q1 {}})))
    (is (nil? (f process {::q1 {:q/topics nil}})))
    (is (nil? (f process {::q1 {:q/topics false}})))
    (is (nil? (f process {::q1 {:q/topics {}}})))
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
     {t :tx/t}         :q/meta
     :as               msg} :in}]
  (swap! db update id merge doc)
  (when (= t 2)
    (deliver done true)))

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

;; The tests beyond this point deal with implementation details of
;; persistence and message delivery semantics. Specifically they lift
;; processor backing queues into the graph so that they can be
;; inspected and managed concurrently. For this reason they should not
;; be taken as examples of normal usage.

(defmethod q/processor ::interrupt
  [{{:keys [done interrupt?]} :opts} {msg :in}]
  (when interrupt?
    (try
      (throw-interrupt!)
      (finally (deliver done true))))
  {:out msg})

(defn try-messages
  [g]
  (->> (:processors g)
       (vals)
       (mapcat q/get-try-queues)
       (map (juxt :id q/all-messages))
       (into {})))

(t/deftest exactly-once-processor-unhandled-interrupt-test
  ;; The processor :fn throws an unhandled interrupt during the method
  ;; execution. The processor quits due to the unhandled
  ;; interrupt. Without exactly once semantics the first message would
  ;; be lost. However, using exactly once semantics the message is
  ;; persisted after the graph is restarted.
  (let [p-id  :cues.queue-test.graph.cues.queue-test/interrupt
        p-tid :cues.queue-test.graph.cues.queue-test.interrupt.cues.queue-test/s1
        d-id  :cues.queue-test.graph.cues.queue-test/done
        d-tid :cues.queue-test.graph.cues.queue-test.done.cues.queue-test/q1]
    (let [done-1 (promise)
          done-2 (promise)
          g      (->> {:id         ::graph
                       :processors [{:id ::s1}
                                    {:id   ::interrupt
                                     :in   {:in ::s1}
                                     :out  {:out ::q1}
                                     :opts {:interrupt? true
                                            :done       done-1}}
                                    {:id   ::d2
                                     :fn   ::done-counter
                                     :in   {:in p-id}
                                     :opts {:done    done-2
                                            :counter (atom 0)
                                            :n       1}}]}
                      (q/graph)
                      (q/start-graph!))]
      (q/send! g ::s1 {:x 1})
      (q/send! g ::s1 {:x 2})
      (is (done? done-1))
      (is (done? done-2))
      (is (= (q/all-graph-messages g)
             {::s1 [{:x 1} {:x 2}]
              ::q1 []
              p-id [{:q/type           :q.type/snapshot
                     :q/proc-id        p-id
                     :q/tailer-indices {p-tid 1}}]}))
      (q/stop-graph! g)
      (q/close-graph! g))

    ;; After restarting the graph both messages are delivered, and we
    ;; can see the subsequent attempt in the backing queue.
    (let [done-1 (promise)
          done-2 (promise)
          done-3 (promise)
          g      (->> {:id         ::graph
                       :processors [{:id ::s1}
                                    {:id  ::interrupt
                                     :in  {:in ::s1}
                                     :out {:out ::q1}}
                                    {:id   ::done
                                     :in   {:in ::q1}
                                     :opts {:done done-1
                                            :x-n  2}}
                                    {:id   ::d2
                                     :fn   ::done-counter
                                     :in   {:in p-id}
                                     :opts {:done    done-2
                                            :counter (atom 0)
                                            :n       4}} ; 4 + 1 from previous graph
                                    {:id   ::d3
                                     :fn   ::done-counter
                                     :in   {:in d-id}
                                     :opts {:done    done-3
                                            :counter (atom 0)
                                            :n       5}}]}
                      (q/graph)
                      (q/start-graph!))]
      (is (done? done-1))
      (is (done? done-2))
      (is (done? done-3))
      (is (= (q/all-graph-messages g)
             {::s1 [{:x 1} {:x 2}]
              ::q1 [{:x 1} {:x 2}]
              p-id [{:q/type           :q.type/snapshot
                     :q/proc-id        p-id
                     :q/tailer-indices {p-tid 1}}
                    {:q/type          :q.type/attempt-output
                     :q/message-index 1}
                    {:q/type           :q.type/snapshot
                     :q/proc-id        p-id
                     :q/tailer-indices {p-tid 2}}
                    {:q/type          :q.type/attempt-output
                     :q/message-index 2}
                    {:q/type           :q.type/snapshot
                     :q/proc-id        p-id
                     :q/tailer-indices {p-tid 3}}]
              d-id [{:q/type           :q.type/snapshot
                     :q/proc-id        d-id
                     :q/tailer-indices {d-tid 1}}
                    {:q/type :q.type/attempt-nil
                     :q/hash -1173148930}
                    {:q/type           :q.type/snapshot
                     :q/proc-id        d-id
                     :q/tailer-indices {d-tid 2}}
                    {:q/type :q.type/attempt-nil
                     :q/hash 1121832113}
                    {:q/type           :q.type/snapshot
                     :q/proc-id        d-id
                     :q/tailer-indices {d-tid 3}}]}))
      (q/stop-graph! g)
      (q/close-and-delete-graph! g true))))

(defmethod q/processor ::interrupt-alts
  [{{:keys [done interrupt?]} :opts} msgs]
  (when interrupt?
    (try
      (throw-interrupt!)
      (finally (deliver done true))))
  {:out (first (vals msgs))})

(t/deftest exactly-once-processor-unhandled-interrupt-alts-test
  ;; The alts processor :fn throws an unhandled interrupt during the
  ;; method execution. The processor quits due to the unhandled
  ;; interrupt. Without exactly once semantics the first message would
  ;; be lost. However, using exactly once semantics the message is
  ;; persisted after the graph is restarted.
  (let [p-id   :cues.queue-test.graph.cues.queue-test/interrupt-alts
        p-tid1 :cues.queue-test.graph.cues.queue-test.interrupt-alts.cues.queue-test/s1
        p-tid2 :cues.queue-test.graph.cues.queue-test.interrupt-alts.cues.queue-test/s2
        d-id   :cues.queue-test.graph.cues.queue-test/done
        d-tid  :cues.queue-test.graph.cues.queue-test.done.cues.queue-test/q1]
    (let [done-1 (promise)
          done-2 (promise)
          g      (->> {:id         ::graph
                       :processors [{:id ::s1}
                                    {:id ::s2}
                                    {:id   ::interrupt-alts
                                     :alts {:in-1 ::s1
                                            :in-2 ::s2}
                                     :out  {:out ::q1}
                                     :opts {:interrupt? true
                                            :done       done-1}}
                                    {:id   ::d2
                                     :fn   ::done-counter
                                     :in   {:in p-id}
                                     :opts {:done    done-2
                                            :counter (atom 0)
                                            :n       2}}]}
                      (q/graph)
                      (q/start-graph!))]
      (q/send! g ::s2 {:x 1})
      (is (done? done-1))
      (q/send! g ::s1 {:x 2})
      (is (done? done-2))
      (is (= (q/all-graph-messages g)
             {::s1 [{:x 2}]
              ::s2 [{:x 1}]
              ::q1 []
              p-id [{:q/type           :q.type/snapshot
                     :q/proc-id        p-id
                     :q/tailer-indices {p-tid1 1 p-tid2 1}}
                    {:q/type      :q.type/snapshot-alts
                     :q/tailer-id p-tid2}]}))
      (q/stop-graph! g)
      (q/close-graph! g))

    ;; After restarting the graph both messages are delivered, and we
    ;; can see the subsequent attempt in the backing queue.
    (let [done-1 (promise)
          done-2 (promise)
          done-3 (promise)
          g      (->> {:id         ::graph
                       :processors [{:id ::s1}
                                    {:id ::s2}
                                    {:id   ::interrupt-alts
                                     :alts {:in-2 ::s2
                                            :in-1 ::s1}
                                     :out  {:out ::q1}}
                                    {:id   ::done
                                     :in   {:in ::q1}
                                     :opts {:done done-1
                                            :x-n  2}}
                                    {:id   ::d2
                                     :fn   ::done-counter
                                     :in   {:in p-id}
                                     :opts {:done    done-2
                                            :counter (atom 0)
                                            :n       5}} ; 5 + 2 from previous graph
                                    {:id   ::d3
                                     :fn   ::done-counter
                                     :in   {:in d-id}
                                     :opts {:done    done-3
                                            :counter (atom 0)
                                            :n       5}}]}
                      (q/graph)
                      (q/start-graph!))]
      (is (done? done-1))
      (is (done? done-2))
      (is (done? done-3))
      (is (= (q/all-graph-messages g)
             {::s1 [{:x 2}]
              ::s2 [{:x 1}]
              ::q1 [{:x 1}
                    {:x 2}]
              p-id [{:q/type           :q.type/snapshot
                     :q/proc-id        p-id
                     :q/tailer-indices {p-tid1 1 p-tid2 1}}
                    {:q/type      :q.type/snapshot-alts
                     :q/tailer-id p-tid2}
                    {:q/type          :q.type/attempt-output
                     :q/message-index 1}
                    {:q/type           :q.type/snapshot
                     :q/proc-id        p-id
                     :q/tailer-indices {p-tid1 1 p-tid2 2}}
                    {:q/type      :q.type/snapshot-alts
                     :q/tailer-id p-tid1}
                    {:q/type          :q.type/attempt-output
                     :q/message-index 2}
                    {:q/type           :q.type/snapshot
                     :q/proc-id        p-id
                     :q/tailer-indices {p-tid1 2 p-tid2 2}}]
              d-id [{:q/type           :q.type/snapshot
                     :q/proc-id        d-id
                     :q/tailer-indices {d-tid 1}}
                    {:q/type :q.type/attempt-nil
                     :q/hash -1173148930}
                    {:q/type           :q.type/snapshot
                     :q/proc-id        d-id
                     :q/tailer-indices {d-tid 2}}
                    {:q/type :q.type/attempt-nil
                     :q/hash 1121832113}
                    {:q/type           :q.type/snapshot
                     :q/proc-id        d-id
                     :q/tailer-indices {d-tid 3}}]}))
      (q/stop-graph! g)
      (q/close-and-delete-graph! g true))))

(defmethod q/processor ::interrupt-fork
  [{{:keys [done interrupt?]} :opts} {:keys [in-1 in-2]}]
  (when interrupt?
    (try
      (throw-interrupt!)
      (finally (deliver done true))))
  {:out-1 in-1
   :out-2 in-2})

(t/deftest exactly-once-join-fork-unhandled-interrupt-test
  ;; Same as exactly-once-processor-unhandled-interrupt-test, but
  ;; tests exactly once delivery on join-fork intermediary processing
  ;; loops.
  (let [p-id   :cues.queue-test.graph.cues.queue-test/interrupt-fork
        p-tid1 :cues.queue-test.graph.cues.queue-test.interrupt-fork.cues.queue-test/s1
        p-tid2 :cues.queue-test.graph.cues.queue-test.interrupt-fork.cues.queue-test/s2
        p-jid1 :cues.queue-test.graph.cues.queue-test.interrupt-fork.cues.queue-test/q1
        p-jid2 :cues.queue-test.graph.cues.queue-test.interrupt-fork.cues.queue-test/q2
        p-fid1 :cues.queue-test.graph.cues.queue-test.interrupt-fork.cues.queue-test.q1.cues.queue-test.graph.cues.queue-test.interrupt-fork/fork
        p-fid2 :cues.queue-test.graph.cues.queue-test.interrupt-fork.cues.queue-test.q2.cues.queue-test.graph.cues.queue-test.interrupt-fork/fork]
    (let [done-1 (promise)
          done-2 (promise)
          done-3 (promise)
          done-4 (promise)
          g      (->> {:id         ::graph
                       :processors [{:id ::s1}
                                    {:id ::s2}
                                    {:id   ::interrupt-fork
                                     :in   {:in-1 ::s1
                                            :in-2 ::s2}
                                     :out  {:out-1 ::q1
                                            :out-2 ::q2}
                                     :opts {:interrupt? true
                                            :done       done-1}}
                                    {:id   ::d2
                                     :fn   ::done-counter
                                     :in   {:in p-id}
                                     :opts {:done    done-2
                                            :counter (atom 0)
                                            :n       1}}
                                    {:id   ::d3
                                     :fn   ::done-counter
                                     :in   {:in p-jid1}
                                     :opts {:done    done-3
                                            :counter (atom 0)
                                            :n       1}}
                                    {:id   ::d4
                                     :fn   ::done-counter
                                     :in   {:in p-jid2}
                                     :opts {:done    done-4
                                            :counter (atom 0)
                                            :n       1}}]}
                      (q/graph)
                      (q/start-graph!))]
      (q/send! g ::s1 {:x 1})
      (q/send! g ::s2 {:x 2})
      (is (done? done-1))
      (is (done? done-2))
      (is (done? done-3))
      (is (done? done-4))
      (is (= (q/all-graph-messages g)
             {::s1   [{:x 1}]
              ::s2   [{:x 2}]
              ::q1   []
              ::q2   []
              p-id   [{:q/type           :q.type/snapshot
                       :q/proc-id        p-id
                       :q/tailer-indices {p-tid1 1 p-tid2 1}}]
              p-jid1 [{:q/type           :q.type/snapshot
                       :q/proc-id        p-jid1
                       :q/tailer-indices {p-fid1 1}}]
              p-jid2 [{:q/type           :q.type/snapshot
                       :q/proc-id        p-jid2
                       :q/tailer-indices {p-fid2 1}}]}))
      (q/stop-graph! g)
      (q/close-graph! g))

    ;; After restarting the graph both messages are delivered, and we
    ;; can see the subsequent attempt in the backing queue.
    (let [done-1 (promise)
          done-2 (promise)
          done-3 (promise)
          done-4 (promise)
          g      (->> {:id         ::graph
                       :processors [{:id ::s1}
                                    {:id ::s2}
                                    {:id  ::interrupt-fork
                                     :in  {:in-1 ::s1
                                           :in-2 ::s2}
                                     :out {:out-1 ::q1
                                           :out-2 ::q2}}
                                    {:id   ::done
                                     :in   {:in ::q1}
                                     :opts {:done done-1
                                            :x-n  1}}
                                    {:id   ::d2
                                     :fn   ::done-counter
                                     :in   {:in p-id}
                                     :opts {:done    done-2
                                            :counter (atom 0)
                                            :n       2}} ; 2 + 1 from intermediary graph
                                    {:id   ::d3
                                     :fn   ::done-counter
                                     :in   {:in p-jid1}
                                     :opts {:done    done-3
                                            :counter (atom 0)
                                            :n       2}} ; 2 + 1 from intermediary graph
                                    {:id   ::d4
                                     :fn   ::done-counter
                                     :in   {:in p-jid2}
                                     :opts {:done    done-4
                                            :counter (atom 0)
                                            :n       2}}]} ; 2 + 1 from intermediary graph
                      (q/graph)
                      (q/start-graph!))]
      (is (done? done-1))
      (is (done? done-2))
      (is (done? done-3))
      (is (done? done-4))
      (is (= (q/all-graph-messages g)
             {::s1   [{:x 1}]
              ::s2   [{:x 2}]
              ::q1   [{:x 1}]
              ::q2   [{:x 2}]
              p-id   [{:q/type           :q.type/snapshot
                       :q/proc-id        p-id
                       :q/tailer-indices {p-tid1 1 p-tid2 1}}
                      {:q/type          :q.type/attempt-output
                       :q/message-index 1}
                      {:q/type           :q.type/snapshot
                       :q/proc-id        p-id
                       :q/tailer-indices {p-tid1 2 p-tid2 2}}]
              p-jid1 [{:q/type           :q.type/snapshot
                       :q/proc-id        p-jid1
                       :q/tailer-indices {p-fid1 1}}
                      {:q/type          :q.type/attempt-output
                       :q/message-index 1}
                      {:q/type           :q.type/snapshot
                       :q/proc-id        p-jid1
                       :q/tailer-indices {p-fid1 2}}]
              p-jid2 [{:q/type           :q.type/snapshot
                       :q/proc-id        p-jid2
                       :q/tailer-indices {p-fid2 1}}
                      {:q/type          :q.type/attempt-output
                       :q/message-index 1}
                      {:q/type           :q.type/snapshot
                       :q/proc-id        p-jid2
                       :q/tailer-indices {p-fid2 2}}]}))
      (q/stop-graph! g)
      (q/close-and-delete-graph! g true))))

(defmethod q/processor ::exception
  ;; Throws only first message.
  [{{:keys [counter n]} :opts} {msg :in}]
  (err/wrap-error {:err/context "context"}
    (if (= (swap! counter inc) n)
      (throw (Exception. "Oops"))
      {:out msg})))

(t/deftest exactly-once-processor-handled-error-test
  ;; The processor throws a handled error only on the first
  ;; message. Handled errors are considered delivered according
  ;; exactly once semantics. The error queue is configured and the
  ;; error should appear there.
  (log/with-level :fatal
    (let [p-id   :cues.queue-test.graph.cues.queue-test/exception
          p-tid  :cues.queue-test.graph.cues.queue-test.exception.cues.queue-test/s1
          d-id   :cues.queue-test.graph.cues.queue-test/done
          d-tid  :cues.queue-test.graph.cues.queue-test.done.cues.queue-test/q1
          done-1 (promise)
          done-2 (promise)
          done-3 (promise)]
      (qt/with-graph-and-delete
        [g {:id         ::graph
            :queue-opts {::q/default {:queue-meta false}}
            :processors [{:id ::s1}
                         {:id   ::exception
                          :in   {:in ::s1}
                          :out  {:out ::q1}
                          :opts {:counter (atom 0)
                                 :n       1}}
                         {:id   ::done
                          :in   {:in ::q1}
                          :opts {:done done-1
                                 :x-n  2}}
                         {:id   ::d2
                          :fn   ::done-counter
                          :in   {:in p-id}
                          :opts {:done    done-2
                                 :counter (atom 0)
                                 :n       5}}
                         {:id   ::d3
                          :fn   ::done-counter
                          :in   {:in d-id}
                          :opts {:done    done-3
                                 :counter (atom 0)
                                 :n       3}}]}]
        (q/send! g ::s1 {:x 1})
        (q/send! g ::s1 {:x 2})
        (is (done? done-1))
        (is (done? done-2))
        (is (done? done-3))
        (is (= (-> (q/all-graph-messages g)
                   (update ::qt/error qt/simplify-exceptions))
               {::qt/error [{:q/type            :q.type.err/error
                             :err.proc/config   {:id       ::exception
                                                 :in       {:in ::s1}
                                                 :out      {:out ::q1}
                                                 :strategy ::q/exactly-once}
                             :err.proc/messages {::s1 {:x 1}}
                             :err/context       "context"
                             :err/cause         {:cause "Oops"}}]
                ::s1       [{:x 1} {:x 2}]
                ::q1       [{:x 2}]
                p-id       [{:q/type           :q.type/snapshot
                             :q/proc-id        p-id
                             :q/tailer-indices {p-tid 1}}
                            {:q/type          :q.type/attempt-error
                             :q/message-index 1}
                            {:q/type           :q.type/snapshot
                             :q/proc-id        p-id
                             :q/tailer-indices {p-tid 2}}
                            {:q/type          :q.type/attempt-output
                             :q/message-index 1}
                            {:q/type           :q.type/snapshot
                             :q/proc-id        p-id
                             :q/tailer-indices {p-tid 3}}]
                d-id       [{:q/type           :q.type/snapshot
                             :q/proc-id        d-id
                             :q/tailer-indices {d-tid 1}}
                            {:q/type :q.type/attempt-nil
                             :q/hash -1173148930}
                            {:q/type           :q.type/snapshot
                             :q/proc-id        d-id
                             :q/tailer-indices {d-tid 2}}] }))))))

(def ^:private write-impl
  "Bind original here for use in test fn."
  q/write)

(defn- write-error
  "Throws an error on a specific attempt number."
  [done counter n]
  (fn [appender msg]
    (write-impl appender msg)
    (when (= (swap! counter inc) n)
      (deliver done true))
    (when (= (:q/type msg) :q.type/attempt-output)
      (throw (Exception. "Failed after write")))))

(t/deftest exactly-once-write-handled-error-test
  ;; The processor throws a handled error after the attempt is
  ;; persisted, but before the output message is written. The error
  ;; queue is not configured, so message is considered "handled" after
  ;; logging output.
  (log/with-level :fatal
    (let [p-id  :cues.queue-test.graph.cues.queue-test/map-reduce
          p-tid :cues.queue-test.graph.cues.queue-test.map-reduce.cues.queue-test/s1]
      (let [done-1 (promise)
            done-2 (promise)
            logged (atom 0)]
        (with-redefs [q/write               (write-error done-1 (atom 0) 9)
                      q/log-processor-error (fn [_ _] (swap! logged inc))]
          (let [g (->> {:id         ::graph
                        :errors     ::errors
                        :processors [{:id ::s1}
                                     {:id   ::map-reduce
                                      :in   {:in ::s1}
                                      :out  {:out ::q1}
                                      :opts {:map-fn inc
                                             :to     [:out]}}
                                     {:id   ::done-counter
                                      :in   {:in p-id}
                                      :opts {:done    done-2
                                             :counter (atom 0)
                                             :n       7}}]}
                       (q/graph)
                       (q/start-graph!))]
            (q/send! g ::s1 {:x 1})
            (q/send! g ::s1 {:x 2})
            (is (done? done-1))
            (is (done? done-2))
            (is (= @logged 2))
            (is (= (-> (q/all-graph-messages g)
                       (update ::errors qt/simplify-exceptions))
                   {::errors [{:q/type            :q.type.err/processor
                               :err/cause         {:cause "Failed after write"}
                               :err.proc/config   {:id       ::map-reduce
                                                   :in       {:in ::s1}
                                                   :out      {:out ::q1}
                                                   :strategy ::q/exactly-once}
                               :err.proc/messages {:x 2}}
                              {:q/type            :q.type.err/processor
                               :err/cause         {:cause "Failed after write"}
                               :err.proc/config   {:id       ::map-reduce
                                                   :in       {:in ::s1}
                                                   :out      {:out ::q1}
                                                   :strategy ::q/exactly-once}
                               :err.proc/messages {:x 3}}]
                    ::s1     [{:x 1} {:x 2}]
                    ::q1     []
                    p-id     [{:q/type           :q.type/snapshot
                               :q/proc-id        p-id
                               :q/tailer-indices {p-tid 1}}
                              {:q/type          :q.type/attempt-output
                               :q/message-index 1}
                              {:q/type          :q.type/attempt-error
                               :q/message-index 1}
                              {:q/type           :q.type/snapshot
                               :q/proc-id        p-id
                               :q/tailer-indices {p-tid 2}}
                              {:q/type          :q.type/attempt-output
                               :q/message-index 1}
                              {:q/type          :q.type/attempt-error
                               :q/message-index 2}
                              {:q/type           :q.type/snapshot
                               :q/proc-id        p-id
                               :q/tailer-indices {p-tid 3}}]}))
            (q/stop-graph! g)
            (q/close-and-delete-graph! g true)))))))

(t/deftest exactly-once-attempt-no-write-unhandled-error-test
  ;; The processor throws an unhandled error after the attempt is
  ;; persisted, but before the output message is written. The error
  ;; queue is not configured so the error will not be handled. Without
  ;; exactly once semantics the emssages would be lost.
  (log/with-level :fatal
    (let [p-id  :cues.queue-test.graph.cues.queue-test/map-reduce
          p-tid :cues.queue-test.graph.cues.queue-test.map-reduce.cues.queue-test/s1
          d-id  :cues.queue-test.graph.cues.queue-test/done
          d-tid :cues.queue-test.graph.cues.queue-test.done.cues.queue-test/q1]
      (let [done-1 (promise)
            done-2 (promise)
            logged (atom 0)]
        (with-redefs [q/write               (write-error done-1 (atom 0) 9)
                      q/log-processor-error (fn [_ _] (swap! logged inc))]
          (let [g (->> {:id         ::graph
                        :processors [{:id ::s1}
                                     {:id   ::map-reduce
                                      :in   {:in ::s1}
                                      :out  {:out ::q1}
                                      :opts {:map-fn inc
                                             :to     [:out]}}
                                     {:id   ::done-counter
                                      :in   {:in p-id}
                                      :opts {:done    done-2
                                             :counter (atom 0)
                                             :n       2}}]}
                       (q/graph)
                       (q/start-graph!))]
            (q/send! g ::s1 {:x 1})
            (q/send! g ::s1 {:x 2})
            (is (done? done-1))
            (is (done? done-2))
            (is (= @logged 1))
            (is (= (q/all-graph-messages g)
                   {::s1 [{:x 1} {:x 2}]
                    ::q1 []
                    p-id [{:q/type           :q.type/snapshot
                           :q/proc-id        p-id
                           :q/tailer-indices {p-tid 1}}
                          {:q/type          :q.type/attempt-output
                           :q/message-index 1}]}))
            (q/stop-graph! g)
            (q/close-graph! g))))

      ;; After restarting the graph both messages are delivered, and we
      ;; can see the subsequent attempt in the backing queue.
      (let [done-2 (promise)
            done-3 (promise)
            logged (atom 0)]
        (with-redefs [q/log-processor-error (fn [_ _] (swap! logged inc))]
          (let [g (->> {:id         ::graph
                        :processors [{:id ::s1}
                                     {:id   ::map-reduce
                                      :in   {:in ::s1}
                                      :out  {:out ::q1}
                                      :opts {:map-fn inc
                                             :to     [:out]}}
                                     {:id ::done
                                      :in {:in ::q1}}
                                     {:id   ::d2
                                      :fn   ::done-counter
                                      :in   {:in p-id}
                                      :opts {:done    done-2
                                             :counter (atom 0)
                                             :n       2}} ; 2 + 2 from previous graph
                                     {:id   ::d3
                                      :fn   ::done-counter
                                      :in   {:in d-id}
                                      :opts {:done    done-3
                                             :counter (atom 0)
                                             :n       5}}]}
                       (q/graph)
                       (q/start-graph!))]
            (is (done? done-2))
            (is (done? done-3))
            (is (= @logged 0))
            (is (= (q/all-graph-messages g)
                   {::s1 [{:x 1} {:x 2}]
                    ::q1 [{:x 2} {:x 3}]
                    p-id [{:q/type           :q.type/snapshot
                           :q/proc-id        p-id
                           :q/tailer-indices {p-tid 1}}
                          {:q/type          :q.type/attempt-output
                           :q/message-index 1}
                          {:q/type          :q.type/attempt-output
                           :q/message-index 1}
                          {:q/type           :q.type/snapshot
                           :q/proc-id        p-id
                           :q/tailer-indices {p-tid 2}}
                          {:q/type          :q.type/attempt-output
                           :q/message-index 2}
                          {:q/type           :q.type/snapshot
                           :q/proc-id        p-id
                           :q/tailer-indices {p-tid 3}}]
                    d-id [{:q/type           :q.type/snapshot
                           :q/proc-id        d-id
                           :q/tailer-indices {d-tid 1}}
                          {:q/type :q.type/attempt-nil
                           :q/hash -1173148930}
                          {:q/type           :q.type/snapshot
                           :q/proc-id        d-id
                           :q/tailer-indices {d-tid 2}}
                          {:q/type :q.type/attempt-nil
                           :q/hash 1121832113}
                          {:q/type           :q.type/snapshot
                           :q/proc-id        d-id
                           :q/tailer-indices {d-tid 3}}]}))
            (q/stop-graph! g)
            (q/close-and-delete-graph! g true)))))))

(def ^:private attempt-full
  "Bind original here for use in test fn."
  @#'q/attempt-full)

(defn- attempt-and-write-then-interrupt
  "Throws an unhandled interrupt immediately after both the attempt and
  output write are completed."
  [done]
  (fn [appender msg]
    (attempt-full appender msg)
    (try
      (throw (throw-interrupt!))
      (finally (deliver done true)))))

(t/deftest exactly-once-attempt-and-write-then-interrupt-test
  ;; The processor throws an unhandled interrupt after both the
  ;; attempt and the output message are persisted. The processor then
  ;; quits due to the unhandled interrupt. Without exactly once
  ;; semantics, the message would be written again with at least once
  ;; semantics on restart. However, using exactly once semantics the
  ;; message is not persisted again after the graph is restarted.
  (log/with-level :fatal
    (let [p-id  :cues.queue-test.graph.cues.queue-test/map-reduce
          p-tid :cues.queue-test.graph.cues.queue-test.map-reduce.cues.queue-test/s1
          d-id  :cues.queue-test.graph.cues.queue-test/done
          d-tid :cues.queue-test.graph.cues.queue-test.done.cues.queue-test/q1]
      (let [done-1 (promise)
            done-2 (promise)]
        (with-redefs [q/attempt-full (attempt-and-write-then-interrupt done-1)]
          (let [g (->> {:id         ::graph
                        :processors [{:id ::s1}
                                     {:id   ::map-reduce
                                      :in   {:in ::s1}
                                      :out  {:out ::q1}
                                      :opts {:map-fn inc
                                             :to     [:out]}}
                                     {:id   ::d2
                                      :fn   ::done-counter
                                      :in   {:in p-id}
                                      :opts {:done    done-2
                                             :counter (atom 0)
                                             :n       2}}]}
                       (q/graph)
                       (q/start-graph!))]
            (q/send! g ::s1 {:x 1})
            (is (done? done-1))
            (is (= (q/all-graph-messages g)
                   {::s1 [{:x 1}]
                    ::q1 [{:x 2}]
                    p-id [{:q/type           :q.type/snapshot
                           :q/proc-id        p-id
                           :q/tailer-indices {p-tid 1}}
                          {:q/type          :q.type/attempt-output
                           :q/message-index 1}]}))
            (q/stop-graph! g)
            (q/close-graph! g))))

      ;; After restarting the graph the message is not re-delivered,
      ;; and we can see there are not subsequent attempts in the try
      ;; queue.
      (let [done-2 (promise)
            done-3 (promise)]
        (let [g (->> {:id         ::graph
                      :processors [{:id ::s1}
                                   {:id   ::map-reduce
                                    :in   {:in ::s1}
                                    :out  {:out ::q1}
                                    :opts {:map-fn inc
                                           :to     [:out]}}
                                   {:id ::done
                                    :in {:in ::q1}}
                                   {:id   ::d2
                                    :fn   ::done-counter
                                    :in   {:in p-id}
                                    :opts {:done    done-2
                                           :counter (atom 0)
                                           :n       1}} ; 1 + 2 from previous graph
                                   {:id   ::d3
                                    :fn   ::done-counter
                                    :in   {:in d-id}
                                    :opts {:done    done-3
                                           :counter (atom 0)
                                           :n       3}}]} ; 3 + 0 from previous graph
                     (q/graph)
                     (q/start-graph!))]
          (is (done? done-2))
          (is (done? done-3))
          (is (= (q/all-graph-messages g)
                 {::s1 [{:x 1}]
                  ::q1 [{:x 2}]
                  p-id [{:q/type           :q.type/snapshot
                         :q/proc-id        p-id
                         :q/tailer-indices {p-tid 1}}
                        {:q/type          :q.type/attempt-output
                         :q/message-index 1}
                        {:q/type           :q.type/snapshot
                         :q/proc-id        p-id
                         :q/tailer-indices {p-tid 2}}]
                  d-id [{:q/type           :q.type/snapshot
                         :q/proc-id        d-id
                         :q/tailer-indices {d-tid 1}}
                        {:q/type :q.type/attempt-nil
                         :q/hash -1173148930}
                        {:q/type           :q.type/snapshot
                         :q/proc-id        d-id
                         :q/tailer-indices {d-tid 2}}]}))
          (q/stop-graph! g)
          (q/close-and-delete-graph! g true))))))

(t/deftest issue-1-test
  (let [done-1 (promise)
        done-2 (promise)
        q      (q/queue ::tmp {:queue-path ".cues-tmp"})
        t1     (q/tailer q)
        t2     (q/tailer q)
        a      (q/appender q)]
    (try
      (future (deliver done-1 (q/read!! t1)))
      (future (deliver done-2 (q/read!! t2)))
      (Thread/sleep 1)
      (q/write a {:x 1})
      (is (= (done? done-1) {:x 1}))
      (is (= (done? done-2) {:x 1}))
      (finally
        (q/delete-queue! q true)
        (cutil/delete-file (io/file ".cues-tmp"))))))

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
                          {:id   ::d1
                           :fn   ::done-counter
                           :in   {:in ::q1}
                           :opts {:done    q1-done
                                  :counter (atom 0)
                                  :n       n}}
                          {:id   ::d2
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
  "Elapsed time: 4312.960375 msecs"
  "Elapsed time: 29921.640709 msecs"
  {:success? true
   :counts   {:cues.test/error    0
              :cues.queue-test/s1 1000000
              :cues.queue-test/s2 1000000
              :cues.queue-test/q1 1000000
              :cues.queue-test/q2 1000000}})
