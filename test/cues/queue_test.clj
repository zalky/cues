(ns cues.queue-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [is]]
            [cues.queue :as q]
            [cues.util :as util]
            [taoensso.timbre :as log]

            [cues.log]))                ; Ensure logging is configured for tests.

(defn with-warn
  [f]
  (log/with-level :warn
    (f)))

(defn last-read-index-from-1
  "For testing purposes only."
  [tailer]
  (q/with-tailer [t-1 (:queue tailer)]
    (let [i   (q/last-read-index* tailer)
          _   (q/to-start t-1)
          i-1 (q/index t-1)]
      (inc (- i i-1)))))

(defn to-index-from-1
  "For testing purposes only."
  [tailer i]
  (q/with-tailer [t-1 (:queue tailer)]
    (let [_   (q/to-start t-1)
          i-1 (q/index t-1)]
      (->> (+ i (dec i-1))
           (q/to-index* tailer)))))

(defn written-index-from-1
  "For testing purposes only."
  [queue i]
  (q/with-tailer [t-1 queue]
    (let [_   (q/to-start t-1)
          i-1 (q/index t-1)]
      (inc (- i i-1)))))

(defn with-deterministic-meta
  "The order that messages are placed on outbound fork queues is
  non-deterministic, therefore we can't test timestamps here."
  [f]
  (binding [q/last-read-index last-read-index-from-1
            q/to-index        to-index-from-1
            q/timestamp       (fn [_ msg] msg)
            q/written-index   written-index-from-1]
    (f)))

(t/use-fixtures :each
  (t/join-fixtures
   [with-deterministic-meta with-warn]))

(defn delete-graph-queues!
  "Careful with this function!"
  [g]
  (doseq [q (vals (:queues g))]
    (q/delete-queue! q true)))

(defmacro with-graph-and-delete
  [[sym :as binding] & body]
  `(let ~binding
     (let [~sym (-> ~sym
                    (util/assoc-nil :error-queue ::error)
                    (q/graph)
                    (q/start-graph!))]
       (try
         ~@body
         (finally
           (-> ~sym
               (q/stop-graph!)
               (q/close-graph!)
               (delete-graph-queues!)))))))

(defn p-fn
  [{r-fn   :reduce-fn
    m-fn   :map-fn
    queues :to
    :or    {m-fn identity}}]
  {:pre [(fn? r-fn) (fn? m-fn)]}
  (fn [_ msgs]
    (let [result {:x (->> msgs
                          vals
                          (map (comp m-fn :x))
                          (reduce r-fn))}]
      (zipmap queues (repeat result)))))

(defn done?
  ([done]
   (done? done 10000))
  ([done ms]
   (deref done ms false)))

(defn x=
  [done queue-id x]
  (fn [_ msgs]
    (when (= (get-in msgs [queue-id :x]) x)
      (deliver done true))))

(t/deftest graph-parse-processor
  (is (= (s/conform ::q/processor-impl {:id  ::processor
                                        :fn  identity
                                        :out ::out})
         (s/conform ::q/processor-impl {:id  ::processor
                                        :fn  identity
                                        :out [::out]})
         [::q/source
          {:id  ::processor
           :fn  identity
           :out ::out}]))
  (is (= (s/conform ::q/processor-impl {:id ::processor
                                        :fn identity
                                        :in ::in})
         (s/conform ::q/processor-impl {:id ::processor
                                        :fn identity
                                        :in [::in]})
         [::q/sink
          {:id ::processor
           :fn identity
           :in ::in}]))
  (is (= (s/conform ::q/processor-impl {:id  ::processor
                                        :fn  identity
                                        :in  ::in
                                        :out ::out})
         (s/conform ::q/processor-impl {:id  ::processor
                                        :fn  identity
                                        :in  [::in]
                                        :out [::out]})
         [::q/join
          {:id  ::processor
           :fn  identity
           :in  ::in
           :out ::out}]))
  (is (= (s/conform ::q/processor-impl {:id  ::processor
                                        :fn  identity
                                        :in  ::in
                                        :out [::out-1 ::out-2]})
         [::q/join-fork
          {:id  ::processor
           :fn  identity
           :in  ::in
           :out [::out-1 ::out-2]}])))

(t/deftest graph-test-system
  (let [done (promise)]
    (with-graph-and-delete
      [g {:processors [{:id  ::source
                        :out ::q1}
                       {:id ::sink-system
                        :fn (fn [{:keys [system]} {{x :x} ::q1}]
                              (when (= x 1)
                                (deliver done system)))
                        :in ::q1}]
          :system     {:component :running}}]
      (q/send! g ::source {:x 1})
      (is (done? done))
      (is (= @done {:component :running})))))

(t/deftest graph-test-source-sink
  (let [done (promise)]
    (with-graph-and-delete
      [g {:processors [{:id  ::source
                        :out ::q1}
                       {:id ::sink-source
                        :fn (x= done ::q1 3)
                        :in ::q1}]}]

      (q/send! g ::source {:x 1})
      (q/send! g ::source {:x 2})
      (q/send! g ::source {:x 3})
      (is (done? done))
      (is (= (q/all-graph-messages g)
             {::error []
              ::q1    [{:x 1 :q/meta {:q/queue {::q1 {:q/t 1}}}}
                       {:x 2 :q/meta {:q/queue {::q1 {:q/t 2}}}}
                       {:x 3 :q/meta {:q/queue {::q1 {:q/t 3}}}}]})))))

(t/deftest graph-test-pipe
  (let [done (promise)]
    (with-graph-and-delete
      [g {:processors [{:id  ::source
                        :out ::q1}
                       {:id  ::pipe
                        :fn  (p-fn {:reduce-fn +
                                    :map-fn    inc
                                    :to        [::q2]})
                        :in  ::q1
                        :out ::q2}
                       {:id ::sink-pipe
                        :fn (x= done ::q2 4)
                        :in ::q2}]
          :tx-queue   ::q1}]
      (q/send! g ::source {:x 1})
      (q/send! g ::source {:x 2})
      (q/send! g ::source {:x 3})
      (is (done? done))
      (is (= (q/all-graph-messages g)
             {::error []
              ::q1    [{:x 1 :q/meta {:tx/t 1 :q/queue {::q1 {:q/t 1}}}}
                       {:x 2 :q/meta {:tx/t 2 :q/queue {::q1 {:q/t 2}}}}
                       {:x 3 :q/meta {:tx/t 3 :q/queue {::q1 {:q/t 3}}}}]
              ::q2    [{:x 2 :q/meta {:tx/t 1 :q/queue {::q1 {:q/t 1}
                                                        ::q2 {:q/t 1}}}}
                       {:x 3 :q/meta {:tx/t 2 :q/queue {::q1 {:q/t 2}
                                                        ::q2 {:q/t 2}}}}
                       {:x 4 :q/meta {:tx/t 3 :q/queue {::q1 {:q/t 3}
                                                        ::q2 {:q/t 3}}}}]})))))

(t/deftest graph-test-alts
  (let [d1   (promise)
        d2   (promise)
        d3   (promise)
        done (promise)]
    (with-graph-and-delete
      [g {:processors [{:id  ::s1
                        :out ::q1}
                       {:id  ::s2
                        :out ::q2}
                       {:id   ::alts
                        :fn   (p-fn {:reduce-fn +
                                     :map-fn    (fn [x]
                                                  (case x
                                                    1 (deliver d1 true)
                                                    2 (deliver d2 true)
                                                    3 (deliver d3 true)
                                                    true)
                                                  (inc x))
                                     :to        [::q3]})
                        :in   [::q1 ::q2]
                        :out  ::q3
                        :opts {:alts true}}
                       {:id ::sink-alts
                        :fn (x= done ::q3 4)
                        :in ::q3}]}]
      (q/send! g ::s1 {:x 1})
      (is (done? d1))
      (q/send! g ::s2 {:x 2})
      (is (done? d2))
      (q/send! g ::s2 {:x 3})
      (is (done? d3))
      (q/send! g ::s1 {:x 4})
      (is (done? done))
      (is (= (q/all-graph-messages g)
             {::error []
              ::q1    [{:x 1 :q/meta {:q/queue {::q1 {:q/t 1}}}}
                       {:x 4 :q/meta {:q/queue {::q1 {:q/t 2}}}}]
              ::q2    [{:x 2 :q/meta {:q/queue {::q2 {:q/t 1}}}}
                       {:x 3 :q/meta {:q/queue {::q2 {:q/t 2}}}}]
              ::q3    [{:x 2 :q/meta {:q/queue {::q1 {:q/t 1}
                                                ::q3 {:q/t 1}}}}
                       {:x 3 :q/meta {:q/queue {::q2 {:q/t 1}
                                                ::q3 {:q/t 2}}}}
                       {:x 4 :q/meta {:q/queue {::q2 {:q/t 2}
                                                ::q3 {:q/t 3}}}}
                       {:x 5 :q/meta {:q/queue {::q1 {:q/t 2}
                                                ::q3 {:q/t 4}}}}]})))))

(t/deftest graph-test-imperative
  (let [done (promise)]
    (with-graph-and-delete
      [g {:processors [{:id  ::source
                        :out ::q1}
                       {:id        ::pipe
                        :fn        (fn [{{a ::q2} :appenders
                                         :as      process} {in-msg ::q1}]
                                     (->> {:x (inc (:x in-msg))}
                                          (q/preserve-meta in-msg)
                                          (q/write a)))
                        :in        ::q1
                        :appenders ::q2}
                       {:id ::sink-pipe
                        :fn (x= done ::q2 4)
                        :in ::q2}]
          :tx-queue   ::q1}]
      (q/send! g ::source {:x 1})
      (q/send! g ::source {:x 2})
      (q/send! g ::source {:x 3})
      (is (done? done))
      (is (= (q/all-graph-messages g)
             {::error []
              ::q1    [{:x 1 :q/meta {:tx/t 1 :q/queue {::q1 {:q/t 1}}}}
                       {:x 2 :q/meta {:tx/t 2 :q/queue {::q1 {:q/t 2}}}}
                       {:x 3 :q/meta {:tx/t 3 :q/queue {::q1 {:q/t 3}}}}]
              ::q2    [{:x 2 :q/meta {:tx/t 1 :q/queue {::q1 {:q/t 1}
                                                        ::q2 {:q/t 1}}}}
                       {:x 3 :q/meta {:tx/t 2 :q/queue {::q1 {:q/t 2}
                                                        ::q2 {:q/t 2}}}}
                       {:x 4 :q/meta {:tx/t 3 :q/queue {::q1 {:q/t 3}
                                                        ::q2 {:q/t 3}}}}]})))))

(t/deftest graph-test-join
  (let [done (promise)]
    (with-graph-and-delete
      [g {:processors [{:id  ::s1
                        :out ::q1}
                       {:id  ::s2
                        :out ::q2}
                       {:id  ::join
                        :fn  (p-fn {:reduce-fn +
                                    :to        [::q3]})
                        :in  [::q1 ::q2]
                        :out ::q3}
                       {:id ::sink-join
                        :fn (x= done ::q3 7)
                        :in ::q3}]
          :tx-queue   ::q3}]
      (q/send! g ::s1 {:x 1})
      (q/send! g ::s1 {:x 3})
      (q/send! g ::s2 {:x 2})
      (q/send! g ::s2 {:x 4})
      (is (done? done))
      (is (= (q/all-graph-messages g)
             {::error []
              ::q1    [{:x 1 :q/meta {:q/queue {::q1 {:q/t 1}}}}
                       {:x 3 :q/meta {:q/queue {::q1 {:q/t 2}}}}]
              ::q2    [{:x 2 :q/meta {:q/queue {::q2 {:q/t 1}}}}
                       {:x 4 :q/meta {:q/queue {::q2 {:q/t 2}}}}]
              ::q3    [{:x 3 :q/meta {:tx/t    1
                                      :q/queue {::q1 {:q/t 1}
                                                ::q2 {:q/t 1}
                                                ::q3 {:q/t 1}}}}
                       {:x 7 :q/meta {:tx/t    2
                                      :q/queue {::q1 {:q/t 2}
                                                ::q2 {:q/t 2}
                                                ::q3 {:q/t 2}}}}]})))))

(t/deftest graph-test-join-fork
  (let [q3-done (promise)
        q4-done (promise)]
    (with-graph-and-delete
      [g {:processors [{:id  ::s1
                        :out ::q1}
                       {:id  ::s2
                        :out ::q2}
                       {:id  ::join-fork
                        :fn  (p-fn {:reduce-fn +
                                    :to        [::q3 ::q4]})
                        :in  [::q1 ::q2]
                        :out [::q3 ::q4]}
                       {:id ::k1
                        :fn (x= q3-done ::q3 7)
                        :in ::q3}
                       {:id ::k2
                        :fn (x= q4-done ::q4 7)
                        :in ::q4}]
          :tx-queue   ::q1}]
      (q/send! g ::s1 {:x 1})
      (q/send! g ::s1 {:x 3})
      (q/send! g ::s2 {:x 2})
      (q/send! g ::s2 {:x 4})
      (is (and (done? q3-done)
               (done? q4-done)))
      (is (= (q/all-graph-messages g)
             {::error []
              ::q1    [{:x 1 :q/meta {:tx/t 1 :q/queue {::q1 {:q/t 1}}}}
                       {:x 3 :q/meta {:tx/t 2 :q/queue {::q1 {:q/t 2}}}}]
              ::q2    [{:x 2 :q/meta {:q/queue {::q2 {:q/t 1}}}}
                       {:x 4 :q/meta {:q/queue {::q2 {:q/t 2}}}}]
              ::q3    [{:x 3 :q/meta {:tx/t    1
                                      :q/queue {::q1 {:q/t 1}
                                                ::q2 {:q/t 1}
                                                ::q3 {:q/t 1}}}}
                       {:x 7 :q/meta {:tx/t    2
                                      :q/queue {::q1 {:q/t 2}
                                                ::q2 {:q/t 2}
                                                ::q3 {:q/t 2}}}}]
              ::q4    [{:x 3 :q/meta {:tx/t    1
                                      :q/queue {::q1 {:q/t 1}
                                                ::q2 {:q/t 1}
                                                ::q4 {:q/t 1}}}}
                       {:x 7 :q/meta {:tx/t    2
                                      :q/queue {::q1 {:q/t 2}
                                                ::q2 {:q/t 2}
                                                ::q4 {:q/t 2}}}}]})))))

(t/deftest graph-test-join-fork-conditional
  (let [q3-done (promise)
        q4-done (promise)]
    (with-graph-and-delete
      [g {:processors [{:id  ::s1
                        :out ::q1}
                       {:id  ::s2
                        :out ::q2}
                       {:id  ::join-fork-conditional
                        :fn  (fn [_ msgs]
                               (let [n   (transduce (map :x) + (vals msgs))
                                     msg {:x n}]
                                 {::q3 (when (even? n) msg)
                                  ::q4 (when (odd? n) msg)}))
                        :in  [::q1 ::q2]
                        :out [::q3 ::q4]}
                       {:id ::k1
                        :fn (x= q3-done ::q3 2)
                        :in ::q3}
                       {:id ::k2
                        :fn (x= q4-done ::q4 5)
                        :in ::q4}]
          :tx-queue   ::q1}]
      (q/send! g ::s1 {:x 1})
      (q/send! g ::s1 {:x 2})
      (q/send! g ::s2 {:x 1})
      (q/send! g ::s2 {:x 3})
      (is (and (done? q3-done)
               (done? q4-done)))
      (is (= (q/all-graph-messages g)
             {::error []
              ::q1    [{:x 1 :q/meta {:tx/t 1 :q/queue {::q1 {:q/t 1}}}}
                       {:x 2 :q/meta {:tx/t 2 :q/queue {::q1 {:q/t 2}}}}]
              ::q2    [{:x 1 :q/meta {:q/queue {::q2 {:q/t 1}}}}
                       {:x 3 :q/meta {:q/queue {::q2 {:q/t 2}}}}]
              ::q3    [{:x 2 :q/meta {:tx/t    1
                                      :q/queue {::q1 {:q/t 1}
                                                ::q2 {:q/t 1}
                                                ::q3 {:q/t 1}}}}]
              ::q4    [{:x 5 :q/meta {:tx/t    2
                                      :q/queue {::q1 {:q/t 2}
                                                ::q2 {:q/t 2}
                                                ::q4 {:q/t 1}}}}]})))))

(defn simplify-exceptions
  [messages]
  (map #(update %
                :err/cause
                select-keys
                [:cause])
       messages))

(t/deftest graph-test-error
  ;; Suppress exception logging, just testing messaging
  (log/with-level :fatal
    (let [done (promise)]
      (with-graph-and-delete
        [g {:processors [{:id  ::source
                          :out ::q1}
                         {:id  ::pipe-error
                          :fn  (fn [_ {msg ::q1}]
                                 (if (even? (:x msg))
                                   {::q2 msg}
                                   (throw (Exception. "Oops"))))
                          :in  ::q1
                          :out ::q2}
                         {:id ::sink
                          :fn (x= done ::q2 4)
                          :in ::q2}]}]
        (q/send! g ::source {:x 1})
        (q/send! g ::source {:x 2})
        (q/send! g ::source {:x 3})
        (q/send! g ::source {:x 4})
        (is (done? done))
        (is (= (-> (q/all-graph-messages g)
                   (update ::error simplify-exceptions))
               {::error [{:kr/type           :kr.type.err/processor
                          :err/cause         {:cause "Oops"}
                          :err.proc/config   {:id  ::pipe-error
                                              :in  ::q1
                                              :out ::q2}
                          :err.proc/messages {::q1 {:x      1
                                                    :q/meta {:q/queue {::q1 {:q/t 1}}}}}
                          :q/meta            {:q/queue {::error {:q/t 1}}}}
                         {:kr/type           :kr.type.err/processor
                          :err/cause         {:cause "Oops"}
                          :err.proc/config   {:id  ::pipe-error
                                              :in  ::q1
                                              :out ::q2}
                          :err.proc/messages {::q1 {:x      3
                                                    :q/meta {:q/queue {::q1 {:q/t 3}}}}}
                          :q/meta            {:q/queue {::error {:q/t 2}}}}]
                ::q1    [{:x 1 :q/meta {:q/queue {::q1 {:q/t 1}}}}
                         {:x 2 :q/meta {:q/queue {::q1 {:q/t 2}}}}
                         {:x 3 :q/meta {:q/queue {::q1 {:q/t 3}}}}
                         {:x 4 :q/meta {:q/queue {::q1 {:q/t 4}}}}]
                ::q2    [{:x 2 :q/meta {:q/queue {::q1 {:q/t 2}
                                                  ::q2 {:q/t 1}}}}
                         {:x 4 :q/meta {:q/queue {::q1 {:q/t 4}
                                                  ::q2 {:q/t 2}}}}]}))))))

(defn p-counter
  [p n]
  (let [c (atom 0)]
    (fn [_ _]
      (when (= n (swap! c inc))
        (deliver p true)))))

(def stress-fixtures
  (t/join-fixtures [with-warn]))

(defn stress-test
  [n]
  (stress-fixtures
   (fn []
     (let [q3-done (promise)
           q4-done (promise)
           timeout (max (/ n 10) 1000)]
       (with-graph-and-delete
         [g {:processors [{:id  ::s1
                           :out ::q1}
                          {:id  ::s2
                           :out ::q2}
                          {:id  ::join-fork
                           :fn  (p-fn {:reduce-fn +
                                       :to        [::q3 ::q4]})
                           :in  [::q1 ::q2]
                           :out [::q3 ::q4]}
                          {:id ::k1
                           :fn (p-counter q3-done n)
                           :in ::q3}
                          {:id ::k2
                           :fn (p-counter q4-done n)
                           :in ::q4}]}]
         {:success? (time
                      (do
                        (time
                          (dotimes [n n]
                            (q/send! g ::s1 {:x n})
                            (q/send! g ::s2 {:x n})))
                        (and (done? q3-done timeout)
                             (done? q4-done timeout))))
          :counts   (->> g
                         (:queues)
                         (util/map-vals q/count-messages))})))))

(comment
  ;; This measure a single round-trip read + write, with blocking. The
  ;; messages being serialized on these queues are generally between
  ;; 114-117 Bytes in size.
  (b/quick-bench
   (do (q/write a {:x 1})
       (q/read!! t)))

  "Evaluation count : 152556 in 6 samples of 25426 calls.
               Execution time mean : 3.938096 µs
      Execution time std-deviation : 11.625992 ns
     Execution time lower quantile : 3.924338 µs ( 2.5%)
     Execution time upper quantile : 3.953700 µs (97.5%)
                     Overhead used : 2.029612 ns

  Found 1 outliers in 6 samples (16.6667 %)
  	low-severe	 1 (16.6667 %)
   Variance from outliers : 13.8889 % Variance is moderately inflated by outliers"

  (qt/stress-test 1000000)
  "Elapsed time: 7153.911291 msecs"
  "Elapsed time: 23936.871375 msecs"
  {:success? true
   :counts   {::error 0
              ::q1    1000000
              ::q2    1000000
              ::q3    1000000
              ::q4    1000000}})
