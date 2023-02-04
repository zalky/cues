(ns cues.queue
  (:refer-clojure :exclude [read peek])
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [cues.controller-cache :as controllers]
            [cues.deps :as deps]
            [cues.error :as err]
            [cues.util :as util]
            [qbits.tape.appender :as app]
            [qbits.tape.codec :as codec]
            [qbits.tape.queue :as queue]
            [qbits.tape.tailer :as tail]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as log])
  (:import clojure.lang.IRef
           java.io.File
           java.nio.ByteBuffer
           java.time.Instant
           java.util.stream.Collectors
           [net.openhft.chronicle.queue.impl.single SingleChronicleQueue StoreTailer]
           net.openhft.chronicle.queue.util.FileUtil))

(def queue-path-default
  "data/queues/")

(defn- absolute-path?
  [x]
  (and (string? x)
       (str/starts-with? x "/")))

(defn queue?
  [x]
  (isa? (:type x) ::queue))

(defn tailer?
  [x]
  (isa? (:type x) ::tailer))

(defn appender?
  [x]
  (isa? (:type x) ::appender))

(defn- queue-any?
  [x]
  (if (util/collection? x)
    (every? queue? x)
    (queue? x)))

(defn id->str
  [id]
  (cond
    (qualified-keyword? id) (str (namespace id) "/" (name id))
    (keyword? id)           (name id)
    (uuid? id)              (str id)
    (string? id)            id))

(defn queue-path
  [{id              :id
    {p :queue-path} :opts}]
  {:pre [(not (absolute-path? p))
         (not (absolute-path? id))
         (not (absolute-path? queue-path-default))]}
  (-> (or p queue-path-default)
      (io/file (id->str id))
      (str)))

(defn- codec
  []
  (reify
    codec/ICodec
    (write [_ x]
      (->> x
           (nippy/freeze)
           (ByteBuffer/wrap)))
    (read [_ x]
      (->> x
           (.array)
           (nippy/thaw)))))

(defn- suffix-id
  "Disambiguates beteween similar ids within a topology."
  [suffix id]
  (keyword (namespace id)
           (str (name id) suffix)))

(defn- tailer-id
  [queue-id id]
  (when (and queue-id id)
    (->> (str "tailer" queue-id id)
         (keyword (namespace ::_)))))

(declare last-index)

(def transient-queue-opts
  "Transient queues are purged after 10 days."
  {:roll-cycle          :small-daily
   :cycle-release-tasks [{:type         :delete
                          :after-cycles 10}]})

(defn queue-opts
  [{:keys [transient] :as opts}]
  (cond-> opts
    true      (assoc :codec (codec))
    transient (merge transient-queue-opts)))

(defn queue
  ([id]
   (queue id nil))
  ([id opts*]
   (let [opts (queue-opts opts*)
         path (queue-path {:id id :opts opts})
         q    (queue/make path opts)
         i    (last-index q)]
     {:type       ::queue
      :id         id
      :opts       opts
      :controller (controllers/lookup id #(atom i))
      :queue-impl q})))

(declare prime-tailer)

(defn tailer
  "Providing an id enables persistent restarts on the given queue."
  ([queue] (tailer queue nil nil))
  ([queue id stop]
   {:pre [(queue? queue)]}
   (let [qid  (:id queue)
         tid  (tailer-id qid id)
         opts {:id (some-> tid id->str)}]
     (prime-tailer
      {:type        ::tailer
       :id          tid
       :stop        stop
       :queue       queue
       :tailer-impl (-> queue
                        (:queue-impl)
                        (tail/make opts))}))))

(defn appender
  [{q :queue-impl :as queue}]
  {:pre [(queue? queue)]}
  {:type          ::appender
   :queue         queue
   :appender-impl (app/make q)})

(def closed?       (comp queue/closed? :queue-impl))
(def set-direction (fn [t dir] (-> t :tailer-impl (tail/set-direction! dir)) t))
(def to-end        (fn [t] (-> t :tailer-impl tail/to-end!) t))
(def to-start      (fn [t] (-> t :tailer-impl tail/to-start!) t))
(def index         (comp tail/index :tailer-impl))
(def queue-obj     (comp queue/underlying-queue :queue-impl))
(def close-tailer! (fn [t] (-> t :tailer-impl tail/tailer .close) t))

(defn close!
  [{id  :id
    q   :queue-impl
    :as queue}]
  {:pre [(queue? queue)]}
  (controllers/purge-controller id)
  (queue/close! q)
  (System/runFinalization))

(defn to-index*
  [t i]
  (if (zero? i)
    (tail/to-start! (:tailer-impl t))
    (tail/to-index! (:tailer-impl t) i)))

(defn- prime-tailer
  "In certain rare cases, a newly initialized StoreTailer will start
  reading from the second index, but reports as if it had started at
  the first. Forcing the tailer explicitly to the index it reports to
  be at mitigates the issue. This is always done on tailer
  initialization."
  [tailer]
  {:pre [(tailer? tailer)]}
  (->> (index tailer)
       (to-index* tailer))
  tailer)

(def ^:dynamic to-index
  "Only rebind for testing!"
  to-index*)

(defn last-index
  "A last index implementation that works for any kind of queue or
  appender."
  [x]
  (letfn [(last-index* [q]
            (.lastIndex
             ^SingleChronicleQueue
             (queue/underlying-queue q)))]
    (cond
      (satisfies? queue/IQueue x)  (last-index* x)
      (satisfies? app/IAppender x) (app/last-index x)
      (appender? x)                (app/last-index (:appender-impl x))
      (queue? x)                   (last-index* (:queue-impl x)))))

(defn last-read-index*
  [tailer]
  {:pre [(tailer? tailer)]}
  (let [^StoreTailer t (->> tailer
                            :tailer-impl
                            tail/tailer)]
    (.lastReadIndex t)))

(def ^:dynamic last-read-index
  "Only rebind for testing!"
  last-read-index*)

(defn to-last-read
  [tailer]
  (->> tailer
       (last-read-index)
       (to-index tailer))
  nil)

(defn read
  "Reads message from tailer, inserting index into message."
  [{{id             :id
     {tx :tx-queue} :opts} :queue
    t-impl                 :tailer-impl
    :as                    tailer}]
  {:pre [(tailer? tailer)]}
  (when-let [msg (tail/read! t-impl)]
    (let [t (last-read-index tailer)]
      (cond-> msg
        t  (assoc-in [:q/meta :q/queue id :q/t] t)
        tx (assoc-in [:q/meta :tx/t] t)))))

(defn peek
  "Like read, but does not advance tailer."
  [tailer]
  {:pre [(tailer? tailer)]}
  (when-let [msg (read tailer)]
    (->> tailer
         (last-read-index)
         (to-index tailer))
    msg))

(defn mono-inc
  "Monotonically increases counter."
  [old new]
  (if (and (some? new)
           (or (nil? old)
               (< old new)))
    new
    old))

(defn- controller-inc!
  "Monotonically increases queue controller. It is okay to drop indexes,
  as long as each update is greater than or equal to the previous."
  [queue new-index]
  (update queue :controller swap! mono-inc new-index))

(def ^:dynamic timestamp
  "Only rebind for testing!"
  (fn [id msg]
    (assoc-in msg
              [:q/meta :q/queue id :q/time]
              (Instant/now))))

(def ^:dynamic written-index
  "Only rebind for testing!"
  (fn [_ i] i))

(defn write
  "The queue controller approximately follows the index of the queue: it
  can fall behind, but must be eventually consistent."
  [{{id  :id
     :as q} :queue
    a       :appender-impl
    :as     appender} msg]
  {:pre [(appender? appender)
         (map? msg)]}
  (let [index (->> msg
                   (timestamp id)
                   (app/write! a))]
    (controller-inc! q index)
    (written-index q index)))

(defn- watch-controller!
  [{id              :id
    {c :controller} :queue
    :as             tailer} unblock]
  (when c
    (let [tailer-i (index tailer)]
      (add-watch c id
        (fn [id _ _ i]
          (when (<= tailer-i i)
            (deliver unblock tailer)))))))

(defn- watch-stop!
  [{id   :id
    stop :stop} unblock]
  (when stop
    (add-watch stop id
      (fn [id _ _ new]
        (when (true? new)
          (deliver unblock nil))))
    (swap! stop identity)))

(defn- rm-watches
  [{id              :id
    {c :controller} :queue
    stop            :stop}]
  (when c
    (remove-watch c id))
  (when stop
    (remove-watch stop id)))

(defn running?
  [{stop :stop
    :as  obj}]
  (and (some? obj)
       (or (not (instance? IRef stop))
           (not @stop))))

(defn- try-read
  [tailer]
  (when (running? tailer)
    (loop [n 10000]
      (if (pos? n)
        (or (read tailer)
            (recur (dec n)))
        (throw (ex-info "Max read tries" {:tailer tailer}))))))

(defn read!!
  "Blocking read. The tailer will consume eagerly if it can. If not, it
  watches the controller until a new message is available. It is
  critical that the watch be placed before the first read. Note that
  the tailer index will always be one ahead of the last index it
  read. Read will unblock and return nil if at any point a stop signal
  is received. This allows blocked threads to be cleaned up on stop
  events."
  [tailer]
  {:pre [(tailer? tailer)]}
  (let [unblock (promise)]
    (watch-controller! tailer unblock)
    (watch-stop! tailer unblock)
    (if-let [msg (read tailer)]
      (do (rm-watches tailer) msg)
      (let [t @unblock]
        (rm-watches t)
        (try-read t)))))

(defn- alts
  [tailers]
  (reduce
   (fn [_ t]
     (when-let [msg (read t)]
       (rm-watches t)
       (reduced [t msg])))
   nil
   tailers))

(defn alts!!
  "Completes at most one blocking read from several tailers. Semantics
  of each read are that or read!!."
  [tailers]
  {:pre [(every? tailer? tailers)]}
  (let [unblock (promise)]
    (doseq [t tailers]
      (watch-controller! t unblock)
      (watch-stop! t unblock))
    (or (alts tailers)
        (let [t @unblock]
          (doseq [t* tailers]
            (rm-watches t*))
          (some->> (try-read t)
                   (vector t))))))

(defn snapshot
  "Record tailer indices for error recovery."
  [{tailers :tailers
    :as     process}]
  (->> tailers
       (map (juxt identity index))
       (doall)
       (assoc process :snapshot)))

(defn recover
  "Reset tailer positions."
  ([process]
   (recover process (constantly true)))
  ([{snapshot :snapshot} pred?]
   (doseq [[tailer i] snapshot]
     (when (pred? tailer)
       (to-index tailer i)))))

(defn- log-processor-error
  [{{:keys [id in out]
     :or   {in  "source"
            out "sink"}} :config} e]
  (log/error e (format "%s (%s -> %s)" id in out)))

(defn- merge-meta
  [in out]
  (when out
    (let [m (->> (vals in)
                 (map :q/meta)
                 (apply util/merge-deep))]
      (util/map-vals
       (fn [v]
         (when v
           (assoc v :q/meta m)))
       out))))

(defn preserve-meta
  [in out]
  (assoc out :q/meta (:q/meta in)))

(defn- get-result
  [process result]
  (get result (-> process
                  :appender
                  :queue
                  :id)))

(defn zip-read!!
  "Blocking read from all tailers into a map. Returns nil if any one of
  the blocking tailers returns nil."
  [{tailers :tailers}]
  (let [vals (map read!! tailers)
        keys (map (comp :id :queue) tailers)]
    (when (every? some? vals)
      (zipmap keys vals))))

(defn alts-read!!
  [{tailers :tailers}]
  (when-let [[t msg] (alts!! tailers)]
    {(:id (:queue t)) msg}))

(defn- processor-read
  [{{alts :alts} :opts
    read-fn!!    :read-fn!!
    :as          process}]
  (let [f (cond
            read-fn!! read-fn!!
            alts      alts-read!!
            :else     zip-read!!)]
    (f process)))

(defn- select-processor
  "Keys passed on to the processor fn"
  [{imperative :imperative
    :as        process}]
  (merge process imperative))

(defn- run-fn!!
  [{f         :fn
    appender  :appender
    result-fn :result-fn
    :or       {result-fn get-result}
    :as       process}]
  (if-let [in (processor-read process)]
    (let [out (-> process
                  (select-processor)
                  (f in))]
      (when appender
        (->> (merge-meta in out)
             (result-fn process))))
    (recover process)))

(defn- processor-write
  [{:keys [appender]} msg]
  (when (and appender msg)
    (write appender msg)))

(defn- get-error-fn
  [{:keys [error-queue]}]
  (if error-queue
    (let [a (appender error-queue)]
      (fn [process e]
        (log-processor-error process e)
        (->> (err/error e)
             (ex-data)
             (write a))))
    log-processor-error))

(defn- processor-error
  [{config :config} msgs]
  {:kr/type           :kr.type.err/processor
   :err.proc/config   config
   :err.proc/messages msgs})

(defn- wrap-processor-fn
  [f]
  (fn [process msgs]
    (err/on-error (processor-error process msgs)
      (f process msgs))))

(defn- wrap-error-handling
  [process]
  (-> process
      (assoc :error-fn (get-error-fn process))
      (update :fn wrap-processor-fn)))

(defn- processor-loop*
  [{:keys [run-fn error-fn]
    :or   {run-fn run-fn!!}
    :as   process}]
  {:pre [error-fn run-fn]}
  (while (running? process)
    (let [p (snapshot process)]
      (try
        (processor-write p (run-fn p))
        (catch Throwable e
          (error-fn p e))))))

(def ^:private processor-loop
  (comp processor-loop* wrap-error-handling))

(defn- join-tailers
  [{:keys [tid in]} stop]
  (some->> in
           (util/seqify)
           (map #(tailer % tid stop))
           (doall)))

(defn- fork-appenders
  [{:keys [out]}]
  (some->> out
           (util/seqify)
           (map appender)
           (doall)))

(defn- join
  "Reads messages from a seq of input tailers, applies a processor fn,
  and writes the reuslt to a single output appender. While queues can
  be shared across threads, appenders and tailers cannot. They must be
  created in the thread they are meant to be used in to avoid errors."
  [{:keys [out] :as process} stop]
  [(future
     (processor-loop
      (assoc process
             :stop      stop
             :appender  (appender out)
             :tailers   (join-tailers process stop))))])

(defn- fork-run!!
  "Conditionally passes message from the join backing queue onto to the
  fork output queue, iff a message for the output queue exists in the
  map. Note the processor will only ever have one input queue (the
  join backing queue) and one output queue."
  [{{out-id :id} :out
    [tailer]     :tailers
    :as          process}]
  (if-let [msg (read!! tailer)]
    (get msg out-id)
    (recover process)))

(defn- join-fork
  [{:keys [in out] :as process} stop backing-queue]
  (doall
   (flatten
    (cons
     (-> process
         (assoc :out       backing-queue
                :result-fn (fn [_ r] r))
         (join stop))
     (for [{id :id :as o} out]
       (-> process
           (assoc :tid    id
                  :run-fn fork-run!!
                  :in     backing-queue
                  :out    o)
           (join stop)))))))

(defn- zip
  [xs]
  (zipmap (map (comp :id :queue) xs) xs))

(defn- imp-tailers
  [{tid            :tid
    {t :tailers
     a :appenders} :imperative
    :as            process} stop]
  (let [p {:in  t
           :out a
           :tid (suffix-id "-i" tid)}]
    {:tailers   (zip (join-tailers p stop))
     :appenders (zip (fork-appenders p))}))

(defn- imperative
  [{:keys [out] :as process} stop]
  [(future
     (processor-loop
      (assoc process
             :stop       stop
             :tailers    (join-tailers process stop)
             :appender   (when out (appender out))
             :imperative (imp-tailers process stop))))])

(defn- sink
  [process stop]
  [(future
     (processor-loop
      (assoc process
             :stop    stop
             :tailers (join-tailers process stop))))])

(derive ::source     ::appender)
(derive ::source     ::processor)
(derive ::sink       ::processor)
(derive ::join       ::processor)
(derive ::join-fork  ::processor)
(derive ::imperative ::processor)

(defn processor?
  [x]
  (isa? (:type x) ::processor))

(defmulti start-processor-impl
  "Analogous to Kafka streams processor."
  :type)

(defmethod start-processor-impl ::source
  [{:keys [out] :as process}]
  (merge (appender out) process))

(defmethod start-processor-impl ::sink
  [{:keys [id] :as process}]
  (let [stop (atom nil)]
    (assoc process
           :stop stop
           :futures (sink process stop))))

(defmethod start-processor-impl ::join
  [{:keys [id] :as process}]
  (let [stop (atom nil)]
    (assoc process
           :stop stop
           :futures (join process stop))))

(defmethod start-processor-impl ::join-fork
  ;; Guarantees idempotency via backing queue.
  [{:keys [id config] :as process}]
  (let [stop (atom nil)
        p    (:queue-path config)
        q    (queue id {:transient  true
                        :queue-path p})]
    (assoc process
           :stop stop
           :futures (join-fork process stop q)
           :backing-queue q)))

(defmethod start-processor-impl ::imperative
  [{:keys [id] :as process}]
  (let [stop (atom nil)]
    (assoc process
           :stop stop
           :futures (imperative process stop))))

(defn coerce-cardinality-impl
  [[t form]]
  (case t
    :many-1 (first form)
    form))

(s/def ::id     qualified-keyword?)
(s/def :impl/fn fn?)

(s/def ::id-many-1
  (s/coll-of ::id
             :distinct true
             :kind util/collection?
             :count 1))

(s/def ::id-many
  (s/coll-of ::id
             :distinct true
             :kind util/collection?
             :min-count 2))

(s/def :one/in
  (util/conform-to
    (s/or :one    ::id
          :many-1 ::id-many-1)
    coerce-cardinality-impl))

(s/def :any/in
  (util/conform-to
    (s/or :one    ::id
          :many   ::id-many
          :many-1 ::id-many-1)
    coerce-cardinality-impl))

(s/def :one/out    :one/in)
(s/def :any/out    :any/in)
(s/def :many/out   ::id-many)
(s/def :many/alts  ::id-many)
(s/def ::tailers   :any/in)
(s/def ::appenders :any/out)

(s/def ::source
  (s/keys :req-un [:one/out]))

(s/def ::sink
  (s/keys :req-un [:impl/fn :any/in]))

(s/def ::join
  (s/keys :req-un [:impl/fn :any/in :one/out]))

(s/def ::join-fork
  (s/keys :req-un [:impl/fn :any/in :many/out]))

(s/def ::imperative
  (s/keys :req-un [:impl/fn :any/in (or ::tailers ::appenders)]
          :opt-un [:one/out]))

(s/def ::processor-impl
  (s/or ::imperative ::imperative
        ::join-fork  ::join-fork
        ::join       ::join
        ::source     ::source
        ::sink       ::sink))

(defn- parse-processor-impl
  [process]
  (let [[t p] (util/parse ::processor-impl process)]
    (-> p
        (assoc :type t)
        (assoc :tid (:id p)))))

(defn- get-one-q
  [g id]
  (get-in g [:queues id]))

(defn- get-q
  [g ids]
  (if (keyword? ids)
    (get-one-q g ids)
    (map (partial get-one-q g) ids)))

(def ^:private processor-config-keys
  [:id :in :out :tailers :appenders :topics :types])

(defn- build-config
  [{p :queue-path} process]
  (cond-> (select-keys process processor-config-keys)
    p (assoc :queue-path p)))

(defn- build-processor
  [{e      :error-queue
    system :system
    :as    g} {t   :tailers
               a   :appenders
               in  :in
               out :out
               :as process}]
  (cond-> process
    system  (assoc :system system)
    in      (assoc :in (get-q g in))
    out     (assoc :out (get-q g out))
    e       (assoc :error-queue (get-q g e))
    t       (assoc-in [:imperative :tailers] (get-q g t))
    a       (assoc-in [:imperative :appenders] (get-q g a))
    process (assoc :config (build-config g process))
    true    (assoc :state (atom nil))))

(defn- build-processors
  [g processors]
  (->> processors
       (map (partial build-processor g))
       (map (juxt :id identity))
       (update g :processors util/into-once)))

(defn- collate-queues
  [{:keys [in out appenders tailers]}]
  (concat (util/seqify in)
          (util/seqify out)
          (util/seqify appenders)
          (util/seqify tailers)))

(defn- build-queue
  [{tx-queue :tx-queue
    p        :queue-path
    opts     :queue-opts
    :as      g} id]
  (when (and id (not (get-one-q g id)))
    (->> {:tx-queue   (= tx-queue id)
          :queue-path p}
         (merge (get opts id))
         (queue id))))

(defn- build-queues
  [{:keys [error-queue] :as g} processors]
  (->> processors
       (mapcat collate-queues)
       (cons error-queue)
       (distinct)
       (keep (partial build-queue g))
       (map (juxt :id identity))
       (update g :queues util/into-once)))

(defn- queue-ids
  [x]
  (->> x
       (util/seqify)
       (map :id)
       (map (partial suffix-id "-q"))
       (set)))

(defn- remove-queues
  [p deps]
  (->> (:nodes deps)
       (map (fn [n] [(deps/two-hops deps n) n]))
       (filter (fn [[_ n]] (contains? p n)))
       (into {})
       (deps/graph)))

(defn- imperative-edges
  [id {a-q :appenders
       t-q :tailers}]
  (cond-> {}
    t-q (assoc (queue-ids t-q) id)
    a-q (assoc id (queue-ids a-q))))

(defn- processor-edges
  [{id  :id
    in  :in
    out :out
    i   :imperative}]
  (cond-> (imperative-edges id i)
    in  (assoc (queue-ids in) id)
    out (assoc id (queue-ids out))))

(defn topology
  [processors]
  (->> processors
       (vals)
       (map processor-edges)
       (set)
       (deps/graph)
       (remove-queues processors)))

(defn isomorphic?
  [g1 g2]
  (= (topology g1) (topology g2)))

(defn build-topology
  [graph]
  (->> (:processors graph)
       (topology)
       (assoc graph :topology)))

(defn graph
  [{p :processors :as config}]
  (let [p (map parse-processor-impl p)]
    (-> config
        (dissoc :processors)
        (build-queues p)
        (build-processors p)
        (build-topology))))

(defn- ensure-done
  [{:keys [futures]}]
  (when futures
    (doall (map deref futures))))

(defn start-processor!
  [{:keys [id state]
    :as   process}]
  (if (compare-and-set! state nil ::started)
    (do (log/info "Starting" id)
        (ensure-done process)
        (start-processor-impl process))
    process))

(defn stop-processor!
  [{:keys [id state stop]
    :as   process}]
  (if (compare-and-set! state ::started ::stopped)
    (do (log/info "Stopping" id)
        (when stop (reset! stop true))
        (ensure-done process)
        (assoc process :state (atom nil)))
    process))

(defn start-graph!
  [graph]
  (->> (partial util/map-vals start-processor!)
       (update graph :processors)))

(defn stop-graph!
  [graph]
  (->> (partial util/map-vals stop-processor!)
       (update graph :processors)))

(defn close-graph!
  [{:keys [processors queues]
    :as   graph}]
  (->> (vals processors)
       (keep :backing-queue)
       (map close!)
       (dorun))
  (->> (vals queues)
       (map close!)
       (dorun))
  graph)

(defmacro with-tailer
  [bindings & body]
  (let [b (take-nth 2 bindings)
        q (take-nth 2 (rest bindings))]
    `(let ~(->> q
                (map #(list `tailer %))
                (interleave b)
                vec)
       (try
         ~@body
         (finally
           (doseq [t# ~(vec b)]
             (close-tailer! t#)))))))

(defn messages
  "Returns a lazy list of all remaining messages."
  [tailer]
  (->> (partial read tailer)
       (repeatedly)
       (take-while some?)))

(defn all-messages
  "Careful, this eagerly gets all messages in the cue!"
  [queue]
  (with-tailer [t queue]
    (doall (messages t))))

(defn count-messages
  "Lazily counts all messages in the cue!"
  [queue]
  (with-tailer [t queue]
    (count (messages t))))

(defn all-graph-messages
  "Careful, this eagerly gets all messages in the graph!"
  [{queues :queues}]
  (let [queues* (vals queues)]
    (zipmap
     (map :id queues*)
     (map all-messages queues*))))

(defn send!
  ([g tx]
   (send! g nil tx))
  ([{p           :processors
     {s :source} :config
     :as         g} source msg]
   (if-let [a (get p (or source s))]
     (write a msg)
     (throw (ex-info "Could not find source" {:id source})))))

;;;; Processors

(s/def ::id-map        (s/map-of ::id keyword?))
(s/def ::in            ::id-map)
(s/def ::out           ::id-map)
(s/def :map/tailers    ::id-map)
(s/def :map/appenders  ::id-map)
(s/def :map/source     (s/keys :req-un [::id]))
(s/def ::fn            ::id)

(defn- coerce-cardinality
  [[t form]]
  (case t
    :one  [form]
    :many form))

(s/def ::types
  (util/conform-to
    (s/or :one  ::id
          :many (s/coll-of ::id))
    coerce-cardinality))

(s/def ::topics ::types)

(s/def ::processor-generic
  (s/keys :req-un [::id (or ::in ::out)]
          :opt-un [:map/tailers
                   :map/appenders
                   ::fn
                   ::types
                   ::topics]))

(s/def ::processor
  (s/or ::processor ::processor-generic
        ::source    :map/source))

(defmulti processor
  (constantly nil))

(defn- topics-filter
  [values msgs]
  (reduce-kv
   (fn [m id msg]
     (or (some->> values
                  (select-keys (:q/topics msg))
                  (not-empty)
                  (assoc msg :q/topics)
                  (assoc m id))
         m))
   nil
   msgs))

(defn- types-filter
  [values msgs]
  (->> msgs
       (filter #(some #{(:kr/type (val %))} values))
       (into {})
       (not-empty)))

(defn- rename-keys
  [key-map m]
  (cond-> m
    key-map (set/rename-keys key-map)))

(defn- wrap-guard-out
  [handler]
  (fn [process msgs]
    (let [r (handler process msgs)]
      (when (-> process
                :config
                :out)
        r))))

(defn- wrap-msg-keys
  [handler in-map out-map]
  (fn [process msgs]
    (some->> msgs
             (rename-keys in-map)
             (handler process)
             (rename-keys out-map))))

(defn- wrap-msg-filter
  [handler filter-fn]
  (fn [process msgs]
    (some->> msgs
             (filter-fn)
             (handler process))))

(defn- wrap-imperative
  [handler in-map out-map]
  (fn [{a   :appenders
        t   :tailers
        :as process} msgs]
    (cond-> process
      t    (assoc :tailers (rename-keys in-map t))
      a    (assoc :appenders (rename-keys out-map a))
      true (handler msgs))))

(defn- msgs->topics
  [msgs]
  (->> msgs
       (vals)
       (mapcat (comp keys :q/topics))
       (set)))

(defn- get-handler
  [{f  :fn
    id :id}]
  (or (get-method processor (or f id))
      (throw (ex-info "Unresolved fn" {:id id}))))

(defn- msg-filter-fn
  [{:keys [types topics]}]
  (cond
    topics (partial topics-filter topics)
    types  (partial types-filter types)
    :else  identity))

(defn- processor->fn
  [{:keys [id in out tailers appenders]
    :as   parsed}]
  (let [rev       (util/reverse-map out)
        filter-fn (msg-filter-fn parsed)]
    (-> parsed
        (get-handler)
        (wrap-guard-out)
        (wrap-imperative tailers appenders)
        (wrap-msg-filter filter-fn)
        (wrap-msg-keys in rev))))

(defn parse-processor
  [process]
  (let [[t parsed]          (util/parse ::processor process)
        {:keys [id
                in
                out
                topics
                opts
                tailers
                appenders]} parsed]
    (util/some-entries
     (case t
       ::processor {:id        id
                    :in        (keys in)
                    :out       (keys out)
                    :topics    topics
                    :tailers   (keys tailers)
                    :appenders (keys appenders)
                    :opts      opts
                    :fn        (processor->fn parsed)}
       ::source    {:id   id
                    :out  id
                    :opts opts}))))

(defn parse-graph
  [g]
  (-> g
      (assoc :config g)
      (update :processors (partial mapv parse-processor))))

;; Utility

(defn delete-queue!
  "Caution: failing to purge the queue controller when deleting a queue
  will cause blocking to break the next time the queue is made."
  ([queue]
   (delete-queue! queue false))
  ([{:keys [id]
     :as   queue} force]
   {:pre [(queue? queue)]}
   (let [p (queue-path queue)]
     (when (or force (util/prompt-delete-data! p))
       (close! queue)
       (controllers/purge-controller id)
       (util/delete-file (io/file p))))))

(defn delete-graph-queues!
  [g]
  (stop-graph! g)
  (doseq [q (vals (:queues g))]
    (delete-queue! q true)))

(defn delete-all-queues!
  "Caution: failing to purge the queue controller when deleting a queue
  will cause blocking to break the next time the queue is made."
  ([]
   (delete-all-queues! queue-path-default))
  ([queue-path]
   (when (util/prompt-delete-data! queue-path)
     (reset! controllers/cache {})
     (util/delete-file (io/file queue-path)))))

(defn unused-queue-files
  [queue]
  (let [p (queue-path queue)]
    (-> (io/file p)
        (FileUtil/removableRollFileCandidates)
        (.collect (Collectors/toList)))))

(defn delete-unused-queue-files!
  [queue]
  (doseq [^File f (unused-queue-files queue)]
    (.delete f)))
