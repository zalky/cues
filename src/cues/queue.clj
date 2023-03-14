(ns cues.queue
  "Core Cues API."
  (:refer-clojure :exclude [read peek])
  (:require [cinch.core :as util]
            [cinch.spec :as s*]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [cues.controller-cache :as controllers]
            [cues.deps :as deps]
            [cues.error :as err]
            [cues.util :as cutil]
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
           net.openhft.chronicle.bytes.Bytes
           net.openhft.chronicle.queue.ExcerptAppender
           [net.openhft.chronicle.queue.impl.single SingleChronicleQueue StoreTailer]
           net.openhft.chronicle.queue.util.FileUtil
           net.openhft.chronicle.wire.DocumentContext))

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
  (if (or (sequential? x) (set? x))
    (every? queue? x)
    (queue? x)))

(defn id->str
  [id]
  (cond
    (qualified-keyword? id) (str (namespace id) "/" (name id))
    (keyword? id)           (name id)
    (uuid? id)              (str id)
    (string? id)            id))

(defn- queue-path
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
  "Disambiguates between similar ids within a topology."
  [id suffix]
  (cond
    (keyword? id) (keyword (namespace id) (str (name id) suffix))
    (uuid? id)    (str id suffix)
    (string? id)  (str id suffix)))

(defn- combined-id
  [& ids]
  (when-let [ids (not-empty (remove nil? ids))]
    (->> ids
         (map id->str)
         (interpose ".")
         (apply str))))

(declare last-index)

(def transient-queue-opts
  "Transient queues are purged after 10 days."
  {:roll-cycle          :small-daily
   :cycle-release-tasks [{:type         :delete
                          :after-cycles 10}]})

(defn- queue-opts
  [{:keys [transient] :as opts}]
  (cond-> (-> opts
              (assoc :codec (codec))
              (dissoc :transient))
    (true? transient) (merge transient-queue-opts)))

(defn queue
  "Creates a persistent blocking queue.

  Options include:

  :transient
            A Boolean. If true, queue backing file will be rolled
            daily, and deleted after 10 days. You can configure
            alternative behaviour using the :roll-cycle,
            :cycle-release-tasks and :cycle-acquire-tasks options
            described below.

  The following options are passed along to the underlying
  implementation:

  :roll-cycle
            How frequently the queue data file on disk is rolled
            over. Default is :small-daily. Can be:

            :minutely, :daily, :test4-daily, :test-hourly, :hourly,
            :test-secondly, :huge-daily-xsparse, :test-daily,
            :large-hourly-xsparse, :large-daily, :test2-daily,
            :xlarge-daily, :huge-daily, :large-hourly, :small-daily,
            :large-hourly-sparse

  :autoclose-on-jvm-exit?
            A Boolean. Whether to cleanly close the JVM on
            exit. Default is true.

  :cycle-release-tasks
            Tasks to run on queue cycle release. For more, see
            qbits.tape.cycle-listener in the Tape library.

  :cycle-acquire-tasks
            Tasks to run on queue cycle acquisition. For more, see
            qbits.tape.cycle-listener in the Tape library."
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

(defn- tailer-id
  [queue id]
  (when id
    (combined-id id (:id queue))))

(defn- tailer-opts
  [tid]
  {:id (some-> tid id->str)})

(defn tailer
  "Creates a tailer.

  Providing an id enables tailer position on the queue to persist
  across restarts. You can also optionally provide an unblock
  reference that when set to true will prevent the tailer from
  blocking. This is typically used to unblock and clean up blocked
  processor threads."
  ([queue] (tailer queue nil nil))
  ([queue id] (tailer queue id nil))
  ([queue id unblock]
   {:pre [(queue? queue)]}
   (let [tid  (tailer-id queue id)
         opts (tailer-opts tid)
         t    (tail/make (:queue-impl queue) opts)]
     (prime-tailer
      {:type        ::tailer
       :id          tid
       :unblock     unblock
       :queue       queue
       :dirty       (atom false)
       :tailer-impl t}))))

(defn appender
  [{q :queue-impl :as queue}]
  {:pre [(queue? queue)]}
  {:type          ::appender
   :queue         queue
   :appender-impl (app/make q)})

(def queue-closed?
  (comp queue/closed? :queue-impl))

(defn set-direction
  "Sets the direction of the tailer to either :forward
  or :backward. Note: after changing the direction on the tailer you
  must do a read before you can measure the index again via
  `cues.queue/index`. This is an artifact of ChronicleQueue
  behaviour."
  [{t   :tailer-impl
    d   :dirty
    :as tailer} dir]
  (tail/set-direction! t dir)
  (reset! d true)
  tailer)

(defn to-end
  "Moves the tailer to the end of the queue."
  [{t   :tailer-impl
    d   :dirty
    :as tailer}]
  (tail/to-end! t)
  (reset! d false)
  tailer)

(defn to-start
  "Moves the tailer to the beginning of the queue."
  [{t   :tailer-impl
    d   :dirty
    :as tailer}]
  (tail/to-start! t)
  (reset! d false)
  tailer)

(defn index*
  "Gets the index at the tailer's current position. ChronicleQueue
  tailers do not update their current index after changing direction
  until AFTER the next read. Cues guards against this edge case by
  throwing an error if you attempt to take the index before the next
  read."
  [{t   :tailer-impl
    d   :dirty
    :as tailer}]
  (if-not @d
    (tail/index t)
    (-> "Cannot take the index of a tailer after setting direction without first doing a read"
        (ex-info tailer)
        (throw))))

(def ^:dynamic index
  "Only rebind for testing!"
  index*)

(def queue-obj
  "Returns the underlying ChronicleQueue object."
  (comp queue/underlying-queue :queue-impl))

(defn close-tailer!
  "Closes the given tailer."
  [tailer]
  (-> tailer
      (:tailer-impl)
      (tail/underlying-tailer)
      (.close))
  tailer)

(defn close-queue!
  "Closes the given queue."
  [{id  :id
    q   :queue-impl
    :as queue}]
  {:pre [(queue? queue)]}
  (controllers/purge id)
  (queue/close! q)
  (System/runFinalization))

(defn to-index*
  [{t   :tailer-impl
    d   :dirty
    :as tailer} i]
  (if (zero? i)
    (tail/to-start! t)
    (tail/to-index! t i))
  (reset! d false)
  tailer)

(def ^:dynamic to-index
  "Only rebind for testing!"
  to-index*)

(defmacro with-tailer
  [bindings & body]
  (let [b (take-nth 2 bindings)
        q (take-nth 2 (rest bindings))]
    `(let ~(->> q
                (map #(list `tailer %))
                (interleave b)
                (vec))
       (try
         ~@body
         (finally
           (doseq [t# ~(vec b)]
             (close-tailer! t#)))))))

(defn- prime-tailer
  "In certain rare cases, a newly initialized StoreTailer will start
  reading from the second index, but reports as if it had started at
  the first. Forcing the tailer explicitly to the index it reports to
  be at mitigates the issue. This is always done on tailer
  initialization."
  [tailer]
  {:pre [(tailer? tailer)]}
  (->> (index* tailer)
       (to-index* tailer)))

(defn last-index*
  "A last index implementation that works for any kind of queue or
  appender. Note that appenders and queues will not necessarily return
  the same result, and appenders will throw an error if they have not
  appended anything yet."
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

(def ^:dynamic last-index
  "Only rebind for testing!"
  last-index*)

(defn last-read-index*
  [tailer]
  {:pre [(tailer? tailer)]}
  (let [^StoreTailer t (->> tailer
                            (:tailer-impl)
                            (tail/underlying-tailer))]
    (.lastReadIndex t)))

(def ^:dynamic last-read-index
  "Only rebind for testing!"
  last-read-index*)

(defn- dissoc-hash
  [msg]
  (if-let [m (-> msg
                 (:q/meta)
                 (dissoc :q/hash)
                 (not-empty))]
    (assoc msg :q/meta m)
    (dissoc msg :q/meta)))

(defn- materialize-meta
  [id tailer {m :q/meta :as msg}]
  (let [tx? (true? (get m :tx/t))
        t?  (true? (get-in m [:q/queue id :q/t]))
        t   (when (or tx? t?)
              (last-read-index tailer))]
    (cond-> msg
      tx? (assoc-in [:q/meta :tx/t] t)
      t?  (assoc-in [:q/meta :q/queue id :q/t] t))))

(defn- read-with-hash
  [{{id :id} :queue
    t-impl   :tailer-impl
    dirty    :dirty
    :as      tailer}]
  {:pre [(tailer? tailer)]}
  (let [msg (tail/read! t-impl)]
    (reset! dirty false)
    (when msg
      (materialize-meta id tailer msg))))

(defn read
  "Reads message from tailer without blocking. Materializes metadata in
  message."
  [tailer]
  (-> tailer
      (read-with-hash)
      (dissoc-hash)))

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

(def ^:dynamic written-index
  "Returns the index written by the appender. Only rebind for testing!"
  (fn [_ i] i))

(defn- lift-set
  [x]
  (if (set? x) x #{}))

(defn- add-meta
  [{id              :id
    {m :queue-meta} :opts} msg]
  (let [m* (lift-set m)
        t  (get m* :q/t)
        tx (get m* :tx/t)
        ts (get m* :q/time)]
    (cond-> msg
      (false? m) (dissoc :q/meta)
      t          (assoc-in [:q/meta :q/queue id :q/t] true)
      tx         (assoc-in [:q/meta :tx/t] true)
      ts         (assoc-in [:q/meta :q/queue id :q/time]
                           (Instant/now)))))

(defn- wrap-write
  [write-fn]
  (fn [{q   :queue
        a   :appender-impl
        :as appender} msg]
    {:pre [(appender? appender)
           (map? msg)]}
    (let [index (->> msg
                     (add-meta q)
                     (write-fn a))]
      (controller-inc! q index)
      (written-index q index))))

(defn write
  "The queue controller approximately follows the index of the queue: it
  can fall behind, but must be eventually consistent."
  [appender msg]
  (let [f (wrap-write app/write!)]
    (f appender msg)))

(defn- watch-controller!
  [{id              :id
    {c :controller} :queue
    :as             tailer} continue]
  (when c
    (let [tailer-i (index tailer)]
      (add-watch c id
        (fn [id _ _ i]
          (when (<= tailer-i i)
            (deliver continue tailer)))))))

(defn- watch-unblock!
  [{id      :id
    unblock :unblock} continue]
  (when unblock
    (add-watch unblock id
      (fn [id _ _ new]
        (when (true? new)
          (deliver continue nil))))
    (swap! unblock identity)))

(defn- rm-watches
  [{id              :id
    {c :controller} :queue
    unblock         :unblock}]
  (when c
    (remove-watch c id))
  (when unblock
    (remove-watch unblock id)))

(defn continue?
  [{unblock :unblock
    :as     obj}]
  (and (some? obj)
       (or (not (instance? IRef unblock))
           (not @unblock))))

(defn- try-read
  [tailer]
  (when (continue? tailer)
    (loop [n 10000]
      (if (pos? n)
        (or (read tailer)
            (recur (dec n)))
        (throw (ex-info "Max read tries" {:tailer tailer}))))))

(defn read!!
  "Blocking read. The tailer will consume eagerly if it can. If not, it
  will block until a new message is available. Implementation note: it
  is critical that the watch be placed before the first read. Note
  that the tailer index will always be ahead of the last index it
  read. Read will continue and return nil if at any point an unblock
  signal is received. This allows blocked threads to be cleaned up on
  unblock events."
  [tailer]
  {:pre [(tailer? tailer)]}
  (let [continue (promise)]
    (watch-controller! tailer continue)
    (watch-unblock! tailer continue)
    (if-let [msg (read tailer)]
      (do (rm-watches tailer) msg)
      (let [t @continue]
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
  (let [continue (promise)]
    (doseq [t tailers]
      (watch-controller! t continue)
      (watch-unblock! t continue))
    (or (alts tailers)
        (let [t @continue]
          (doseq [t* tailers]
            (rm-watches t*))
          (some->> (try-read t)
                   (vector t))))))

(defmulti persistent-snapshot
  "Persists processor tailer indices to a backing queue. Called once
  before each processor step. Taken together, persistent-snapshot,
  persistent-attempt, and persistent-recover implement message
  delivery semantics in Cues processors."
  (comp :strategy :config))

(defmulti persistent-attempt
  "Persists message to processor output queues, as well as attempt data
  to backing queue. Called once during each processor step. Taken
  together, persistent-snapshot, persistent-attempt, and
  persistent-recover implement message delivery semantics in Cues
  processors."
  (fn [process _]
    (-> process :config :strategy)))

(defmulti persistent-recover
  "Recovers processor tailer indices from backing queue once on
  processor start. Taken together, persistent-snapshot,
  persistent-attempt, and persistent-recover implement message
  delivery semantics in Cues processors."
  (comp :strategy :config))

(defn- processor-error
  [{config :config} msgs]
  {:q/type            :q.type.err/processor
   :err.proc/config   config
   :err.proc/messages msgs})

(defn- snapshot-map
  [id tailer-indices]
  {:q/type               :q.type.try/snapshot
   :q.try/proc-id        id
   :q.try/tailer-indices tailer-indices})

(defn- attempt-type
  [process]
  (if (= (:id (:error-queue process))
         (:id (:queue (:appender process))))
    :q.type.try/attempt-error
    :q.type.try/attempt))

(defn- attempt-map
  [process attempt-hash msg-index]
  {:q/type              (attempt-type process)
   :q/hash              attempt-hash
   :q.try/message-index msg-index})

(defn- nil-attempt-map
  [attempt-hash]
  {:q/type :q.type.try/attempt-nil
   :q/hash attempt-hash})

(defmethod persistent-snapshot ::exactly-once
  [{uid     :uid
    try-a   :try-appender
    tailers :tailers
    :as     process}]
  {:pre [(appender? try-a)]}
  (let [m (->> tailers
               (map (juxt :id index))
               (into {})
               (snapshot-map uid))]
    (write try-a m)
    (assoc process :snapshot-hash (hash m))))

(def ^:dynamic add-attempt-hash
  "Only rebind in testing."
  (fn [attempt-hash msg]
    (assoc-in msg [:q/meta :q/hash] attempt-hash)))

(defn- encode-msg ^Bytes
  [appender msg attempt-hash]
  (let [codec (-> appender
                  (:queue)
                  (:queue-impl)
                  (queue/codec))]
    (->> msg
         (add-attempt-hash attempt-hash)
         (codec/write codec)
         (Bytes/wrapForRead))))

(defn- appender-obj ^ExcerptAppender
  [appender]
  (-> appender
      (:appender-impl)
      (app/underlying-appender)))

(def ^:dynamic attempt-index
  "Only rebind for tesitng!"
  (fn [_ i] i))

(defn- wrap-attempt
  [{a     :appender
    try-a :try-appender
    h     :snapshot-hash
    :as   process}]
  (fn [_ msg]
    (let [write-a (appender-obj a)
          msg*    (encode-msg a msg h)]
      (with-open [doc (.writingDocument write-a)]
        (try
          (-> doc
              (.wire)
              (.write)
              (.bytes msg*))
          (let [i (.index doc)]
            (->> i
                 (attempt-index a)
                 (attempt-map process h)
                 (write try-a))
            i)
          (catch Throwable e
            (.rollbackOnClose doc)
            e))))))

(defn- wrap-throwable
  [write-fn]
  (fn [appender msg]
    (let [x (write-fn appender msg)]
      (when (instance? Throwable x)
        (throw x))
      x)))

(defn- full-attempt
  [{a     :appender
    :as   process} msg]
  (let [f (-> process
              (wrap-attempt)
              (wrap-throwable)
              (wrap-write))]
    (f a msg)))

(defn- nil-attempt
  [{try-a :try-appender
    h     :snapshot-hash}]
  {:pre [(appender? try-a)]}
  (->> h
       (nil-attempt-map)
       (write try-a)))

(defmethod persistent-attempt ::exactly-once
  [process msg]
  (err/on-error (processor-error process msg)
    (if (and (:appender process) msg)
      (full-attempt process msg)
      (nil-attempt process))
    true))

(defn- snapshot?
  [msg]
  (= (:q/type msg) :q.type.try/snapshot))

(defn- throw-recover-error!
  [process-id tailer-id]
  (-> "No tailer recovery index"
      (ex-info {:tailer    tailer-id
                :processor process-id})
      (throw)))

(defn- recover-snapshot
  [{process-id :id
    tailers    :tailers} snapshot]
  {:pre [(snapshot? snapshot)]}
  (doseq [{tid :id :as t} tailers]
    (if-let [i (-> snapshot
                   (:q.try/tailer-indices)
                   (get tid))]
      (to-index t i)
      (log/warn "Topology changed, cannot recover tailer" tid))))

(defn- next-snapshot
  [try-tailer]
  (->> #(read try-tailer)
       (repeatedly)
       (take-while some?)
       (some #(when (= (:q/type %) :q.type.try/snapshot) %))))

(defn- recover-attempt
  [try-tailer {{q :queue} :appender
               :as        process} {i :q.try/message-index
                                    h :q/hash}]
  (with-tailer [t q]
    (when-not (= h (-> t
                       (to-index i)
                       (read-with-hash)
                       (:q/meta)
                       (:q/hash)))
      (->> (next-snapshot try-tailer)
           (recover-snapshot process)))))

(defn- recover-attempt-error
  [try-tailer process msg]
  (let [p (->> (:error-queue process)
               (appender)
               (assoc process :appender))]
    (recover-attempt try-tailer p msg)))

(defn- recover-attempt-nil
  [try-tailer process {h :q/hash}]
  (let [snapshot (next-snapshot try-tailer)]
    (when-not (= h (hash snapshot))
      (recover-snapshot process snapshot))))

(defn- recover
  [try-tailer process msg]
  (case (:q/type msg)
    :q.type.try/snapshot      (recover-snapshot process msg)
    :q.type.try/attempt       (recover-attempt try-tailer process msg)
    :q.type.try/attempt-error (recover-attempt-error try-tailer process msg)
    :q.type.try/attempt-nil   (recover-attempt-nil try-tailer process msg)
    nil))

(defmethod persistent-recover ::exactly-once
  [{{q :queue} :try-appender
    :as        process}]
  (try
    (with-tailer [t q]
      (set-direction t :backward)
      (some->> q
               (last-index)
               (to-index t)
               (read-with-hash)
               (recover t process)))
    (catch InterruptedException e false)
    (catch Throwable e
      (log/error e "Could not recover tailers")
      false)))

(defn- snapshot-mem
  "Record tailer indices for unblock recovery."
  [{tailers :tailers
    :as     process}]
  (->> tailers
       (map (juxt identity index))
       (doall)
       (assoc process :snapshot-mem)))

(defn throw-interrupt!
  []
  (throw (InterruptedException. "Interrupting processor")))

(defn- recover-mem
  "Reset tailer positions and interrupts the processor."
  ([process]
   (recover-mem process (constantly true)))
  ([{snapshot :snapshot-mem} pred?]
   (doseq [[tailer i] snapshot]
     (when (pred? tailer)
       (to-index tailer i)))
   (throw-interrupt!)))

(defn- merge-meta?
  [config]
  (get-in config [:queue-opts :queue-meta] true))

(defn- merge-meta-in
  [in]
  (->> (vals in)
       (keep :q/meta)
       (apply util/merge-deep)))

(defn- assoc-meta-out
  [out m]
  (reduce-kv
   (fn [out k v]
     (cond-> out
       v (assoc k (assoc v :q/meta m))))
   {}
   out))

(defn- merge-meta
  [in out config]
  (when out
    (if (merge-meta? config)
      (if-let [m (merge-meta-in in)]
        (assoc-meta-out out m)
        out)
      out)))

(defn- get-result
  [process result]
  (get result (-> process
                  (:appender)
                  (:queue)
                  (:id))))

(defn- zip-read!!
  "Blocking read from all tailers into a map. Returns nil if any one of
  the blocking tailers returns nil."
  [{tailers :tailers}]
  (let [vals (map read!! tailers)
        keys (map (comp :id :queue) tailers)]
    (when (every? some? vals)
      (zipmap keys vals))))

(defn- alts-read!!
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
  "Keys passed on to the processor fn."
  [{imperative :imperative
    :as        process}]
  (merge process imperative))

(defn- run-fn!!
  [{f         :fn
    appender  :appender
    result-fn :result-fn
    config    :config
    :or       {result-fn get-result}
    :as       process}]
  (if-let [in (processor-read process)]
    (let [out (-> process
                  (select-processor)
                  (f in))]
      (when appender
        (->> (merge-meta in out config)
             (result-fn process))))
    (recover-mem process)))

(defn log-processor-error
  [{{:keys [id in out]
     :or   {in  "source"
            out "sink"}} :config} e]
  (log/error e (format "%s (%s -> %s)" id in out)))

(defn- get-error-fn
  [{:keys [error-queue]}]
  (let [a (some-> error-queue appender)]
    (fn [process e]
      (let [p (assoc process :appender a)]
        (log-processor-error process e)
        (->> (err/error e)
             (ex-data)
             (persistent-attempt p))))))

(defn- wrap-processor-fn
  [f]
  (fn [process msgs]
    (err/on-error (processor-error process msgs)
      (f process msgs))))

(defn- wrap-error-handling
  [process]
  (-> process
      (util/assoc-nil :error-fn (get-error-fn process))
      (update :fn wrap-processor-fn)))

(defn- close-tailers!
  [process]
  (doseq [t (:tailers process)]
    (close-tailer! t))
  (doseq [t (-> process
                (:imperative)
                (:tailers)
                (vals))]
    (close-tailer! t)))

(defn- processor-snapshot
  [process]
  (try
    (-> process
        (snapshot-mem)
        (persistent-snapshot))
    (catch InterruptedException e false)
    (catch Throwable e
      (log/error e "Could not snapshot tailers")
      false)))

(defn- processor-step
  [{:keys [run-fn error-fn]
    :or   {run-fn run-fn!!}
    :as   process}]
  {:pre [error-fn run-fn]}
  (let [p (processor-snapshot process)]
    (try
      (persistent-attempt p (run-fn p))
      (catch InterruptedException e false)
      (catch Throwable e
        (error-fn p e)
        true))))

(defn- processor-loop*
  [process]
  (try
    (persistent-recover process)
    (while (and (continue? process)
                (processor-step process)))
    (finally
      (close-tailers! process))))

(def ^:private processor-loop
  (comp processor-loop* wrap-error-handling))

(defn- join-tailers
  [{:keys [uid in]} unblock]
  (some->> in
           (util/seqify)
           (map #(tailer % uid unblock))
           (doall)))

(defn- fork-appenders
  [{:keys [out]}]
  (some->> out
           (util/seqify)
           (map appender)
           (doall)))

(defn- backing-queue
  [{:keys [id uid config]}]
  (let [p (-> config
              (:queue-opts)
              (:queue-path))]
    (queue uid {:transient  true
                :queue-path p})))

(defn- start-impl
  [process unblock start-fn]
  (let [q (backing-queue process)]
    {:try-queues [q]
     :futures    (start-fn process unblock q)}))

(defn- start-join
  "Reads messages from a seq of input tailers, applies a processor fn,
  and writes the result to a single output appender. While queues can
  be shared across threads, appenders and tailers cannot."
  [{:keys [out] :as process} unblock try-queue]
  [(future
     (processor-loop
      (assoc process
             :unblock      unblock
             :tailers      (join-tailers process unblock)
             :appender     (appender out)
             :try-appender (appender try-queue))))])

(defn- fork-run-fn!!
  "Conditionally passes message from one input queue, usually a backing
  queue, onto to the fork output queue, iff a message for the output
  queue exists in the map."
  [{{oid :id} :out
    [tailer]  :tailers
    :as       process}]
  (if-let [msg (read!! tailer)]
    (get msg oid)
    (recover-mem process)))

(defn- start-jf-join
  [process unblock fork-queue]
  (-> process
      (assoc :out       fork-queue
             :result-fn (fn [_ r] r))
      (start-impl unblock start-join)))

(defn- start-jf-fork
  [{:keys [id uid out] :as process} unblock fork-queue]
  (doall
   (for [{oid :id :as o} out]
     (-> process
         (assoc :id     (combined-id id oid)
                :run-fn fork-run-fn!!
                :in     fork-queue
                :out    o
                :uid    (combined-id uid oid))
         (start-impl unblock start-join)))))

(defn- fork-id
  [process]
  (combined-id (:uid process) :fork))

(defn- start-join-fork
  "An backing queue used to coordinate between fork and join."
  [process unblock]
  (let [q (->> (fork-id process)
               (assoc process :uid)
               (backing-queue))
        j (start-jf-join process unblock q)
        f (start-jf-fork process unblock q)]
    (-> (apply merge-with concat j f)
        (assoc :fork-queue q))))

(defn- zip
  [xs]
  (zipmap (map (comp :id :queue) xs) xs))

(defn- imp-tailers
  [{uid            :uid
    {t :tailers
     a :appenders} :imperative
    :as            process} unblock]
  (let [p {:in  t
           :out a
           :uid (suffix-id uid "-i")}]
    {:tailers   (zip (join-tailers p unblock))
     :appenders (zip (fork-appenders p))}))

(defn- start-imperative
  [{:keys [out] :as process} unblock try-queue]
  [(future
     (processor-loop
      (assoc process
             :unblock      unblock
             :tailers      (join-tailers process unblock)
             :appender     (when out (appender out))
             :try-appender (appender try-queue)
             :imperative   (imp-tailers process unblock))))])

(defn- start-sink
  [process unblock try-queue]
  [(future
     (processor-loop
      (assoc process
             :unblock      unblock
             :tailers      (join-tailers process unblock)
             :try-appender (appender try-queue))))])

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
  "Analogous to Kafka processors."
  :type)

(defmethod start-processor-impl ::source
  [{:keys [out] :as process}]
  (merge (appender out) process))

(defmethod start-processor-impl ::sink
  [{:keys [id] :as process}]
  (let [unblock (atom nil)]
    (-> process
        (merge (start-impl process unblock start-sink))
        (assoc :unblock unblock))))

(defmethod start-processor-impl ::join
  [{:keys [id] :as process}]
  (let [unblock (atom nil)]
    (-> process
        (merge (start-impl process unblock start-join))
        (assoc :unblock unblock))))

(defmethod start-processor-impl ::join-fork
  [process]
  (let [unblock (atom nil)]
    (-> process
        (merge (start-join-fork process unblock))
        (assoc :unblock unblock))))

(defmethod start-processor-impl ::imperative
  [{:keys [id] :as process}]
  (let [unblock (atom nil)]
    (-> process
        (merge (start-impl process unblock start-imperative))
        (assoc :unblock unblock))))

(defn- coerce-cardinality-impl
  [[t form]]
  (case t
    :many-1 (first form)
    form))

(s/def :impl/fn fn?)

(s/def ::id
  (s*/non-conformer
   (s/or :k qualified-keyword?
         :s string?)))

(s/def ::id-many-1
  (s/coll-of ::id
             :distinct true
             :kind #(or (sequential? %) (set? %))
             :count 1))

(s/def ::id-many
  (s/coll-of ::id
             :distinct true
             :kind #(or (sequential? %) (set? %))
             :min-count 2))

(s/def :one/in
  (s*/conform-to
    (s/or :one    ::id
          :many-1 ::id-many-1)
    coerce-cardinality-impl))

(s/def :any/in
  (s*/conform-to
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
  (let [[t p] (s*/parse ::processor-impl process)]
    (-> p
        (assoc :type t)
        (assoc :uid (:id p)))))

(defn- get-one-q
  [g id]
  (get-in g [:queues id]))

(defn- get-q
  [g ids]
  (if (coll? ids)
    (map (partial get-one-q g) ids)
    (get-one-q g ids)))

(def ^:private processor-config-keys
  [:id :in :out :tailers :appenders :topics :types])

(defn- build-config
  "If default queues opts are provided, they are also used for processor
  backing queues."
  [g process]
  (let [default (get-in g [:queue-opts ::default])
        s       (:strategy g ::exactly-once)]
    (-> process
        (select-keys processor-config-keys)
        (assoc :queue-opts default)
        (assoc :strategy s))))

(defn- build-processor
  [{id     :id
    e      :error-queue
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
    id      (update :uid (partial combined-id id))
    true    (assoc :state (atom nil))))

(defn- build-processors
  [g processors]
  (->> processors
       (map (partial build-processor g))
       (map (juxt :id identity))
       (update g :processors cutil/into-once)))

(defn- collect-processor-queues
  [{:keys [in out appenders tailers]}]
  (concat (util/seqify in)
          (util/seqify out)
          (util/seqify appenders)
          (util/seqify tailers)))

(defn- build-queue
  [{opts :queue-opts :as g} id]
  (when (and id (not (get-one-q g id)))
    (->> (get opts id)
         (merge (::default opts))
         (queue id))))

(defn- build-queues
  [{:keys [error-queue] :as g} processors]
  (->> processors
       (mapcat collect-processor-queues)
       (cons error-queue)
       (distinct)
       (keep (partial build-queue g))
       (map (juxt :id identity))
       (update g :queues cutil/into-once)))

(defn- queue-ids
  [x]
  (->> x
       (util/seqify)
       (map :id)
       (map #(suffix-id % "-q"))
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

(defn- build-topology*
  [processors]
  (->> processors
       (vals)
       (map processor-edges)
       (set)
       (deps/graph)
       (remove-queues processors)))

(defn- build-topology
  [graph]
  (->> (:processors graph)
       (build-topology*)
       (assoc graph :topology)))

(defn- build-graph
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
  [{:keys [id state unblock]
    :as   process}]
  (if (compare-and-set! state ::started ::stopped)
    (do (log/info "Stopping" id)
        (when unblock (reset! unblock true))
        (ensure-done process)
        (assoc process :state (atom nil)))
    process))

(defn start-graph!
  [graph]
  (->> (partial cutil/map-vals start-processor!)
       (update graph :processors)))

(defn stop-graph!
  [graph]
  (->> (partial cutil/map-vals stop-processor!)
       (update graph :processors)))

(defn collect-graph-queues
  [{:keys [processors queues]}]
  (concat
   (->> (vals processors)
        (keep :fork-queue))
   (->> (vals processors)
        (mapcat :try-queues))
   (->> (vals queues))))

(defn close-graph!
  [graph]
  (->> graph
       (collect-graph-queues)
       (map close-queue!)
       (dorun))
  graph)

(defn messages
  "Returns a lazy list of all remaining messages."
  [tailer]
  (->> (partial read tailer)
       (repeatedly)
       (take-while some?)))

(defn all-messages
  "Eagerly gets all messages in the cue. Could be many!"
  [queue]
  (with-tailer [t queue]
    (doall (messages t))))

(defn graph-messages
  "Eagerly gets all messages in the queue. Could be many!"
  [{queues :queues} id]
  (-> queues
      (get id)
      (all-messages)))

(defn all-graph-messages
  "Eagerly gets all messages in the graph. Could be many!"
  [{queues :queues}]
  (let [queues* (vals queues)]
    (zipmap
     (map :id queues*)
     (map all-messages queues*))))

(s/def ::id-map       (s/map-of keyword? ::id))
(s/def ::in           ::id-map)
(s/def ::out          ::id-map)
(s/def :map/tailers   ::id-map)
(s/def :map/appenders ::id-map)
(s/def :map/source    (s/keys :req-un [::id]))
(s/def ::fn           ::id)

(defn- coerce-cardinality
  [[t form]]
  (case t
    :one  [form]
    :many form))

(s/def ::types
  (s*/conform-to
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

(s/def ::processors
  (s/coll-of ::processor :kind sequential?))

(s/def ::graph
  (s/keys :req-un [::processors ::id]))

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
       (filter #(some #{(:q/type (val %))} values))
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
                (:config)
                (:out))
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
  (let [rev       (set/map-invert in)
        t         (set/map-invert tailers)
        a         (set/map-invert appenders)
        filter-fn (msg-filter-fn parsed)]
    (-> parsed
        (get-handler)
        (wrap-guard-out)
        (wrap-imperative t a)
        (wrap-msg-filter filter-fn)
        (wrap-msg-keys rev out))))

(defn- parse-processor
  [process]
  (let [[t parsed]          process
        {:keys [id
                in
                out
                topics
                opts
                tailers
                appenders]} parsed]
    (cutil/some-entries
     (case t
       ::processor {:id        id
                    :in        (vals in)
                    :out       (vals out)
                    :topics    topics
                    :tailers   (vals tailers)
                    :appenders (vals appenders)
                    :opts      opts
                    :fn        (processor->fn parsed)}
       ::source    {:id   id
                    :out  id
                    :opts opts}))))

(defn- parse-graph
  [g]
  (-> (s*/parse ::graph g)
      (assoc :config g)
      (update :processors (partial mapv parse-processor))))

(defn graph
  [g]
  (-> g
      (parse-graph)
      (build-graph)))

(defn topology
  [graph]
  (:topology graph))

(defn isomorphic?
  [g1 g2]
  (= (topology g1)
     (topology g2)))

(defn send!
  ([g tx]
   (send! g nil tx))
  ([{p           :processors
     {s :source} :config
     :as         g} source msg]
   (if-let [a (get p (or source s))]
     (write a msg)
     (throw (ex-info "Could not find source" {:id source})))))

;; Data file management

(defn delete-queue!
  "Deletes the queue data on disk, prompting by default.

  Implementation note: must also purge the queue controller or
  blocking will break the next time the queue is made."
  ([queue]
   (delete-queue! queue false))
  ([{:keys [id] :as queue} force]
   {:pre [(queue? queue)]}
   (let [p (queue-path queue)]
     (when (or force (cutil/prompt-delete! p))
       (close-queue! queue)
       (controllers/purge id)
       (-> p
           (io/file)
           (cutil/delete-file))))))

(defn close-and-delete-graph!
  "Closes the graph and all queues and deletes all queue data."
  ([g]
   (close-and-delete-graph! g false))
  ([g force]
   (let [queues (collect-graph-queues g)]
     (when (or force (-> (count queues)
                         (str " queues")
                         (cutil/prompt-delete!)))
       (stop-graph! g)
       (close-graph! g)
       (doseq [q queues]
         (delete-queue! q true))))))

(defn delete-all-queues!
  "Deletes all queue data at either the provided or default path.

  Implementation note: must also purge the queue controller or
  blocking will break the next time the queue is made."
  ([]
   (delete-all-queues! queue-path-default))
  ([queue-path]
   (when (cutil/prompt-delete! queue-path)
     (reset! controllers/cache {})
     (-> queue-path
         (io/file)
         (cutil/delete-file)))))

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
