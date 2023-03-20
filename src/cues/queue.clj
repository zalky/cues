(ns cues.queue
  "Core Cues API."
  (:refer-clojure :exclude [read peek])
  (:require [cinch.core :as util]
            [cinch.spec :as s*]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [cues.controllers :as controllers]
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

(defn queue?
  [x]
  (= (:type x) ::queue))

(defn tailer?
  [x]
  (= (:type x) ::tailer))

(defn appender?
  [x]
  (= (:type x) ::appender))

(defn- queue-any?
  [x]
  (if (or (sequential? x) (set? x))
    (every? queue? x)
    (queue? x)))

(defn id->str
  [id]
  {:pre [(keyword? id)]}
  (if (qualified-keyword? id)
    (str (namespace id) "/" (name id))
    (name id)))

(defn- absolute-path?
  [x]
  (and (string? x)
       (.isAbsolute (io/file x))))

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
  [id suffix]
  {:pre [(keyword? id)]}
  (keyword (namespace id)
           (str (name id) suffix)))

(defn- combined-id
  [id1 id2]
  {:pre [(keyword? id1)
         (keyword? id2)]}
  (let [ns1 (namespace id1)
        ns2 (namespace id2)
        n1  (name id1)
        n2  (name id2)]
    (keyword (->> [ns1 n1 ns2]
                  (remove nil?)
                  (interpose ".")
                  (apply str)
                  (not-empty))
             n2)))

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
      :controller (controllers/lookup path #(atom i))
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
    :as tailer} direction]
  {:pre [(tailer? tailer)]}
  (tail/set-direction! t direction)
  (reset! d true)
  tailer)

(defn to-end
  "Moves the tailer to the end of the queue."
  [{t   :tailer-impl
    d   :dirty
    :as tailer}]
  {:pre [(tailer? tailer)]}
  (tail/to-end! t)
  (reset! d false)
  tailer)

(defn to-start
  "Moves the tailer to the beginning of the queue."
  [{t   :tailer-impl
    d   :dirty
    :as tailer}]
  {:pre [(tailer? tailer)]}
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
  {:pre [(tailer? tailer)]}
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
  {:pre [(tailer? tailer)]}
  (-> tailer
      (:tailer-impl)
      (tail/underlying-tailer)
      (.close))
  tailer)

(defn close-queue!
  "Closes the given queue."
  [{q   :queue-impl
    :as queue}]
  {:pre [(queue? queue)]}
  (controllers/purge (queue-path queue))
  (queue/close! q)
  (System/runFinalization))

(defn to-index*
  [{t   :tailer-impl
    d   :dirty
    :as tailer} i]
  {:pre [(tailer? tailer)]}
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

(defmacro unhandled-error
  [msg & body]
  `(try
     ~@body
     (catch Exception e#
       (log/error e# (format "Unhandled error: %s" ~msg))
       (throw e#))))

(defmulti processor
  "Analogous to Kafka processors. No default method."
  (constantly nil))

(defmulti persistent-snapshot
  "Persists processor tailer indices to a backing queue. Called once
  before each processor step. Taken together, persistent-snapshot,
  persistent-attempt, and persistent-recover implement message
  delivery semantics in Cues processors."
  :strategy)

(defmulti persistent-snapshot-alts
  "Persists the alts processor tailer id to a backing queue. Called
  immediately after the processor alts read. Taken together,
  persistent-snapshot, persistent-attempt, and persistent-recover
  implement message delivery semantics in Cues processors."
  (fn [process _]
    (:strategy process)))

(defmulti persistent-attempt
  "Persists message to processor output queues, as well as attempt data
  to backing queue. Called once during each processor step. Taken
  together, persistent-snapshot, persistent-attempt, and
  persistent-recover implement message delivery semantics in Cues
  processors."
  (fn [process _]
    (:strategy process)))

(defmulti persistent-recover
  "Recovers processor tailer indices from backing queue once on
  processor start. Taken together, persistent-snapshot,
  persistent-attempt, and persistent-recover implement message
  delivery semantics in Cues processors."
  :strategy)

(defn- snapshot?
  [msg]
  (= (:q/type msg) :q.type/snapshot))

(defn- snapshot-alts?
  [msg]
  (= (:q/type msg) :q.type/alts))

(defn- snapshot-map
  [id tailer-indices]
  {:q/type           :q.type/snapshot
   :q/proc-id        id
   :q/tailer-indices tailer-indices})

(defn- snapshot-alts-map
  [tailer-id]
  {:q/type    :q.type/snapshot-alts
   :q/tailer-id tailer-id})

(defn- attempt-type
  [process]
  (if (= (:id (:errors process))
         (:id (:queue (:appender process))))
    :q.type/attempt-error
    :q.type/attempt-output))

(defn- attempt-map
  [process msg-index]
  {:q/type          (attempt-type process)
   :q/message-index msg-index})

(defn- attempt-nil-map
  [attempt-hash]
  {:q/type :q.type/attempt-nil
   :q/hash attempt-hash})

(defn- error-config
  [{:keys [config strategy] :as process}]
  (-> config
      (select-keys [:id :topics :types :in :alts :out
                    :errors :tailers :appenders])
      (assoc :strategy strategy)))

(defn- error-message
  [process msgs]
  {:q/type            :q.type.err/processor
   :err.proc/config   (error-config process)
   :err.proc/messages msgs})

(defmethod persistent-snapshot ::exactly-once
  [{uid     :uid
    try-a   :try-appender
    tailers :tailers
    retry   :retry
    :as     process}]
  {:pre [(appender? try-a)]}
  (let [m (->> tailers
               (map (juxt :id index))
               (into {})
               (snapshot-map uid))]
    (when-not retry (write try-a m))
    (assoc process :delivery-hash (hash m))))

(defmethod persistent-snapshot-alts ::exactly-once
  [{try-a :try-appender
    retry :retry} tailer]
  {:pre [(appender? try-a)]}
  (when-not retry
    (->> (:id tailer)
         (snapshot-alts-map)
         (write try-a))))

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
    h     :delivery-hash
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
                 (attempt-map process)
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

(defn- attempt-full
  [{a     :appender
    :as   process} msg]
  (let [f (-> process
              (wrap-attempt)
              (wrap-throwable)
              (wrap-write))]
    (f a msg)))

(defn- attempt-nil
  [{try-a :try-appender
    h     :delivery-hash}]
  {:pre [(appender? try-a)]}
  (->> h
       (attempt-nil-map)
       (write try-a)))

(defmethod persistent-attempt ::exactly-once
  [process msg]
  (err/wrap-error (error-message process msg)
    (if (and (:appender process) msg)
      (attempt-full process msg)
      (attempt-nil process))
    process))

(defn- recover-tailers
  [{process-id :id
    tailers    :tailers} snapshot]
  {:pre [(snapshot? snapshot)]}
  (doseq [{tid :id :as t} tailers]
    (if-let [i (-> snapshot
                   (:q/tailer-indices)
                   (get tid))]
      (to-index t i)
      (log/info "Topology changed, no snapshot for tailer" tid))))

(defn- recover-snapshot
  "Note that with respect to exactly-once delivery recover-tailers is
  idemopotent. We do not need to track if they have been reset we
  simply do it every time we detect the most recent delivery attempt
  has failed."
  [process {id :q/tailer-id} snapshot]
  (recover-tailers process snapshot)
  (assoc process
         :retry true
         :retry-tailer-id id))

(defn- next-snapshot
  "Reads back to the next snapshot on the try queue. Also return the
  last alts map if it is read along the way."
  [try-tailer]
  (loop [alts     nil
         snapshot nil]
    (when-let [msg (read try-tailer)]
      (if (snapshot? msg)
        [alts msg]
        (if (snapshot-alts? msg)
          (recur msg snapshot)
          (recur alts snapshot))))))

(defn- recover-attempt-output
  [try-tailer {{q :queue} :appender
               :as        process} {i :q/message-index}]
  (with-tailer [t q]
    (let [[alts snapshot] (next-snapshot try-tailer)]
      (when (not= (hash snapshot)
                  (-> t
                      (to-index i)
                      (read-with-hash)
                      (:q/meta)
                      (:q/hash)))
        (recover-snapshot process alts snapshot)))))

(defn- recover-attempt-error
  [try-tailer process msg]
  (let [p (->> (:errors process)
               (appender)
               (assoc process :appender))]
    (recover-attempt-output try-tailer p msg)))

(defn- recover-attempt-nil
  [try-tailer process {h :q/hash}]
  (let [[alts snapshot] (next-snapshot try-tailer)]
    (when (not= h (hash snapshot))
      (recover-snapshot process alts snapshot))))

(defn- recover-snapshot-alts
  [try-tailer process alts]
  (let [[_ snapshot] (next-snapshot try-tailer)]
    (recover-snapshot process alts snapshot)))

(defn- recover
  [try-tailer process msg]
  (case (:q/type msg)
    :q.type/snapshot       (recover-snapshot process nil msg)
    :q.type/snapshot-alts  (recover-snapshot-alts try-tailer process msg)
    :q.type/attempt-error  (recover-attempt-error try-tailer process msg)
    :q.type/attempt-output (recover-attempt-output try-tailer process msg)
    :q.type/attempt-nil    (recover-attempt-nil try-tailer process msg)))

(defmethod persistent-recover ::exactly-once
  [{{q :queue} :try-appender
    :as        process}]
  (unhandled-error "tailer recovery"
    (with-tailer [t q]
      (set-direction t :backward)
      (or (some->> t
                   (to-end)
                   (read-with-hash)
                   (recover t process))
          process))))

(defn- snapshot-unblock
  "Record tailer indices for unblock recovery."
  [{tailers :tailers
    :as     process}]
  (->> tailers
       (map (juxt identity index))
       (doall)
       (assoc process :snapshot-unblock)))

(defn- recover-unblock
  "Reset tailer positions and interrupts the processor."
  ([process]
   (recover-unblock process (constantly true)))
  ([{snapshot :snapshot-unblock} pred?]
   (doseq [[tailer i] snapshot]
     (when (pred? tailer)
       (to-index tailer i)))))

(defn- merge-deep
  [& args]
  (letfn [(f [& args]
            (if (every? map? args)
              (apply merge-with f args)
              (last args)))]
    (apply f (remove nil? args))))

(defn- merge-meta?
  [process]
  (get-in process [:queue-opts :queue-meta] true))

(defn- merge-meta-in
  [in]
  (->> (vals in)
       (keep :q/meta)
       (apply merge-deep)))

(defn- assoc-meta-out
  [out m]
  (reduce-kv
   (fn [out k v]
     (cond-> out
       v (assoc k (update v :q/meta merge-deep m))))
   {}
   out))

(defn- merge-meta
  [process in out]
  (when out
    (if (merge-meta? process)
      (if-let [m (merge-meta-in in)]
        (assoc-meta-out out m)
        out)
      out)))

(defn- default-result-fn
  [process result]
  (get result (-> process
                  (:appender)
                  (:queue)
                  (:id))))

(defn- default-run-fn
  [{f         :fn
    appender  :appender
    result-fn :result-fn
    :or       {result-fn default-result-fn}
    :as       process} in]
  (let [out (f process in)]
    (when appender
      (->> out
           (merge-meta process in)
           (result-fn process)))))

(defn- processor-run
  [{:keys [run-fn error-fn]
    :or   {run-fn default-run-fn}
    :as   process} in]
  {:pre [error-fn run-fn]}
  (try
    (->> in
         (run-fn process)
         (persistent-attempt process))
    (catch InterruptedException e (throw e))
    (catch Exception e
      (error-fn process e)
      process)))

(defn- zip-processor-read!!
  "Blocking read from all tailers into a map. Returns nil if any one of
  the blocking tailers returns nil."
  [{tailers :tailers}]
  (let [vals (map read!! tailers)
        keys (map (comp :id :queue) tailers)]
    (when (every? some? vals)
      (zipmap keys vals))))

(defn- alts-processor-tailers
  "On delivery retries, filters the list of tailers to the one that was
  previously read from."
  [{tailers  :tailers
    retry-id :retry-tailer-id}]
  (cond->> tailers
    retry-id (filter (comp #{retry-id} :id))))

(defn- alts-processor-read!!
  [process]
  (let [tailers (alts-processor-tailers process)]
    (when-let [[t msg] (alts!! tailers)]
      (persistent-snapshot-alts process t)
      {(:id (:queue t)) msg})))

(defn- processor-read
  [{{alts :alts} :config
    read-fn      :read-fn
    :as          process}]
  (unhandled-error "processor read"
    (let [f (cond
              read-fn read-fn
              alts    alts-processor-read!!
              :else   zip-processor-read!!)]
      (f process))))

(defn- reset-processor
  [process]
  (dissoc process :retry :retry-tailer-id))

(defn- processor-step
  [process]
  (let [p (unhandled-error "processor snapshot"
            (-> process
                (snapshot-unblock)
                (persistent-snapshot)))]
    (if-let [in (processor-read p)]
      (->> in
           (processor-run p)
           (reset-processor))
      (recover-unblock p))))

(defn log-processor-error
  [{{:keys [id in alts out]
     :or   {in  "source"
            out "sink"}} :config} e]
  (log/error e (format "%s (%s -> %s)" id (or alts in) out)))

(defn- get-error-fn
  [{q :errors}]
  (if-let [a (some-> q appender)]
    (fn [process e]
      (let [p (assoc process :appender a)]
        (log-processor-error process e)
        (->> (err/error e)
             (ex-data)
             (persistent-attempt p))))
    (fn [process e]
      (log-processor-error process e)
      (throw e))))

(defn- wrap-processor-error
  [handler]
  (fn [process msgs]
    (err/wrap-error (error-message process msgs)
      (handler process msgs))))

(defn- rename-keys
  [key-map m]
  (cond-> m
    key-map (set/rename-keys key-map)))

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

(defn- wrap-guard-no-out
  [handler]
  (fn [process msgs]
    (let [r (handler process msgs)]
      (when (-> process
                (:config)
                (:out))
        r))))

(defn- wrap-imperative
  [handler in-map out-map]
  (fn [{{a :appenders
         t :tailers} :imperative
        :as          process} msgs]
    (cond-> (dissoc process :tailers :appenders)
      t    (assoc :tailers (rename-keys in-map t))
      a    (assoc :appenders (rename-keys out-map a))
      true (handler msgs))))

(defn- wrap-select-processor
  [handler]
  (fn [process msgs]
    (-> process
        (select-keys [:id
                      :config
                      :tailers
                      :appenders
                      :errors
                      :strategy
                      :delivery-hash
                      :system
                      :opts])
        (handler msgs))))

(defn- topics-filter
  [values msgs]
  (reduce-kv
   (fn [m id {t :q/topics :as msg}]
     (if (and (map? t) (some t values))
       (assoc m id msg)
       m))
   nil
   msgs))

(defn- types-filter
  [values msgs]
  (->> msgs
       (filter #(some #{(:q/type (val %))} values))
       (into {})
       (not-empty)))

(defn- msg-filter-fn
  [{:keys [types topics]}]
  (cond
    topics (partial topics-filter topics)
    types  (partial types-filter types)
    :else  identity))

(defn- get-handler
  [{f  :fn
    id :id}]
  (or (get-method processor (or f id))
      (-> "Could not resolve processor :fn"
          (ex-info {:id id})
          (throw))))

(defn- get-processor-fn
  [{{:keys [in alts out tailers appenders]
     :as   config} :config}]
  (let [rev       (set/map-invert (or alts in))
        t         (set/map-invert tailers)
        a         (set/map-invert appenders)
        filter-fn (msg-filter-fn config)]
    (-> config
        (get-handler)
        (wrap-select-processor)
        (wrap-imperative t a)
        (wrap-guard-no-out)
        (wrap-msg-filter filter-fn)
        (wrap-msg-keys rev out)
        (wrap-processor-error))))

(defn- processor-handlers
  [process]
  (unhandled-error "building processor handlers"
    (-> process
        (assoc :error-fn (get-error-fn process))
        (assoc :fn (get-processor-fn process)))))

(defn- close-tailers!
  [process]
  (doseq [t (:tailers process)]
    (close-tailer! t))
  (doseq [t (-> process
                (:imperative)
                (:tailers)
                (vals))]
    (close-tailer! t)))

(defn- processor-loop
  [{:keys [id] :as process}]
  (try
    (loop [p (-> process
                 (processor-handlers)
                 (persistent-recover))]
      (when (continue? p)
        (recur (processor-step p))))
    (catch InterruptedException e (throw e))
    (catch Exception e
      (log/error "Processor: exit on unhandled error" id)
      (throw e))
    (finally
      (close-tailers! process))))

(defn- join-tailers
  [{:keys [uid queue/in]} unblock]
  (some->> in
           (util/seqify)
           (map #(tailer % uid unblock))
           (sort-by :id)
           (doall)))

(defn- fork-appenders
  [{:keys [queue/out]}]
  (some->> out
           (util/seqify)
           (map appender)
           (doall)))

(defn- backing-queue
  [{:keys [uid] :as process}]
  (let [p (-> process
              (:queue-opts)
              (:queue-path))]
    (queue uid {:transient  true
                :queue-path p})))

(defn- start-impl
  [process unblock start-fn]
  (let [q (backing-queue process)]
    {:try-queues [q]
     :futures    [(start-fn process unblock q)]}))

(defn- start-join
  "Reads messages from a seq of input tailers, applies a processor fn,
  and writes the result to a single output appender. While queues can
  be shared across threads, appenders and tailers cannot."
  [{:keys [queue/out]
    :as   process} unblock try-queue]
  (future
    (processor-loop
     (assoc process
            :unblock      unblock
            :tailers      (join-tailers process unblock)
            :appender     (appender out)
            :try-appender (appender try-queue)))))

(defn- fork-read!!
  [{[tailer] :tailers}]
  (read!! tailer))

(defn- fork-run-fn
  "Conditionally passes input message from one input queue, usually a
  backing queue, onto to the fork output queue, iff a message for the
  output queue exists in the map."
  [process in]
  (->> process
       (:queue/out)
       (:id)
       (get in)))

(defn- start-jf-join
  [process unblock fork-queue]
  (-> process
      (assoc :queue/out fork-queue
             :result-fn (fn [_ r] r))
      (start-impl unblock start-join)))

(defn- start-jf-fork
  "Always remove :errors queues: all exceptions must be unhandled."
  [{:keys [id uid queue/out]
    :as   process} unblock fork-queue]
  (doall
   (for [{oid :id :as o} out]
     (-> (dissoc process :errors)
         (assoc :id        (combined-id id oid)
                :uid       (combined-id uid oid)
                :run-fn    fork-run-fn
                :read-fn   fork-read!!
                :queue/in  fork-queue
                :queue/out o)
         (start-impl unblock start-join)))))

(defn- fork-id
  [process]
  (combined-id (:uid process) :fork))

(defn- start-join-fork
  "Many to many processors are modelled as a single join loop connected
  to one or more fork loops via a backing queue. Once a join loop has
  committed to the delivery of the output messages, each fork
  processing loop must deliver the message or raise an unhandled
  exception. This ensures that while not all messages will arrive at
  the same time, the delivery will be atomic."
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
  (-> (map (comp :id :queue) xs)
      (zipmap xs)
      (not-empty)))

(defn- imp-primitives
  [{uid :uid
    t   :queue/tailers
    a   :queue/appenders
    :as process} unblock]
  (let [p {:uid       (suffix-id uid "-i")
           :queue/in  t
           :queue/out a}]
    {:tailers   (zip (join-tailers p unblock))
     :appenders (zip (fork-appenders p))}))

(defn- start-imperative
  [{:keys [queue/out]
    :as   process} unblock try-queue]
  (future
    (processor-loop
     (assoc process
            :unblock      unblock
            :tailers      (join-tailers process unblock)
            :appender     (when out (appender out))
            :try-appender (appender try-queue)
            :imperative   (imp-primitives process unblock)))))

(defn- start-sink
  [process unblock try-queue]
  (future
    (processor-loop
     (assoc process
            :unblock      unblock
            :tailers      (join-tailers process unblock)
            :try-appender (appender try-queue)))))

(derive ::source     ::processor)
(derive ::sink       ::processor)
(derive ::join       ::processor)
(derive ::join-fork  ::processor)
(derive ::imperative ::processor)

(defn processor?
  [x]
  (isa? (:type x) ::processor))

(defmulti start-processor-impl
  :type)

(defmethod start-processor-impl ::source
  [{:keys [queue/out] :as process}]
  (->> (appender out)
       (assoc process :appender)))

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

(defn- ensure-done
  [{:keys [futures]}]
  (when futures
    (doseq [r futures]
      (try @r (catch Exception e)))))

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

(defn- get-one-q
  [g id]
  (get-in g [:queues id]))

(defn- get-q
  [g ids]
  (if (coll? ids)
    (map (partial get-one-q g) ids)
    (get-one-q g ids)))

(defn- build-config
  [{system   :system
    strategy :strategy
    opts     :queue-opts
    :or      {strategy ::exactly-once}} process]
  (cond-> process
    system   (assoc :system system)
    strategy (assoc :strategy strategy)
    opts     (assoc :queue-opts (::default opts))
    true     (assoc :state (atom nil))))

(defn- build-processor
  [{g-e :errors
    :as g} {id  :id
            in  :bind/in
            out :bind/out
            t   :bind/tailers
            a   :bind/appenders
            e   :errors
            :or {e g-e}
            :as process}]
  (cond-> (build-config g process)
    id  (assoc :uid (combined-id (:id g) id))
    e   (assoc :errors (get-q g e))
    in  (assoc :queue/in (get-q g in))
    out (assoc :queue/out (get-q g out))
    t   (assoc :queue/tailers (get-q g t))
    a   (assoc :queue/appenders (get-q g a))))

(defn- build-processors
  [g processors]
  (->> processors
       (map (partial build-processor g))
       (map (juxt :id identity))
       (update g :processors cutil/into-once)))

(defn- collect-processor-queues
  [{:keys      [errors]
    :bind/keys [in out appenders tailers]}]
  (concat (util/seqify in)
          (util/seqify out)
          (util/seqify appenders)
          (util/seqify tailers)
          (util/seqify errors)))

(defn- build-queue
  [{opts :queue-opts :as g} id]
  (when (and id (not (get-one-q g id)))
    (->> (get opts id)
         (merge (::default opts))
         (queue id))))

(defn- build-queues
  [{:keys [errors] :as g} processors]
  (->> processors
       (mapcat collect-processor-queues)
       (cons errors)
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

(defn- processor-edges
  [{id  :id
    in  :queue/in
    out :queue/out
    a   :queue/appenders
    t   :queue/tailers}]
  (cond-> {}
    in  (assoc (queue-ids in) id)
    out (assoc id (queue-ids out))
    t   (assoc (queue-ids t) id)
    a   (assoc id (queue-ids a))))

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
  [{p :processors :as g}]
  (-> g
      (dissoc :processors)
      (build-queues p)
      (build-processors p)
      (build-topology)))

(defn- parse-bindings
  [bindings]
  (let [b (vals bindings)]
    (case (count b)
      1 (first b)
      b)))

(defn- parse-processor
  [[t {:keys [id in out alts tailers appenders errors opts]
       :as   config}]]
  (cutil/some-entries
   (case t
     ::source {:id       id
               :type     t
               :bind/out id
               :opts     opts}
     {:id             id
      :type           t
      :bind/in        (parse-bindings (or alts in))
      :bind/out       (parse-bindings out)
      :bind/tailers   (parse-bindings tailers)
      :bind/appenders (parse-bindings appenders)
      :errors         errors
      :config         config
      :opts           opts})))

(defn- coerce-many-cardinality
  [[t form]]
  (case t
    :one  [form]
    :many form))

(defn- distinct-vals?
  [m]
  (apply distinct? (vals m)))

(s/def ::fn qualified-keyword?)
(s/def ::id qualified-keyword?)

(s/def ::id-one
  (s/and (s/map-of keyword ::id :count 1)
         distinct-vals?))

(s/def ::id-many
  (s/and (s/map-of keyword ::id :min-count 2)
         distinct-vals?))

(s/def :any/in
  (s*/non-conformer
   (s/or :one  ::id-one
         :many ::id-many)))

(s/def :one/in     ::id-one)
(s/def :one/out    ::id-one)
(s/def :any/out    :any/in)
(s/def :many/out   ::id-many)
(s/def :many/alts  ::id-many)
(s/def ::tailers   :any/in)
(s/def ::appenders :any/out)

(s/def ::types
  (s*/conform-to
    (s/or :one  ::id
          :many (s/coll-of ::id))
    coerce-many-cardinality))

(s/def ::topics ::types)

(s/def ::generic
  (s/keys :opt-un [::fn ::types ::topics]))

(s/def ::sink
  (s/merge (s/keys :req-un [(or :many/alts :any/in)])
           ::generic))

(s/def ::join
  (s/merge (s/keys :req-un [(or :many/alts :any/in) :one/out])
           ::generic))

(s/def ::join-fork
  (s/merge (s/keys :req-un [(or :many/alts :any/in) :many/out])
           ::generic))

(s/def ::imperative
  (s/merge (s/keys :opt-un [:one/out]
                   :req-un [(or :many/alts :any/in)
                            (or ::tailers ::appenders)])
           ::generic))

(s/def ::source
  (s/and (s/keys :req-un [::id])
         #(not (contains? % :in))
         #(not (contains? % :out))))

(s/def ::alts-or-in
  #(not (and (contains? % :alts)
             (contains? % :in))))

(s/def ::processor
  (s/and ::alts-or-in
         (s/or ::imperative ::imperative
               ::join-fork  ::join-fork
               ::join       ::join
               ::source     ::source
               ::sink       ::sink)))

(s/def ::processors
  (s/coll-of ::processor :kind sequential?))

(s/def ::graph
  (s/keys :req-un [::processors ::id]))

(defn graph
  [g]
  (-> (cutil/parse ::graph g)
      (update :processors (partial map parse-processor))
      (assoc :config g)
      (build-graph)))

(defn topology
  [graph]
  (:topology graph))

(defn isomorphic?
  [g1 g2]
  (= (topology g1)
     (topology g2)))

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
        (keep :errors))
   (->> (vals processors)
        (keep :fork-queue))
   (->> (vals processors)
        (mapcat :try-queues))
   (->> (vals queues))))

(defn close-graph!
  [graph]
  (stop-graph! graph)
  (->> graph
       (collect-graph-queues)
       (map close-queue!)
       (dorun))
  graph)

(defn send!
  ([g tx]
   (send! g nil tx))
  ([{p           :processors
     {s :source} :config
     :as         g} source msg]
   (if-let [s (get p (or source s))]
     (write (:appender s) msg)
     (throw (ex-info "Could not find source" {:id source})))))

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

;; Data file management

(defn delete-queue!
  "Deletes the queue data on disk, prompting by default.

  Implementation note: must also purge the queue controller or
  blocking will break the next time the queue is made."
  ([queue]
   (delete-queue! queue false))
  ([queue force]
   {:pre [(queue? queue)]}
   (let [p (queue-path queue)]
     (when (or force (cutil/prompt-delete! p))
       (close-queue! queue)
       (controllers/purge p)
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
     (controllers/purge queue-path)
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
