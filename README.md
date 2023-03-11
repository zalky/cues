<img src="https://i.imgur.com/GH71uSi.png" title="zalky" align="right" width="250"/>

# Cues

[![Clojars Project](https://img.shields.io/clojars/v/io.zalky/cues?labelColor=blue&color=green&style=flat-square&logo=clojure&logoColor=fff)](https://clojars.org/io.zalky/cues)

Queues on cue: low-latency persistent blocking queues, processors, and
graphs via ChronicleQueue.

For when distributed systems like Kafka are too much, durable-queue is
not enough, and both are too slow.

[ChronicleQueue](https://github.com/OpenHFT/Chronicle-Queue) is a
broker-less queue framework that provides microsecond latencies
(sometimes less) when persisting data to
disk. [Tape](https://github.com/mpenet/tape) is an excellent wrapper
around ChronicleQueue that exposes an idiomatic Clojure API, but does
not provide blocking or persistent tailers.

**Cues** extends both to provide:

1. Persistent _blocking_ queues, persistent tailers, and appenders
2. Processors for consuming and producing messages
3. Simple, declarative graphs for connecting processors together via
   queues
4. Brokerless, fault-tolerant _exactly-once_ graph delivery semantics
5. Message metadata
6. Microsecond latencies (sometimes even less)
7. Zero-configuration defaults
8. Not distributed

By themselves, the blocking queues are similar to what durable-queue
provides, just one or more orders of magnitude faster. They also come
with an API that aligns more closely with the persistence model: you
get queues, tailers, and appenders, and addressable, immutable indices
that you can traverse forwards and backwards.

The processors and graphs are meant to provide a dead-simple version
of the abstractions you get in a distributed messaging system like
Kafka. But there are no clusters to configure, no partitions to worry
about, and it is several orders of magnitude faster.

Ultimately the goals of Cues are fairly narrow: a minimal DSL for
connecting message processors into graphs using persistent queues in a
non-distributed environment.

## Use Cases

Cues could be used for:

- Robust, persistent, low-latency communication between threads or
  processes
- Anywhere you might use `clojure.core.async` but require a persistent
  model
- Prototyping or mocking up a distributed architecture

## Contents

The Cues API could be grouped into two categories: primitives and
graphs.

The low-level primitives are easy to get started with, but can be
tedious to work with once you start connecting systems together.

You could just as easily start with the higher-level graph
abstractions that hide most of the boiler-plate, but then you might
want to circle back at some point to understand the underlying
mechanics.

1. [Quick Start](#quick-start)
2. [Primitives: Queues, Tailers, and Appenders](#queues)
3. [Processors and Graphs](#processors-graphs)
   - [Processors](#processors)
   - [Graphs](#graphs)
   - [Topic and Type Filters](#filters)
   - [Topologies](#topologies)
   - [Errors](#errors)
4. [Queue Configuration](#configuration)
5. [Queue Metadata](#metadata)
6. [Utility Functions](#utilities)
7. [Java 11 & 17](#java)
8. [ChronicleQueue Analytics (disabled by default)](#analytics)

## Installation

Just add the following dependency in your `deps.edn`:

```clj
io.zalky/cues {:mvn/version "0.2.0"}
```

Also see the additional notes on running ChronicleQueue on [Java 11 &
17](#java).

## Quick Start <a name="quick-start"></a>

It is really easy to get started with queue primitives:

```clj
(require '[cues.queue :as q])

(def q (q/queue ::queue-id))
(def a (q/appender q))
(def t (q/tailer q))

(q/write a {:x 1})
;; =>
83313775607808

(q/read!! t)
;; =>
{:x 1}
```

But building an entire system is also straightforward:

```clj
(defmethod q/processor ::inc-x
  [process {msg :input}]
  {:output (update msg :x inc)})

(defmethod q/processor ::store-x
  [{{db :db} :opts} {msg :input}]
  (swap! db assoc (:x msg) msg)
  nil)

(defonce example-db
  (atom nil))

(defn example-graph
  [db]
  {:id         ::example
   :processors [{:id ::source}
                {:id  ::inc-x
                 :in  {:input ::source}
                 :out {:output ::tx}}
                {:id   ::store-x
                 :in   {:input ::tx}
                 :opts {:db db}}]})

(def g
  (-> (example-graph example-db)
      (q/graph)
      (q/start-graph!)))

(q/send! g ::source {:x 1})
(q/send! g ::source {:x 2})

@example-db
;; =>
{2 {:x 2}
 3 {:x 3}}

(q/all-graph-messages g)
;; =>
{::source ({:x 1} {:x 2})
 ::tx     ({:x 2} {:x 3})}
```

The rest of this document just covers these two APIs in more detail.

## Primitives: Queues, Tailers, and Appenders <a name="queues"></a>

Cues takes the excellent primitives offered by the
[Tape](https://github.com/mpenet/tape) library and extends them to
provide blocking and a couple of other features.

Queues couldn't be easier to create:

```clj
(require '[cues.queue :as q])

(def q (q/queue ::queue-id))
```

The `::queue-id` id uniquely identifies this queue throughout the
system and across restarts. Anywhere you call `(q/queue ::queue-id)`
it will return the same queue object.

You can then append messages to the queue with an appender (audible
gasp!):

```clj
(def a (q/appender q))

(q/write a {:x 1})
;; =>
83313775607808

(q/write a {:x 2})
;; =>
83313775607809
```

All messages are Clojure maps. Once the message has been written,
`cues.queue/write` returns its index on the queue. Both the index and
the message are immutable. There is no way to update a message once it
has been written to disk.

To read the message back you use a tailer:

```clj
(def t (q/tailer q))

(q/read t)
;; =>
{:x 1}
```

Tailers are stateful: each tailer tracks its position on the queue
that it is tailing. You can get the current _unread_ index of the
tailer:

```clj
(q/index t)
;; =>
83313775607809
```

When it consumes a message from the queue, the tailer advances to the
next index:

```clj
(q/read t)
;; =>
{:x 2}

(q/index t)
;; =>
83313775607810
```

This tailer is now one index ahead of the last message that we wrote
at `83313775607809`. Note that while indices are guaranteed to
increase monotonically, there is _no guarantee that they are
contiguous_. In general you should avoid code that tries to predict
future indices on the queue.

Since we have already read two messages there will be no message at
the tailer's current index. If we try another read with this tailer,
it will return `nil`, without advancing:

```clj
(q/read t)
;; =>
nil
```

This is because `cues.queue/read` is non-blocking.

We can use `cues.queue/read!!` to do a blocking read, which will block
the current thread until a message is available at the tailer's
index. Let's do this in another thread so that we can continue using
the REPL:

```clj
(def f (future
         (let [t (q/tailer q)]
           (while true
             (let [msg (q/read!! t)]
               ;; blocks until there is something to read
               (println "message:" (:x msg)))))))
;; prints
message: 1
message: 2
```

Notice that we created a new tailer in the future thread. This is
because unlike queues, tailers _cannot be shared across threads_. An
error will be thrown if you attempt to read with the same tailer in
more than one thread.

Also notice the loop immediately printed the first two messages on the
queue. This is because by default all new tailers will start at the
beginning of the queue (we'll see how to change this next). After
reading the first two messages, the tailer blocked on the third
iteration of the loop.

We can continue adding messages to the queue from the REPL:

```clj
(q/write a {:x 3})
;; =>
83313775607810
;; prints
message: 3

(q/write a {:x 4})
;; =>
83313775607811
;; prints
message: 4
```

And Voila! We are using ultra-low latency, persistent messaging to
communicate between threads. How fast is it?

```clj
(require '[criterium.core :as b])

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
```

Quite fast. Of course it will always depend to a large extent on the
size of the messages you are serializing.

Cues tailers have two other tricks up their sleeves:

1. **Persistence**: like queues, tailers can also be persistent. If
   you pass a tailer an id at creation, then that tailer's state will
   persist with that queue across runtimes:

   ```clj
   (q/tailer q ::tailer-id)
   ```

   Without an id, this tailer would restart from the beginning of the
   queue `q` every time you launch the application.

2. **Unblocking**: you can additionally pass a tailer an `unblock`
   atom. If the value of the atom is ever set to true, then the tailer
   will no longer block on reads:

   ```clj
   (let [unblock (atom nil)]
     (q/tailer q ::tailer-id unblock)
     ...)
   ```

   This would typically be used to unblock and dispose of blocked
   threads.

Finally there is another blocking read function, `cues.queue/alts!!`
that given a list of tailers, will complete at most _one_ read from
the first tailer that has a message available:

```clj
(q/alts!! [t1 t2 t3])
;; =>
{:x 1}
```

### Additional Queue Primitive Functions

Before moving on, let's cover a few more of the low level functions
for working directly with queues, tailers and appenders.

Instead of moving `:forward` along a queue, you can change the
direction of a tailer:

```clj
(q/set-direction t :backward)
```

You can move a tailer to either the start or end of a queue:

```clj
(q/to-start t)

(q/to-end t)
```

You can move a tailer to a _specific_ index:

```clj
(q/to-index t 83313775607810)
;; =>
true

(q/read t)
;; =>
{:x 3}
```

And you can get the last index written to a queue, either from the
queue or an associated appender:

```clj
(q/last-index q)
;; =>
83313775607811

(q/last-index a)   ; appender associated with q
;; =>
83313775607811
```

Recall that `cue.queue/index` gets the current _unread_ index of a
tailer. Instead you can get the last _read_ index for a tailer:

```clj
(q/index t)
;; =>
83313775607811

(q/last-read-index t)
;; =>
83313775607810
```

Finally, you can read a message from a tailer _without_ advancing the
tailer's position:

```clj
(q/index t)
;; =>
83313775607811

(q/peek t)
;; =>
{:x 4}

(q/index t)
;; =>
83313775607811
```

Finally, while tailers are very cheap, they do represent open
resources on the queue. You should close them once they're no longer
needed or it can add up over time.

To this end, you can either call `q/close-tailer!` directly on the
tailer, or:

```clj
(q/with-tailer [tailer queue]
  ...)
```
Here, if you provide a queue, you get a bound `tailer` that will be closed
when the block exits scope.

## Processors and Graphs <a name="processors-graphs"></a>

While the queue primitives are easy to use, they can be tedious to
work with when combining primitives into systems. To this end, Cues
provides two higher-level features:

1. Processor functions
2. A DSL to connect processors into graphs

### Processors <a name="processors"></a>

Processors are defined using the `cues.queue/processor` multimethod:

```clj
(defmethod q/processor ::inc-x
  [process {msg :input}]
  {:output (update msg :x inc)})

(defmethod q/processor ::store-x
  [{{db :db} :opts} {msg :input}]
  (swap! db assoc (:x msg) msg)
  nil)
```

Each processor method has two arguments. The first is a `process`
spec. This is a map of options and other system resources that the
processor may need to handle messages. We'll return to this in the
next section.

The other argument is a map of input messages. Each message has an
input binding, and there can be more than one message in the map. In
`::inc-x`, the input message is bound to `:input`.

Typically a processor will then take the input messages, and return
one or more output messages in an output binding map. Here,
`::inc-x` binds the output message to `:output`. In contrast,
you'll notice that `::store-x` returns `nil`, not a binding map. The
next section will explain why `::inc-x` and `::store-x` are
different in this respect.

Otherwise that's really all there is to processors.

### Graphs <a name="graphs"></a>

Cues provides a DSL that matches processor bindings to queues, and
connects everything together into a graph:

```clj
(defn example-graph
  [db]
  {:id         ::example
   :processors [{:id ::source}
                {:id  ::inc-x
                 :in  {:input ::source}
                 :out {:output ::tx}}
                {:id   ::store-x
                 :in   {:input ::tx}
                 :opts {:db db}}]})
```

This function returns a declarative graph spec. Every graph requires a
unique `:id`. The `:processors` attribute in the spec declares a
catalog of processors, each of which also requires a unique `:id`.

The processor `:id` is used to dispatch to the `q/processor` method,
but because each `:id` must be unique, you can instead use the `:fn`
attribute to set the `q/processor` method dispatch value for different
`:id`s.

```clj
{:id  ::unique-id-1
 :fn  ::inc-x
 ...}
{:id  ::unique-id-2
 :fn  ::inc-x
 ...}
```

The keys in the `:in` and `:out` maps are always _processor bindings_
and the values are always _queue ids_.

Taking `::inc-x` as an example:

```clj
{:id  ::inc-x
 :in  {:input ::source}
 :out {:output ::tx}}
```

The input messages from the `::source` queue are bound to `:input`,
and output messages bound to `:output` will be placed on the `::tx`
queue.

Similarly for `::store-x`, input messages from the `::tx` queue are
bound to `:input`:

```clj
{:id   ::store-x
 :in   {:input ::tx}
 :opts {:db db}}
```

However, notice that there are no output queues defined for
`::store-x`. Instead `::store-x` takes the input messages and
transacts them to the `db` provided via `:opts`:

```clj
(defmethod q/processor ::store-x
  [{{db :db} :opts} {msg :input}]
  (swap! db assoc (:x msg) msg)
  nil)
```

If you're familiar with Kafka, `::store-x` would be analogous to a
Sink Connector. Essentially this is an exit node for messages from the
graph. The return value in a sink processor fn is discarded, and so
`::store-x` does not bother with an output binding. A sink processor
does not actually have to return `nil`, but it is good practice.

You can also define processors similar to Kafka Source
Connectors. This would be the third processor in the catalog:

```clj
{:id ::source}
```

A processor with no `:in` or `:out` is considered a source. The `:id`
of the source processor is also the queue on which some external agent
will deposit messages.

However before we can send messages to the source, we need to
construct and start the graph:

```clj
(defonce example-db
  (atom nil))

(def g
  (-> (example-graph example-db)
      (q/graph)
      (q/start-graph!)))
```

Now we can `send!` our message:

```clj
(q/send! g ::source {:x 1})
```

You can simplify things for users of the graph by setting a default
source:

```clj
(defn example-graph
  [db]
  {:id         ::example
   :source     ::source
   :processors [{:id ::source}
                ...]})
```

Now a user of the graph can send messages without having to specify
the source:

```clj
(q/send! g {:x 1})
```

The message will move through the queues and the processors until it
is deposited in the `example-db` atom by the `::store-x` sink:

```clj
@example-db
;; =>
{2 {:x 2}}   ; :x was incremented by ::inc-x

;; get all the messages in the graph, each key is a queue
(q/all-graph-messages g)
;; =>
{::source ({:x 1})
 ::tx     ({:x 2})}
```

You can stop the graph using:

```clj
(q/stop-graph! g)
```

And close it:

```clj
(q/close-graph! g)
```

### Topic and Type Filters <a name="filters"></a>

Cues provides an additional layer of control over how your messages
pass through your graph: topic and type filters.

For any processor spec, you can specify either one or more topics, or
one or more types that the message must match for it to be handled by
the processor. Messages that do not match are skipped.

Here is a type filter:

```clj
{:id    ::store-x
 :types :q.type/doc
 :in    {:in ::tx}}
```

This processor will only handle messages whose `:q/type` attribute is
`:q.type/doc`:

```clj
;; yes
{:q/type :q.type/doc
 :attr   1
 :q/meta {...}}

;; no
{:q/type  :q.type/control
 :control :stop
 :q/meta  {...}}

;; no
{:x 1}
```

Similarly, we can define topic filters:

```clj
{:id     ::db.a.query/processor
 :topics #{:db.a/query :db.a/control}
 :in     {:in  ::source}
 :out    {:out ::tx}}
```

This processor will only handle messages whose `:q/topics` map
contains a truthy value for at least one of the topics:

```clj
;; yes
{:q/topics {:db.a/query [{:some [:domain :query]}]}
 :q/meta   {...}}

;; yes
{:q/topics {:db.a/control true}
 :control  :stop
 :other    "context"
 :q/meta   {...}}

;; no
{:q/topics {:db.a/write {:doc "doc"}
            :db.a/log   true}
 :q/meta   {...}}

;; no
{:x 1}
```

It is up to user code to decide whether the topic _value_ should
contain meaningful data, or whether the data is contained in the main
body of the message.

However, Cues _will remove from the message any topics that do not
match the filter_ before it is passed to the processor:

```clj
{:id     ::transactor
 :topics #{:db.a/write
           :db.b/write}
 :in     {:in  ::source}
 :out    {:out ::tx}}

;; message on ::source queue
{:q/topics {:db.a/write   [{:id 1 :doc "doc"}]
            :db.a/control :stop}
 :q/meta   {...}}

;; message seen by ::transactor
{:q/topics {:db.a/write [{:id 1 :doc "doc"}]}
 :q/meta   {...}}
```

The intent of this is two-fold:

1. Enforce a separation of concerns: if your processor is not
   subscribed to a topic, it should not know about that topic
2. Reduce the amount of unnecessary data that is written to queues

### Topologies <a name="topologies"></a>

Besides sources and sinks, there are other variations of processors.

Join processors take messages from multiple queues, blocking until all
queues have a message available:

```clj
{:id  ::join-processor
 :in  {:source  ::source
       :control ::control}
 :out {:output ::tx}}
```

There is an `alts!!` variation of a join processor that will take the
first message available from a set of queues.

```clj
{:id   ::join-processor
 :in   {:s1 ::crawler-old
        :s2 ::crawler-new
        :s3 ::crawler-experimental}
 :out  {:output ::tx}
 :opts {:alts true}}
```

Forks write messages to multiple queues:

```clj
{:id  ::fork-processor
 :in  {:input ::source}
 :out {:tx  ::tx
       :log ::log}}
```

And join-forks do both:

```clj
{:id  ::join-fork-processor
 :in  {:source  ::source
       :control ::control}
 :out {:tx  ::tx
       :log ::log}}
```

You do not need to specify the type of the processor, it will be
inferred from the `:in`, `:out` and the `:opts` maps.

We can inspect the topology our graph directly:

```clj
(q/topology g)
;; =>
{:nodes #{::store-x ::source ::inc-x}
 :deps  {:dependencies
         {::store-x #{::inc-x}
          ::inc-x   #{::source}}
         :dependents
         {::inc-x  #{::store-x}
          ::source #{::inc-x}}}}
```

And compute properties of the topology:

```clj
(require '[cues.deps :as deps])

(deps/transitive-dependencies (q/topology g) ::store-x)
;; =>
#{::source ::inc-x}
```

There's a number of topology/dependency functions that are available
in `cues.deps`. `cues.deps` basically provides the same functionality
that
[`com.stuartsierra/dependency`](https://github.com/stuartsierra/dependency)
does, but extends support for disconnected graphs.

There's also a couple of helper functions in `cues.util` for merging
and rebinding processor catalogs:

```clj
{:processors (-> (util/merge-catalogs cqrs/base-catalog
                                      features-a-catalog
                                      features-b-catalog)
                 (util/bind-catalog {::my-old-error-queue  ::new-error-queue
                                     ::features/tx-queue   ::new-tx-queue
                                     ::features/undo-queue ::new-undo-queue}))}
```

### Errors <a name="errors"></a>

Uncaught processor exceptions are logged via the excellent
[`com.taoensso/timbre`](https://github.com/ptaoussanis/timbre) library
for maximum extensibility.

However, you can also configure your graph to write uncaught
exceptions to an error queue:

```clj
(defn example-graph
  [db]
  {:id          ::example
   :source      ::source
   :error-queue ::error
   :processors  [{:id ::source}
                 ...]})
```

Now you can read processor exceptions from the `::error` queue:

```clj
(q/graph-messages g ::error)
;;=>
({:q/type            :q.type.err/processor
  :err.proc/config   {:id         :cues.build/processor
                      :in         :cues.build/source
                      :out        :cues.build/tx}
  :err.proc/messages {:cues.build/source {:x 1}}
  :err/cause         {:via   [{:type    clojure.lang.ExceptionInfo
                               :message "Something bad happened"
                               :data    {:data "data"}
                               :at      [cues.build$eval20160$fn__20162 invoke "build.clj" 11]}]
                      :trace [[cues.build$eval20160$fn__20162 invoke "build.clj" 11]
                              [cues.queue$wrap_guard_out$fn__10467 invoke "queue.clj" 1058]
                              [cues.queue$wrap_imperative$fn__10479 invoke "queue.clj" 1087]
                              ...
                              [java.util.concurrent.ThreadPoolExecutor$Workerrun "ThreadPoolExecutor.java" 628]
                              [java.lang.Thread run "Thread.java" 829]]
                      :cause "Something bad happened"
                      :data  {:data "data"}}})
```

By default cues will generate a generic error message like the one
above.

You can also add additional context to the error message with the
`cues.error/on-error` macro:

```clj
(defmethod q/processor ::inc-x
  [process {msg :input}]
  (err/on-error {:q/type       :q.type.err/my-error-type
                 :more-context context
                 :more-data    data}
    ...))
```

Here, the provided message map will be merged with the default error
context and placed on the configured `:error-queue`.

Of course, you can always catch and handle errors yourself, and place
them on any queue of your choice. Ultimately errors are just like any
other kind of data.

## Queue Configuration <a name="configuration"></a>

Whether used as primitives or as part of a graph, the following queue
properties are configurable:

1. Path of the queue data on disk
2. Message metadata
3. Queue data expiration and cycle handlers

For primitives these are passed as an options map to the queue
constructor:

```clj
(def q (q/queue ::queue-id {:queue-path "data/example"
                            :queue-meta #{:q/t :q/time}
                            :transient  true}))
```

The default path for all queues is `"data/queues"` in your project
root.

The same options can be passed to queues that participate in a graph
using the queue id:

```clj
(defn example-graph
  [db]
  {:id         ::example
   :queue-opts {::queue-id {:queue-path "data/example"
                            :queue-meta #{:q/t :q/time}
                            :transient  true}
                ::source   {...}
                ::tx       {...}}
   :processors [{:id ::source}
                {:id  ::inc-x
                 :in  {:input ::source}
                 :out {:output ::tx}}
                ...]})
```

For graphs, you can specify a set of default queue options. The
options for specific queues will be merged with the defaults:

```clj
(require '[cues.queue :as q])

(defn example-graph
  [db]
  {:id         ::example
   :queue-opts {::q/default {:queue-path "data/default-path"
                             :queue-meta #{:q/t}}
                ::source    {:queue-path "data/other"}      ; merge ::q/default ::source
                ::tx        {...}}                          ; merge ::q/default ::tx
   :processors [{:id ::source}
                {:id  ::inc-x
                 :in  {:input ::source}
                 :out {:output ::tx}}
                ...]})
```

The full set of options are:

1. **`:queue-path`**: The path on disk where the queue data files are
   stored. It is perfectly fine for multiple queues to share the same
   `:queue-path`.

2. **`:queue-meta`**: The default behaviour if this is **_not_** set
   is to leave the message unchanged when writing it to disk. However,
   if one or more metadata attributes are enabled via this setting,
   then the queue will ensure that those attributes are added to the
   message under a single `:q/meta` attribute.

   There are three configurable attributes, `#{:q/t :q/time :tx/t}`,
   which are discussed in the next section. But as an example, the
   following asserts that the `:q/t` attribute should be added for all
   queues, and that all three attributes should be added for the
   `::tx` queue:

   ```clj
   (defn example-graph
     [db]
     {:id         ::example
      :queue-opts {::q/default {:queue-path "data/example"
                                :queue-meta #{:q/t}}
                   ::tx        {:queue-meta #{:q/t :q/time :tx/t}}}
      :processors [{:id ::source}
                   {:id  ::inc-x
                    :in  {:input ::source}
                    :out {:output ::tx}}
                   ...]})
   ```

   You can also set `:queue-meta` to `false`, in which case that queue
   will actively _remove all metadata_ from any message before writing
   it to disk.

3. **`:transient`**: By default all messages written to disk persist
   forever. Setting `:transient true` will configure the roll-cycle of
   queue data files to be daily, and for data files to be deleted
   after 10 cycles (10 days). With `:transient true` you are only ever
   storing 10 days of data.

4. All the options supported by [Tape's underlying
   implementation](https://github.com/mpenet/tape/blob/615293e2d9eeaac36b5024f9ca1efc80169ac75c/src/qbits/tape/queue.clj#L26-L45)
   are also configurable. This includes direct control over the roll
   cycle and cycle handlers. For example, if the pre-defined
   `:transient` configuration is not suitable to your needs, you could
   use these settings to define new roll-cycle behaviour. See the
   doc-string in the link for details.

## Queue Metadata <a name="metadata"></a>

Queues can be configured to automatically add metadata to messages
when they are persisted.

There are three metadata attributes `:q/t`, `:q/time`, and
`:tx/t`. Each of these is added to the body of the message under the
root `:q/meta` attribute:

```clj
(first (q/graph-messages g ::tx))
;; =>
{:x      2
 :q/meta {:q/queue {::source {:q/t    83318070575104
                              :q/time #object[java.time.Instant 0x6805b000 "2023-02-11T22:58:26.650462Z"]}
                    ::tx     {:q/t    83318070575104
                              :q/time #object[java.time.Instant 0x40cfaf7b "2023-02-11T22:58:26.655232Z"]}}
          :tx/t    83318070575104}}
```

- **`:q/t`** is the index on the queue at which the message was
  written. This is a _per queue_ metadata attribute. If `:q/t` is
  enabled on multiple queues, the provenance of each message will
  accumulate as it passes from one queue to the next. You would
  typically use `:q/t` for audit or debugging purposes. In the above
  example, we see that this message happens to have the same index on
  both the `::source` and the `::tx` queues: `83318070575104`. This is
  not always going to be the case.

- **`:q/time`** is similar, except instead of an index, it collects
  the time instant at which the message was written to that queue. In
  the example above we can see that the message timestamps between the
  `::source` and the `::tx` queue are a few microseconds apart.

- **`:tx/t`** is identical to `:q/t` in one sense: it is also derived
  from the index on the queue at which the message was
  written. However unlike `:q/t` the _semantics_ of `:tx/t` are not
  relative to a single queue. Rather, `:tx/t` is meant to represent a
  global transaction t that can be used to achieve
  [serializability](https://en.wikipedia.org/wiki/Serializability) (in
  the transactional sense) of messages anywhere.

  Typically you would enable `:tx/t` on one queue, and use this as
  your source of truth throughout the rest of the graph. However
  achieving serializability also depends to a great extent on the
  topology of the graph, and the nature of the processors. Simply
  enabling `:tx/t` will not by itself be enough to ensure
  serializability. You need to understand how the properties of your
  graph and processors determine serializability.

## Utility Functions <a name="utilities"></a>

There some utility functions in `cues.queue` that are worth
mentioning.

If you already have a tailer, `q/messages` will return a lazy list of
messages:

```clj
(->> (q/messages tailer)
     (map do-something-to-message)
     ...)
```

There are also functions that will process messages eagerly:

```clj
;; get all message from queue object
(q/all-messages queue)

;; get all messages from a queue in a graph object
(q/graph-messages graph ::queue-id)

;; get all messages from a graph object
(q/all-graph-messages graph)
```

There are also a set of functions for managing queue files. By default
they will prompt you to confirm:

```clj
;; Close and delete the queue's data files
(q/delete-queue! queue)

;; Close and delete all queues in a graph
(q/delete-graph-queues! g)

;; Delete all queues in default queue path, or in the path
;; provided.
(q/delete-all-queues!)
(q/delete-all-queues "data/example")
```

## Java 11 & 17 <a name="java"></a>

ChronicleQueue [works under both Java 11 and
17](https://chronicle.software/chronicle-support-java-17/). However
some JVM options need to be set:

```clj
{:cues/j17 {:jvm-opts ["--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED"
                       "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
                       "--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED"
                       "--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED"
                       "--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED"
                       "--add-opens=java.base/java.lang=ALL-UNNAMED"
                       "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
                       "--add-opens=java.base/java.io=ALL-UNNAMED"
                       "--add-opens=java.base/java.util=ALL-UNNAMED"]}}
```

See the Cues
[`deps.edn`](https://github.com/zalky/cues/blob/main/deps.edn#L30-L38)
file for what this looks like as an alias.

## ChronicleQueue Analytics (disabled by default) <a name="analytics"></a>

ChronicleQueue is a great open source product, but it enables
[analytics
collection](https://github.com/OpenHFT/Chronicle-Map/blob/ea/DISCLAIMER.adoc)
by default (opt-out) to improve its product.

Cues changes this by **_removing and disabling_ analytics by
default**, making it opt-in.

If for some reason you want analytics enabled, you can add the
following into your `deps.edn` file:

```clj
net.openhft/chronicle-analytics {:mvn/version "2.24ea0"}
```

When enabled, the analytics engine will emit a message the first time
software is run, and generate a `~/.chronicle.analytics.client.id`
file in the user's home directory.

## Getting Help

First you probably want to check out the
[ChronicleQueue](https://github.com/OpenHFT/Chronicle-Queue)
documentation, as well as their
[FAQ](https://github.com/OpenHFT/Chronicle-Queue/blob/ea/docs/FAQ.adoc).

Otherwise you can either submit an issue here on Github, or tag me
(`@zalky`) with your question in the `#clojure` channel on the
[Clojurians](https://clojurians.slack.com) slack.

## License

Cues is distributed under the terms of the Apache License 2.0.
