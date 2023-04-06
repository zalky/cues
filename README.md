
# Cues

<img src="https://i.imgur.com/4eFmEDk.jpg" title="zalky" align="right" width="350"/>

[![Clojars Project](https://img.shields.io/clojars/v/io.zalky/cues?labelColor=blue&color=green&style=flat-square&logo=clojure&logoColor=fff)](https://clojars.org/io.zalky/cues)

Queues on cue: low-latency persistent blocking queues, processors, and
graphs via Chronicle Queue.

For when distributed systems like Kafka are too much, durable-queue is
not enough, and both are too slow.

[Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) is a
broker-less queue framework that provides microsecond latencies
(sometimes less) when persisting data to
disk. [Tape](https://github.com/mpenet/tape) is an excellent wrapper
around Chronicle Queue that exposes an idiomatic Clojure API, but does
not provide blocking or persistent tailers.

**Cues** extends both to provide:

1. Persistent _blocking_ queues, persistent tailers, and appenders
2. Processors for consuming and producing messages
3. Simple, declarative graphs for connecting processors together via
   queues
4. Brokerless, fault-tolerant _exactly-once_ message delivery
5. Message metadata
6. Microsecond latencies (sometimes even less)
7. Zero-configuration defaults
8. Not distributed

By themselves, the blocking queues are similar to what
[durable-queue](https://github.com/clj-commons/durable-queue)
provides, just one or more orders of magnitude faster. They also come
with an API that aligns more closely with the persistence model: you
get queues, tailers, and appenders, and addressable, immutable indices
that you can traverse forwards and backwards.

The processors and graphs are meant to provide a dead-simple version
of the abstractions you get in a distributed messaging system like
[Kafka](https://kafka.apache.org/). But there are no clusters to
configure, no partitions to worry about, and it is several orders of
magnitude faster.

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

To give you a sense of how it scales, from the Chronicle Queue
[FAQ](https://github.com/OpenHFT/Chronicle-Queue/blob/develop/docs/FAQ.adoc):

> Our largest Chronicle Queue client pulls in up to 100 TB into a
> single JVM using an earlier version of Chronicle Queue.

As for message limits:

> The limit is about 1 GB, as of Chronicle 4.x. The practical limit
> without tuning the configuration is about 16 MB. At this point you
> get significant inefficiencies, unless you increase the data
> allocation chunk size.

## Contents

The Cues API could be grouped into two categories: primitives and
graphs.

The low-level primitives are easy to get started with, but can be
tedious to work with once you start connecting systems together.

You could just as easily start with the higher-level graph
abstractions that hide most of the boiler-plate, but then you might
want to circle back at some point to understand the underlying
mechanics.

1.  [Installation](#installation)
2.  [Quick Start](#quick-start)
3.  [Primitives: Queues, Tailers, and Appenders](#queues)
4.  [Processors and Graphs](#processors-graphs)
    - [Processors](#processors)
    - [Graphs](#graphs)
    - [Topic and Type Filters](#filters)
    - [Topologies](#topologies)
5.  [Errors and Exactly Once Message Delivery](#errors)
6.  [Queue Configuration](#configuration)
7.  [Queue Metadata](#metadata)
8.  [Utility Functions](#utilities)
9.  [Runway: Developing Stateful Systems](#runway)
10. [Data Serialization](#serialization)
11. [Java 11 & 17](#java)
12. [Chronicle Queue Analytics (disabled by default)](#analytics)

## Installation <a name="installation"></a>

Just add the following dependency in your `deps.edn`:

```clj
io.zalky/cues {:mvn/version "0.2.1"}
```

If you do not already have SLF4J bindings loaded in your project,
SLF4J will print a warning and fall back on the no-operation (NOP)
bindings. To suppress the warning, simply include the nop bindings
explicitly in your deps:

```clj
org.slf4j/slf4j-nop {:mvn/version "2.0.6"}
```

Java compatibility: Chronicle Queue targets [LTS Java releases 8, 11,
and
17](https://github.com/OpenHFT/OpenHFT/blob/ea/docs/Java-Version-Support.adoc). See
the additional notes on running Chronicle Queue on [Java 11 &
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

But connecting queues into a system is also straightforward:

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
  "Connect processors together using ::source and ::tx queues."
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

;; Messages propagate asynchronously through the graph... then:

@example-db
;; =>
{2 {:x 2}
 3 {:x 3}}

;; Inspect the queues that particpate in the graph:
(q/all-graph-messages g)
;; =>
{::source ({:x 1} {:x 2})
 ::tx     ({:x 2} {:x 3})}
```

A similar example graph is defined in the
[`cues.build`](https://github.com/zalky/cues/blob/main/build/cues/build.clj)
namespace. To try it, clone this repo and run the following from the
project root:

```clj
clojure -X:server:repl
```

Or if you're on Java 11 or 17:

```clj
clojure -X:server:repl:cues/j17
```

Then connect your REPL and try the following:

```clj
user> (require '[cues.repl :as repl])
nil
user> (q/send! (repl/graph) {:x 1})
83464099463168
user> (q/all-graph-messages (repl/graph))
{:cues.build/error  (),
 :cues.build/source ({:x 1}),
 :cues.build/tx     ({:x 2, :q/meta {:tx/t 83464099463168}})}
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
it will return an object that references the same queue data.

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

We can continue adding messages to the queue using the appender:

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
   you pass a tailer an id at creation, then that tailer's current
   index will persist with that queue across runtimes:

   ```clj
   (q/tailer q ::tailer-id)
   ```

   Without an id, this tailer would restart from the beginning of the
   queue `q` every time you launch the application.

2. **Unblocking**: you can additionally pass a tailer an `unblock`
   atom. If the value of the atom is ever set to `true`, then the
   tailer will no longer block on reads:

   ```clj
   (let [unblock (atom nil)]
     (q/tailer q ::tailer-id unblock)
     ...)
   ```

   This would typically be used to unblock and dispose of blocked
   threads.

Finally there is another blocking read function, `cues.queue/alts!!`
that given a list of tailers, will complete at most _one_ read from
the first tailer that has a message available. `alts!!` returns a two
element tuple: the first element is the tailer that was read from and
the second element is the message that was read:

```clj
(q/alts!! [t1 t2 t3])
;; => t2 was the first with a message
[t2 {:x 1}]
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

You can also read a message from a tailer _without_ advancing the
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
tailer, or use a scoped tailer constructor:

```clj
(q/with-tailer [tailer queue]
  ...)
```
This gives you a new `tailer` on the given queue that will be closed
when the `with-tailer` block exits scope.

## Processors and Graphs <a name="processors-graphs"></a>

While the queue primitives are easy to use, they can be tedious to
work with when combining primitives into systems. To this end, Cues
provides two higher-level features:

1. Processor functions
2. A graph DSL to connect processors together via queues

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

Each processor method has two arguments. The first argument,
`process`, is a roll-up of the processor's execution context and all
the resources that the processor may need to handle messages. We'll
return to this in the following sections.

The other argument is a map of input messages. Each message is stored
at an input binding, and there can be more than one message in the
map. In the `::inc-x` processor, a single input message is bound to
`:input`.

Typically a processor will then take the input messages, and return
one or more output messages in an output binding map. Here,
`::inc-x` binds the output message to `:output`. In contrast,
you'll notice that `::store-x` returns `nil`, not a binding map. The
next section will explain why `::inc-x` and `::store-x` are
different in this respect.

Otherwise that's really all there is to processors.

### Graphs <a name="graphs"></a>

The Cues graph DSL matches processor bindings to queues, and connects
everything together into a graph:

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

Both the top level graph and every processor in the `:processors`
catalog requires a unique `:id`.

By default the processor `:id` is used to dispatch to the
`q/processor` method. However because each `:id` must be unique, you
can also use the `:fn` attribute to dispatch to a different method
from the `:id`:

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

Here the input messages from the `::source` queue are bound to
`:input`, and output messages bound to `:output` will be placed on the
`::tx` queue.

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
graph.

You can also define processors similar to Kafka Source
Connectors. This would be the third processor in the catalog:

```clj
{:id ::source}
```

A processor with no `:in` or `:out` is considered a source. The `:id`
is also the queue on which some external agent will deposit messages.

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

The message will then move through the queues and processors in the
graph until it is deposited in the `example-db` atom by the
`::store-x` sink:

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

You can stop all the graph processors using:

```clj
(q/stop-graph! g)
```

And close the graph and all its associated queues and tailers:

```clj
(q/close-graph! g)
```

Finally, we've already seen that we can use the `:opts` attribute to pass
options and resources to _specific_ processors. However, we can pass a
general systems map to _all_ processors in the graph via the `:system`
attribute:

```clj
(defn example-graph
  [db connection path]
  {:id         ::example
   :system     {:connection connection
                :path       path}
   :processors [{:id   ::processor
                 :in   {:input ::queue-in}
                 :out  {:output ::queue-out}
                 :opts {:db db}}
                ...]})

(defmethod q/processor ::processor
  [{{db :db}                  :opts
    {:keys [connection path]} :system} {msg :input}]
  ...)
```

### Topic and Type Filters <a name="filters"></a>

Cues provides an additional layer of control over how your messages
pass through your graph: topic and type filters.

Here is a processor with a type filter:

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
{:id     ::db.query/processor
 :topics #{:db.topic/query
           :db.topic/control}
 :in     {:in  ::source}
 :out    {:out ::tx}}
```

This processor will only handle messages whose `:q/topics` map
contains a truthy value for at least one of the topics:

```clj
;; yes
{:q/topics {:db.topic/query [{:some [:domain :query]}]}
 :q/meta   {...}}

;; yes
{:q/topics           {:db.topic/control true}
 :msg.control/signal :stop
 :msg/description    "context"
 :q/meta             {...}}

;; no
{:q/topics {:db.topic/write {:doc {:name "doc" :description "description"}
            :db.topic/log   true}
 :q/meta   {...}}

;; no
{:x 1}
```

The `:q/topic` values in messages can be arbitrary data.

Of course if you require some other kind of filtering besides what
type and topics provides, you can always pass in your own filters via
processor `:opts`, and perform filtering in the processor function
yourself:

```clj
{:id   ::processor
 :in   {:in  ::source}
 :out  {:out ::tx}
 :opts {:my-filter :category}}

(defmethod q/processor ::processor
  [{{:keys [my-filter]} :opts} {msg :in}]
  (when (predicate? my-filter msg)
    ...))
```

### Topologies <a name="topologies"></a>

There are several variations of processors besides sources and sinks.

Join processors take messages from multiple queues, blocking until
_all_ queues have a message available. They then write a single
message to one output queue:

```clj
{:id  ::join-processor
 :in  {:source  ::source
       :control ::control}
 :out {:output ::tx}}
```

There is an `alts!!` variation of a join processor that will take the
first message available from a set of input queues.

```clj
{:id   ::alts-processor
 :alts {:s1 ::crawler-old
        :s2 ::crawler-new
        :s3 ::crawler-experimental}
 :out  {:output ::tx}}
```

Fork processors write messages to multiple queues:

```clj
{:id  ::fork-processor
 :in  {:input ::source}
 :out {:tx  ::tx
       :log ::log}}
```

Message delivery on fork output queues is atomic, and exactly once
semantics _will_ apply. However, not all output messages will appear
on their respective output queues at the same time.

Join/forks or alts/forks both read from multiple input queues and
deliver to multiple output queues:

```clj
{:id  ::join-fork-processor
 :in  {:source  ::source
       :control ::control}
 :out {:tx  ::tx
       :log ::log}}
 
{:id  ::alt-fork-processor
 :alt {:source  ::source
       :control ::control}
 :out {:tx  ::tx
       :log ::log}}
```

You can conditionally write to different output queues based on which
bindings you return:

```clj
(defmethod q/processor ::join-fork-conditional
  [process msgs]
  (let [n   (transduce (map :x) + (vals msgs))
        msg {:x n}]
    {:even (when (even? n) msg)
     :odd  (when (odd? n) msg)}))
```

You can also dynamically compute input and output bindings based on
the processor definition, which is available in the first argument of
the processor under the `:config` attribute:

```clj
(defmethod q/processor ::broadcast
  ;; Broadcast to all the output bindings of the processor.
  [process in-msgs]
  (let [out-msgs     (compute-out-msgs in-msgs)
        out-bindings (keys (:out (:config process)))]
    (zipmap out-bindings (repeat out-msgs))))
```

Once you have connected a set of processors together into a graph, you
can inspect the topology of the graph directly:

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

There's a number of topology functions available in
[`cues.deps`](https://github.com/zalky/cues/blob/main/src/cues/deps.clj). This
namespace provides everything that
[`com.stuartsierra/dependency`](https://github.com/stuartsierra/dependency)
does, but extends functionality to support disconnected graphs.

There's also a couple of helper functions in `cues.util` for merging
and re-binding processor catalogs:

```clj
{:processors (-> (util/merge-catalogs cqrs/base-catalog
                                      features-a-catalog
                                      features-b-catalog)
                 (util/bind-catalog {::my-old-error-queue  ::new-error-queue
                                     ::features/tx-queue   ::new-tx-queue
                                     ::features/undo-queue ::new-undo-queue}))}
```

This makes graph definitions easier to reuse.

## Errors and Exactly Once Message Delivery <a name="errors"></a>

Cues provides persistent, brokerless, and fault tolerant _exactly
once_ message delivery for graph processors, but the approach depends
on whether a processor has side-effects or not.

For pure processors that do _not_ have side effects, _exactly once_
delivery semantics work out of the box. You do not have to do
anything.

For processors that _do_ have side-effects, like sink processors, Cues
provides _at least once_ delivery semantics on the processor, while
also exposing a special delivery hash to user code.

This delivery hash is unique to the set of input messages being
delivered, and is invariant across multiple delivery
attempts. Processors can then use the hash to implement _idempotency_
or _exactly once_ delivery semantics for their side effects:

```clj
(defmethod q/processor ::sink
  [{:keys [delivery-hash] :as process} msgs]
  ;; Use delivery-hash to enforce idempotency or exactly once delivery
  ;; on side-effects.
  ...)
```

For example, consider a sink processor that takes messages from
several input queues, combines them, and writes a result to a
database. If this fails at any point, the processor will attempt to
re-deliver these messages at least once more.

To ensure the result is written to the database exactly once, the user
code can collocate the delivery hash with the result in the db using
transactional semantics, checking that the transaction has not already
been written.

There are other approaches for ensuring exactly once delivery on
side-effects, but all of them would leverage the delivery hash.

### Code Changes

For processors with _no_ side-effects, exactly once delivery semantics
still hold even if the user code in the processor has changed between
failure and restart. The new code will be used to complete the
delivery.

For processors _with_ side-effects, this really depends on the changes
that are made to any user code that leverages the delivery hash.

However changes to the _topology_ of the graph (the connections
between processors) make exactly once delivery semantics inherently
ambiguous.

In such cases you might see the following log output on restart:

```
2023-03-17T19:34:52.085Z user INFO [cues.queue:734] - Topology changed, no snapshot for tailer :some-generated/id
```

This is not necessarily an error, just informing you that after the
topology change the input tailer has lost its recovery context.

### Message Delivery: Handled versus Unhandled Errors

By default, Cues does not handle any exceptions that bubble up from
the processor. Instead, they are treated as failed delivery attempts,
and the processor immediately quits.

On restart, exactly once delivery semantics will ensure that messages
are not lost and delivery is retried. However this is still not very
robust and you probably want to handle those exceptions.

While you can always handle errors directly in the processor code, you
can also configure Cues to handle exceptions for you. Just provide an
`:errors` queue to either the graph or an individual processor:

```clj
(defn graph
  [db]
  {:id         ::example
   :errors     ::graph-errors
   :processors [{:id ::source}
                {:id     ::inc-x
                 :in     {:input ::source}
                 :out    {:output ::tx}
                 :errors ::inc-x-errors}
                {:id   ::store-x
                 :in   {:input ::tx}
                 :opts {:db db}}]})
```

When configured in this way, any uncaught exceptions that are _not_ of
type `java.lang.InterruptedException` or `java.lang.Error` that bubble
up from the processor will be serialized and delivered to the error
queue.

Error messages are considered "delivered" according to exactly once
delivery semantics, and the processor will not retry. In other words:
a message will be delivered exactly once _either_ to an output queue,
_or_ to an error queue _once_, but never both.

You can then read back errors on the error queues:

```clj
(q/graph-messages g ::inc-x-errors)
;;=>
({:q/type            :q.type.err/processor
  :err.proc/config   {:id       ::inc-x
                      :in       {:input ::source}
                      :out      {:output ::tx}
                      :errors   ::inc-x-errors
                      :strategy ::q/exactly-once}
  :err.proc/messages {::source {:x 1}}
  :err/cause         {:via   [{:type    java.lang.Exception
                               :message "Oops"
                               :at      [cues.build$eval77208$fn__77210 invoke "build.clj" 11]}]
                      :trace
                      [[cues.build$eval77208$fn__77210 invoke "build.clj" 11]
                       [cues.queue$wrap_select_processor$fn__75200 invoke "queue.clj" 1061]
                       [cues.queue$wrap_imperative$fn__75194 invoke "queue.clj" 1046]
                       ...
                       [java.util.concurrent.ThreadPoolExecutor runWorker "ThreadPoolExecutor.java" 1128]
                       [java.util.concurrent.ThreadPoolExecutor$Worker run "ThreadPoolExecutor.java" 628]
                       [java.lang.Thread run "Thread.java" 829]]
                      :cause "Oops"}})
```

By default, Cues creates a generic error message like the one above,
which contains the processor's configuration, a stacktrace, and the
input message that triggered the error.

However, you can provide additional context to raised errors with the
`cues.error/wrap-error` macro:

```clj
(require '[cues.errors :as err])

(defmethod q/processor ::inc-x
  [process {msg :input}]
  (err/wrap-error {:q/type       :my-error-type
                   :more-context context
                   :more-data    data}
    ...))
```

The `wrap-error` macro merges the provided context into the raised
error, then re-throws it.

Of course, you can always catch and handle errors yourself, and place
them on arbitrary queues of your choice. Ultimately handled errors are
just like any other data in the graph.

### At Most Once Message Delivery

There are [generally three
strategies](https://medium.com/@madhur25/meaning-of-at-least-once-at-most-once-and-exactly-once-delivery-10e477fafe16)
that exist for message delivery in systems where failure is a
possibility:

1. At most once
2. At least once
3. Exactly once

Cues provides `::q/exactly-once` message delivery by default, but you
can optionally configure graphs to use `::q/at-most-once` delivery
semantics instead:

```clj
(require '[cues.queue :as q])

(defn example-graph
  [db]
  {:id         ::example
   :strategy   ::q/at-most-once
   :processors [{:id ::source}
                ...]})
```

With _at most once_ semantics any processor step is only ever
attempted once, and never retried. While failures may result in
_dropped messages_, this provides two modest benefits if that is not a
problem:

1. Approximately 30-40% faster graph performance
2. You can avoid implementing idempotency on side-effects using the
   delivery hash: processor steps are simply never retried

In contrast _at least once_ semantics pose no meaningful benefits with
respect to _exactly once_ delivery, and so outside of processors with
side-effects (where it is the default and [explained
previously](#errors)) that strategy is not provided.

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

2. **`:queue-meta`**: Normally messages are unchanged by the
   implementation when written to disk. However, setting this option
   can ensure three built-in metadata attributes are automatically
   added on write.
   
   The three attributes, `#{:q/t :q/time :tx/t}`, are discussed in the
   next section. But the following shows how the `:q/t` attribute
   could be configured for all queues, and all three attributes for
   the `::tx` queue:

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

You can always model, manage and propagate message metadata
yourself. However Cues provides some built-in metadata functionality
that should cover many use cases.

There are three metadata attributes `:q/t`, `:q/time`, and
`:tx/t`. Each of these is added to the body of the message under the
root `:q/meta` attribute:

```clj
(first (q/graph-messages g ::tx))
;; =>
{:x      2
 :q/meta {:q/queue {::source {:q/t    83318070575102
                              :q/time #object[java.time.Instant 0x6805b000 "2023-02-11T22:58:26.650462Z"]}
                    ::tx     {:q/t    83318070575104
                              :q/time #object[java.time.Instant 0x40cfaf7b "2023-02-11T22:58:26.655232Z"]}}
          :tx/t    83318070575104}}
```

- **`:q/t`** is the index on the queue at which the message was
  written. This is a _per queue_ metadata attribute. If `:q/t` is
  enabled on multiple queues, the provenance of each message will
  accumulate as it passes from one queue to the next. You would
  typically use `:q/t` for audit or debugging purposes.

- **`:q/time`** is similar, except instead of an index, it collects
  the time instant at which the message was written to that queue.

- **`:tx/t`** is pretty much identical to `:q/t`: it is also derived
  from the index on the queue at which the message was
  written. However unlike `:q/t` the _semantics_ of `:tx/t` are meant
  to be a _global_ transaction t that can be used to achieve
  [serializability](https://en.wikipedia.org/wiki/Serializability) (in
  the transactional sense) of messages anywhere in the graph.

  Typically you would enable `:tx/t` on one queue, and use this as
  your source of truth throughout the rest of the graph. However
  achieving serializability also depends to a great extent on the
  topology of the graph, and the nature of the processors. Simply
  enabling `:tx/t` will not by itself be enough to ensure
  serializability. You need to understand how the properties of your
  graph and processors determine serializability.

### Graph Metadata

Graph processors also have metadata semantics. Processors will
automatically propagate the merged `:q/meta` from all input messages
into any output messages on queues that have any metadata attributes
configured. This means that you do not need to explicitly propagate
`:q/meta` data in your processor functions.

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

There are also functions that will process messages eagerly from
queues. Be careful, these load _all_ the messages on the queue into
memory:

```clj
;; get all message from queue object
(q/all-messages queue)

;; get all messages from a queue in a graph object
(q/graph-messages graph ::queue-id)

;; get all messages from a graph object
(q/all-graph-messages graph)
```

There are also a set of functions for managing queue files. By default
data file deletion will prompt you to confirm:

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

## Runway: Developing Stateful Systems <a name="runway"></a>

Managing the lifecycle of any stateful application requires care,
especially in a live coding environment. With processor threads and
stateful resources running in the background, a Cues graph is no
different. And while you can certainly manage Cues graphs manually
from the REPL, it is easier to let a framework to manage your graph's
lifecycle for you.

This repository demonstrates how you might do this with
[Runway](https://github.com/zalky/runway), a
[`com.stuartsierra.component`](https://github.com/stuartsierra/component)
reloadable build library for managing the lifecycles of complex
applications.

See the [`deps.edn`](https://github.com/zalky/cues/blob/main/deps.edn)
file for how to configure Runway aliases, and the
[`cues.build`](https://github.com/zalky/cues/blob/main/build/cues/build.clj)
namespace for the `com.stuartsierra.component` example Cues graph.

## Data Serialization <a name="serialization"></a>

Data serialization is done via the excellent
[Nippy](https://github.com/ptaoussanis/nippy) library. It is very
fast, and you can extend support for custom types and records using
`nippy/extend-freeze` and `nippy/extend-thaw`. See the Nippy
documentation for more details.

## Java 11 & 17 <a name="java"></a>

Chronicle Queue [works under both Java 11 and
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

## Chronicle Queue Analytics (disabled by default) <a name="analytics"></a>

Chronicle Queue is a great open source product, but it enables
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
[Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue)
documentation, as well as their
[FAQ](https://github.com/OpenHFT/Chronicle-Queue/blob/ea/docs/FAQ.adoc).

Otherwise you can either submit an issue here on Github, or tag me
(`@zalky`) with your question in the `#clojure` channel on the
[Clojurians](https://clojurians.slack.com) slack.

## License

Cues is distributed under the terms of the Apache License 2.0.
