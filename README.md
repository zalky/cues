<img src="https://i.imgur.com/GH71uSi.png" title="zalky" align="right" width="250"/>

# Cues

[![Clojars Project](https://img.shields.io/clojars/v/io.zalky/cues?labelColor=blue&color=green&style=flat-square&logo=clojure&logoColor=fff)](https://clojars.org/io.zalky/cues)

Queues on cue: persistent blocking queues, processors, and topologies
via ChronicleQueue.

For when distributed systems like Kafka are too much, durable-queue is
not enough, and both are too slow.

ChronicleQueue is a broker-less queue framework that provides
microsecond latencies when persisting data to disk. The Cues
implementation extends ChronicleQueue, and another Clojure wrapper
called [Tape](https://github.com/mpenet/tape), to provide:

1. Persistent blocking queues, tailers, and appenders
2. Processors for consuming and producing messages
3. Topologies for connecting processors together via queues

By themselves, the blocking queues are similar to what durable-queue
provides, just one or more orders magnitude faster. They also come
with an API that aligns more closely with the persistence model: you
get queues, tailers, and appenders, and addressable, immutable indices
that you can traverse forwards and backwards.

The processors and topologies are meant to provide a dead-simple
version of the abstractions you get in a distributed messaging system
like Kafka. But there are no clusters to configure, no partitions to
worry about, and it is several orders of magnitude faster. Ultimately
it's goals are fairly narrow: a simple DSL for connecting message
processors into graphs using persistent queues.

## Contents

There are two ways to use Cues:

1. [Queues, Tailers, and Appenders](#queues): low-level primitives
   that are easy to get started with

2. [Processors and Topologies](#processors-topologies): higher-level abstractions
   that hide most of the boiler-plate

## Getting Started

Just add the following dependency in your `deps.edn`:

```clj
io.zalky/cues {:mvn/version "0.2.0"}
```

## Queues, Tailers, and Appenders <a name="queues"></a>

Queues couldn't be easier to create and use:

```clj
(require '[cues.queue :as q])

(def q (q/queue ::queue))
```

The `::queue` id uniquely identifies this queue throughout the system
and across restarts. Anywhere you call `(q/queue ::queue)` it will
return the same queue object.

You can then write messages to the queue with an appender:

```clj
(def a (q/appender q))

(q/write a {:x 1})
;; => 
83305185673216
```

All messages are Clojure maps and are stored at a specific index on
the queue. `cues.queue/write` returns the index where the message was
written. Both the index and the message are immutable. There is no way
to update a message once it has been written to disk.

To read the message back you use a tailer:

```clj
(def t (q/tailer q))

(q/read t)
;; =>
{:x      1
 :q/meta {:q/queue {::queue {:q/time #object[java.time.Instant 0x4179c7f2 "2023-02-08T20:15:22.078240Z"]
                             :q/t    83305185673216}}}}
```

Here we see that some metadata was added to the message. The metadata
is for audit purposes only, and it does not affect how the message
moves through the system.

The other thing about the tailer is that it is stateful: each tailer
tracks its position on the queue that it is tailing. When it consumes
a message from the queue, the tailer advances to the next index on the
queue.

Let's check the current _unread_ position of the tailer:

```clj
(q/index t)
;; =>
83305185673217
```

This tailer is one index ahead of the message that we wrote at
`83305185673216`. Be careful: while indices are guaranteed to increase
monotonically, there is _no guarantee that they are contiguous_.

There is only one message in the queue, and so there is no message at
the tailer's current index. If we try another read with this tailer,
it will return `nil`, without advancing:

```clj
(q/read t)
nil
```

This is because `cues.queue/read` is non-blocking.

We can use `cues.queue/read!!` to do a blocking read, which will wait
until a message is available at the current index. Let's do this in
another thread so that we can continue using the REPL.

```clj
(def f (future
        (let [t (q/tailer q)]
          (while true
            (let [msg (q/read!! t)]
               ;; blocks until there is something to read
               (println "message:" (:x msg)))))))
;; prints
message: 1
```

Notice that we created a new tailer in the future thread. This is
because unlike queues, tailers _cannot be shared across threads_. An
error will be thrown if you attempt to read with the same tailer in
more than one thread.

Also notice the loop immediately printed the first message at queue
index `83305185673216`. This is because by default all new tailers
will start at the beginning of the queue. After reading the first
message, the tailer blocked on the second iteration of the loop.

We can continue adding messages to the queue from the REPL:

```clj
(q/write a {:x 2})
;; =>
83305185673217
;; prints
message: 2

(q/write a {:x 3})
;; =>
83305185673218
;; prints
message: 3
```

Voila, we are already using ultra-low latency persistent messaging
between threads.

Cues tailers have two other tricks up their sleeves:

1. **Persistence across runtimes**: if you pass a tailer an id at
   creation time, then that tailer's state will persist with that
   queue across runtimes:
   
   ```clj
   (q/tailer q ::tailer-id)
   ```
   
   Without an id, this tailer would restart from the beginning of the
   queue every time you launch the application.

2. **Unblocking**: you can additionally pass a tailer an `unblock`
   atom. If the value of the atom is ever set to true, then the tailer
   will no longer block on reads:

   ```clj
   (let [unblock (atom nil)]
     (q/tailer q ::tailer-id unblock)
     ...)
   ```

   This would typically be used to unblock and clean up blocked
   threads.

Finally there is another blocking read function, `cues.queue/alts!!`
that given a list of tailers, will complete at most _one_ read from
the first tailer that has a message available:

```clj
(q/alts!! [t1 t2 t3])
;; =>
{:x      1
 :q/meta {:q/queue {::queue {:q/time #object[java.time.Instant 0x4179c7f2 "2023-02-08T20:15:22.078240Z"]
                             :q/t    83305185673216}}}}
```

#### Additional Queue Primitive Functions

Before moving on, let's cover a few more of the low level functions
for working directly with queues, tailers and appenders.

Instead of moving `:forward` along a queue, you can change the
direction of a tailer using:

```clj
(q/set-direction t :backward)
```

You can move a tailer to either the start or end of a queue using:

```clj
(q/to-start t)

(q/to-end t)
```

You can move a tailer to a _specific_ index using:

```clj
(q/to-index t 83305185673218)
;; =>
true

(q/read t)
;; =>
{:x      3
 :q/meta {:q/queue {::queue {:q/time #object[java.time.Instant 0x4d1dd7f3 "2023-02-08T20:54:49.887653Z"]
                             :q/t    83305185673218}}}}
```

And you can get the last index written to a queue, either from the
queue or an associated appender:

```clj
(q/last-index q)
;; =>
83305185673218

(q/last-index a)   ; appender associated with q
;; =>
83305185673218
```

Recall that `cue.queue/index` gets the current _unread_ index of a
tailer. Instead you can get the last _read_ index for a tailer:

```clj
(q/index t)
;; => 83305185673217

(q/last-read-index t)
;; => 83305185673216
```

Finally, you can peek to read a message from a tailer, without
advancing the tailer's position:

```clj
(q/to-index t 83305185673218)

(q/index t)
;; =>
83305185673218

(q/peek t)
;; =>
{:x      3
 :q/meta {:q/queue {::queue {:q/time #object[java.time.Instant 0x4d1dd7f3 "2023-02-08T20:54:49.887653Z"]
                             :q/t    83305185673218}}}}

(q/index t)
;; =>
83305185673218
```

## Processors and Topologies <a name="processors-topologies"></a>

While the queue primitives are easy to use, they can be tedious to
work with when putting together larger systems. To this end, Cues
provides processor functions, and a simple DSL to connect processors
into topologies.

### Processors

You can create processors using the `q/processor` multimethod:

```clj
(defmethod q/processor ::processor
  [process {msg :in}]
  {:out (update msg :x inc)})

(defmethod q/processor ::doc-store
  [{{db :db} :opts} {msg :in}]
  (swap! db assoc (:x msg) (dissoc msg :q/meta)))
```

Each processor method has two arguments. The first is a `process`
spec. This is a map of options and other system resources that the
processor may need to handle messages. We'll come back to this in the
Topologies section.

The other argument is a map of input messages. Each message is
retrieved from an input binding. In this example, the binding is
`:in`.

The processor then takes those input messages, and returns one or more
output messages in a binding map. Here, the binding for out output
message is `:out`.

The job of the topologies is then to match processor bindings to
queues. It's that simple.

### Topologies

Consider the following:

```clj
(defn example-graph
  [db]
  {:queue-path "data/example"
   :processors [{:id ::source}
                {:id  ::processor
                 :in  {::source :in}
                 :out {::tx :out}}
                {:id   ::doc-store
                 :in   {::tx :in}
                 :opts {:db db}}]})
```

This function returns a declarative graph spec. The `:processors`
attribute in the spec declares a catalog of processors each of which
has a unique `:id`.

The `:id` will usually correspond to a `q/processor` dispatch
value. However, in the case of a conflict, you can use the `:fn`
attribute to specify the `q/processor` dispatch value:

```clj
{:processors [...
              {:id  ::my-processor-a
               :fn  ::processor
               ...}
              {:id  ::my-processor-b
               :fn  ::processor
               ...}
              ...
              ]}
```

Anyways, you'll notice that the processors have input and output
bindings:

```clj
{:id  ::processor
 :in  {::source :in}
 :out {::tx :out}}
{:id   ::doc-store
 :in   {::tx :in}
 :opts {:db db}}
```

The keys in the `:in` and `:out` maps are always _queue ids_ and the
values are always processor _input and output bindings_.

For `::processor`, this means that input messages from the `::source`
queue are bound to `:in`, and that any output messages bound to `:out`
will be placed on the `::tx` queue.

Similarly for `::doc-store`, input messages from the `::tx` queue are
bound to `:in`. However, there are no output queues defined
`::doc-store`. Instead, the processor takes the input messages and
transacts them to a db:

```clj
(defmethod q/processor ::doc-store
  [{{db :db} :opts} {msg :in}]
  (swap! db assoc (:x msg) (dissoc msg :q/meta)))
```

Notice that the `db` is accessed through the `:opts` in the `process`
spec. This is the same `:opts` map that we saw in the processor
catalog:

```clj
{:id   ::doc-store
 :in   {::tx :in}
 :opts {:db db}}      ; <- :opts map
```

If you're familiar with Kafka, `::doc-store` would be analogous to a
Sink Connector. Essentially this is an exit node from the graph.

You can also define processors similar to Kafka Source Connectors. We
have ignored this processor in our example until now:

```clj
{:id ::source}
```

A processor with no `:in` or `:out` is considered a source. For source
processors the `:id` is taken as the queue on which some outside agent
will deposit messages.

For example, you could manually place a message on the `::source`
queue like so:

```clj
(q/send! graph ::source {:x 1})
```

However there's one last step before this will work. We need to build
and start the topology:

```clj
(defonce example-db
  (atom nil))

(def g
  (-> (example-graph example-db)
      (q/graph)
      (q/start-graph!)))
```

Now after we `send!` our message:

```clj
(q/send! g ::source {:x 1})
```

The message will traverse the topology and we should be able to see
the new data in the `example-db`:

```clj
@example-db
;; =>
{2 {:x 2}}
```

We can retrieve a dependency graph for our topology like so:

```clj
(q/topology g)
;; =>
{:nodes #{::doc-store ::source ::processor}
 :deps  {:dependencies
         {::doc-store #{::processor}
          ::processor #{::source}}
         :dependents
         {::processor #{::doc-store}
          ::source    #{::processor}}}}
```

And compute properties on the topology:

```clj
(require '[cues.deps :as deps])

(deps/transitive-dependencies (q/topology g) ::doc-store)
;; =>
#{::source ::processor}
```
