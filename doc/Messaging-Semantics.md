
# Messaging Semantics

There are generally three strategies that exsist for graph processor
message delivery:

1. At most once
2. At least once
3. Exactly once

Currently Cues only implements _exactly once_ semantics for graph
processors. This is significantly harder than the other two, but more
useful overall. The main advantage of the other two is performance,
but not by a large margin.

Nevertheless, _at most once_ and _at least once_ are next in the
feature backlog and will be included in a future release.

## Message Delivery

Input messages for a processor step are considered delivered if either
an output message is successfully written to an output queue, or an
exception is caught and handled. A _handled_ exception means that it
is:

1. Logged
2. Written to an `:error-queue` if the queue has been configured

An _unhandled_ error will result in a processor interrupt, and
undelivered messages. Once the processor restarts, exactly once
semantics will ensure that the messages are not lost and the delivery
will be retried. It also ensures that once the messages are delivered,
they will not be delivered again.

## Exactly Once Algorithm

The exactly once algorithm is different depending on whether the
processor function has side effects or not. Here, "processor function"
refers to the user code defined by `cues.queue/processor`. Obviously
the Cues implementation will have side effects in order to persist
messages to queues.

### Processors Without Side Effects

The algorithm described below is specifically for a "join" processor,
which accepts multiple input messages and produces a single output
message. However every other kind of processor can generalize from
this one case.

A transient "backing queue" is needed to implement the algorithm. This
queue is automatically created and isolated from the user. A user
specified error queue can also be configured for delivery of _handled_
error messages.

The algorithm can be broken down into two components.

1. A persistence component that runs on every processor step
2. A recovery component that runs one time on processor start

Each of the components can safely fail at any point. The recovery
component is idempotent. The persistence component is not: if it
fails, the processor must exit and recovery must be run.

#### The persistence component: runs on every step

1. Collect the current index for all input tailers as well as the
   processor id into a snapshot map:

   ```clj
   {:q/type           :q.type/snapshot
    :q/proc-id        :cues.build/example-processor
    :q/tailer-indices {::t1 080928480
                       ::t2 080928481}}
   ```

2. _If and only if this is the first attempt to deliver these
   messages_, then write this snapshot map to a backing queue.

3. Compute a hash of the snapshot map. This is the delivery hash. The
   value is unique to the combination of indices and processor id, and
   will evaluate to be the same for each delivery attempt. Because the
   messages on the input queues are immutable, their indices and the
   processor id is all you need to compute the hash. You do not need
   the actual input messages.

4. Now read one or more messages from the input tailers, advancing the
   relevant tailers. _If this is an alts processor and we are retrying
   a delivery attempt_, make sure to read from the same tailer that
   was used for the initial attempt regardless of which tailers may
   now have messages (4a of the persistence component and 1b of the
   recovery component helps you do this).

   a) _Immediately after the read, and if this is an alts processor,
      and if this is the first attempt at delivery_, create and alts
      snapshot map that contains the id of the alts tailer that was
      just read from. Because the availability of messages on an alts
      read is non-deterministic, we need to store which tailer was
      initially read from for subsequent alts attempts.

      ```clj
      {:q/type      :q.type/snapshot-alts
       :q/tailer-id :some-tailer-id}
      ```

      Write this map to the backing queue.

5. Run the processor function, which may or may not return an output
   message to be written to the output queue.

   a) If there is an output message, proceed to step 7.

   b) If there is _no_ output message, proceed to step 8.

6. If at any point from steps 3 onward an exception is caught and
   _handled_:

   a) Proceed to step 7 if the `:error-queue` has been configured to
      write an error message.

   b) Proceed to step 8 if _no_ `:error-queue` has been configured,
      and no error message should be written.

7. Open a transaction to write the output message to the output queue
   or an error message to the error queue:

   a) Add the delivery hash to the metadata of the output or error
      message:

      ```clj
      {:q/type :q.type/some-output-message-type
       :q/meta {:q/hash -499796012 ....}
       ...}
      ```

   b) Create an attempt map. The type of the attempt map should depend
      on whether the message is an output or error message. For
      example:

      - Output attempt: `:q.type/attempt-output`
      - Error attempt: `:q.type/attempt-error`

   c) Get the index of the provisional write on the output or error
      queue, and add this message index to the attempt map:

      ```clj
      {:q/type          :q.type/attempt-output
       :q/message-index 080928480}
      ```

   d) Write the attempt map to the backing queue.

   e) Complete the output or error transaction. The messages are
   considered delivered.

8. If there is no output or error message to be written (skip this if
   you have already done 7):

   a) Create an attempt map of type `:q.type/attempt-nil`.

   b) Add the delivery hash to the attempt map:

      ```clj
      {:q/type :q.type/attempt-nil
       :q/hash -499796012}
      ```

   c) Write the attempt map to the backing queue. The messages are
   considered delivered.

9. Continue processing the next set of messages. If at any point
   during steps 1-8 an _unhandled_ error occurs, the processor should
   immediately interrupt and exit.

#### The recovery component: runs once on start

1. Read the most recent message on the backing queue. Depending on the
   message type:

   a) No message on the backing queue: the processor has never made an
      attempt, proceed to 4.

   b) `:q.type/snapshot`: The previous attempt failed. Configure the
      processor to skip making new snapshots until a delivery attempt
      is successful. Reset the tailers to the indices in the snapshot
      and proceed to 4.

   c) `:q.type/snapshot-alts`: The previous attempt failed. Configure
      the alts processor with the tailer id in the message: all
      subsequent alts retries should read from this tailer. Continue
      reading backwards on the backing queue until you reach the first
      snapshot. Proceed to 1b.

   d) `:q.type/attempt-output`: take the `:q/message-index` from the
      attempt map and read back the message at this index on the
      _output queue_. Get the delivery hash from the output message
      metadata. Proceed to 2.

   e) `:q.type/attempt-error`: take the `:q/message-index` from the
      attempt map and read back the message at this index on the
      _error queue_. Get the delivery hash from the error message
      metadata. Proceed to 2.

   f) `:q.type/attempt-nil`: get the delivery hash directly from the
      attempt map. Proceed to 2.

2. Read backwards from the most recent message on the backing queue
   until you reach the first snapshot. Compute the hash of the
   snapshot.

3. Then:

   a) If the delivery hash from step 1 and the snapshot hash from step
      2 _are the same_: then the last delivery attempt was successful.
      Proceed to 4.

   b) Else, they are _not the same_: the last delivery attempt
      failed. Reset tailers to the recovery indices in the snapshot
      message. Then proceed to 4.

4. Continue processing messages.

### Processors With Side Effects

The exactly once algorithm described above relies on transactionally
collocating the delivery hash with the output message on the output
queue (or the error message on the error queue).

This is easy enough when there are no side effects and the data model
of the messages and the implementation of the queues are both
known. However, when processors perform side-effects on arbitrary
external resources, the algorithm fails to generalize.

For these processors, Cues takes an approach similar to Kafka: it
provides _at least once_ messaging semantics on the processor, but
then exposes the delivery hash to user code so that it can use the
hash to implement _idempotency_ or _exactly once_ delivery semantics
for the side effects:

```clj
(defmethod q/processor ::sink
  [{:keys [delivery-hash] :as process} msgs]
  ;; Use delivery-hash to enable idempotence or enforce
  ;; exactly once semantics
  ...)
```

For example, consider a sink processor that takes messages from some
input queues, combines them, and writes a result to a database. After
an _unhandled_ error, interrupt, and restart, the processor will
attempt to re-deliver these messages at least once. However on each
attempt, the delivery hash will be the same.

Therefore when writing the result to the database, the user code can
collocate the hash with the result in the db to ensure the result is
only written once. This usually requires some form of transactional
semantics on the target db, or alternatively, some kind of
single-writer configuration.
