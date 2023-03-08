
# Messaging Semantics

This document covers the three strategies that exsist for message
delivery semantics:

1. At most once
2. At least once
3. Exactly once

For now Cues ignores _at most once_ and _at least once_ in favour of
providing _exactly once_ semantics for processors.

An _at most once_ persistence model does not provide any real benefits
versus an in memory, non-persistent model. Meanwhile the benefits of
_at least once_ semantics are strictly performance.

_Exactly once_, while much harder to implement and incurring a
marginal performance cost, let's you do everything the other two do,
and more.

## Exactly Once

This is different for each of the three types of processors:

1. Sources
2. Streams
3. Sinks

### Sources

For Sources, the guarantee is obviated by the fact that coordinating
other transactions with source commits falls within the scope of user
code.

### Streams

The following algorithm ensures exactly once semantics for stream
processors.

On each processor step:

1. Collect a set of indices for all input tailers and the processor id
   into a snapshot map

   ```clj
   {:q/type               :q.type.try/snapshot
    :q.try/tailer-indices {::t1 080928480 ::t2 080928481}}
   ```
2. Write this snapshot map to the backing queue

3. Perform reads on relevant tailers and run processor fn

5. Open a document transaction on the output queue

6. Get the index of the potential write, add this to the attempt
   map. Also add the hash of the snapshot map in the attempt map

   ```clj
   {:q/type              :q.type.try/attempt
    :q/hash              -499796012
    :q.try/message-index 080928480}
   ```

   If the processor output bindings are `nil`, in other words if there
   is _no message_ to be written for that processor step, then the
   type of the attempt should be changed to `:q.type.try/nil-attempt`.

7. Write the attempt map to a backing queue

8. Write the document adding the snapshot hash to the queue metadata:

   ```clj
   {:q/type :q.type/message
    :q/meta {:q/hash -499796012}}
   ```

8. Continue

On processor start:

1. Read back the most recent attempt and snapshot maps on the backing
   queue

   a) If there is _no attempt map_, and only a snapshot map, skip to 5b
   b) If there is _both an attempt map and a snapshot map_ then,
      depending on the attempt map type:
      i)  `:q.type.try/attempt`: proceed to step 2
      ii) `:q.type.try/nil-attempt`: get the hash of the input
          messages and the hash in the attempt map, and proceed to
          step 5. 

2. On the output queue, read back the message at the write index in
   the attempt map

3. Get the hash in the message metadata and the hash in the attempt
   map

5. Then:

   a) If _the two hashes match_, then attempt was successful:
      start processing from the tailers current positions
   b) Else, the attempt failed: Reset tailers to their recovery
      indices, start processing

### Sinks

Similar to Kafka's approach, the Cues streams algorithm described
above relies on transactionally collocating an index or a hash with
the target message in the target queue. This is easy enough on streams
processors where both the data model of message on the queues, and the
implementation of the store (ChronicleQueues instances) are known.

However, this does not generalize to sink processors. We do not know
either the implementation or the data model on the target store.

So just like Kafka, the Cues implementation guarantees _at least once_
semantics for sink processors, while also exposing a unique hash that
corresponds to the snapshot of the input messages. Then user code can
use this hash to ensure idempotency or _at most once_ semantics in the
sink processor.
