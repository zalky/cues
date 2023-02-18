
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
   into a recovery map

   ```clj
   {:q/type           :q.type.try/recover
    :q.try.proc/id    ::proc
    :q.try.tail/index {::t1 080928480 ::t2 080928481}}
   ```
2. Write this recovery map to the backing queue

3. Perform reads on relevant tailers and run processor fn

4. Create a hash of the recovery map, combine the hash and the
   recovery map into an attempt map

5. Open a document transaction on the output queue

6. Get the index of the potential write, add this to the attempt map

   ```clj
   {:q/type          :q.type.try/attempt
    :q.try/hash      -499796012
    :q.try.msg/index 080928480}
   ```

7. Write the attempt map to a backing queue

8. Try to write the document adding the hash to the queue metadata:

   ```clj
   {:q/type :q.type/message
    :q/meta {:q/hash 1}}
   ```

8. Continue

On processor start:

1. Read back the most recent attempt and recovery maps on the backing
   queue

   a) If there is _no attempt map_, and only a recovery map, skip to 5b
   b) If there is _both an attempt map and a recovery map_, continue
      with 2

2. On the output queue, read back the message at the write index in
   the attempt map

3. Read back the hash in the message metadata

5. Then:

   a) If _the two hashes match_, then attempt was successful:
      start processing
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

Instead, just like Kafka, the Cues implementation guarantees _at least
once_ semantics for sink processors, while also exposing a unique hash
that corresponds to each set of inputs. Then it is up to user code in
the sink processor to ensure idempotency, or _at most once_ semantics
using this unique hash.
