.PHONY: test

version-number  = 0.2.1
group-id        = io.zalky
artifact-id     = cues
description     = Queues on cue: persistent blocking queues, processors, and topologies via ChronicleQueue
license         = :apache
url             = https://github.com/zalky/cues

include make-clj/Makefile

test:
	clojure -M:test:cues/j17
