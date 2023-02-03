(ns cues.log
  (:require [clojure.spec.alpha :as s]
            [expound.alpha :as expound]
            [taoensso.encore :as enc]
            [taoensso.timbre :as log]
            [taoensso.timbre.appenders.core :as appenders]))

(defn std-appender
  "Basic appender that appends to stdout and stderr."
  []
  (let [f (:fn (appenders/println-appender))]
    {:enabled?   true
     :async?     true
     :min-level  nil
     :rate-limit nil
     :output-fn  :inherit
     :fn         (fn [data]
                   (binding [*print-namespace-maps* false]
                     (log/with-default-outs (f data))))}))

(defn realise-lazy-seqs
  "prints lazy sequences in timbre's log output
   https://github.com/ptaoussanis/timbre/pull/200#issuecomment-259535414"
  [data]
  (letfn [(->str [x]
            (if (enc/lazy-seq? x)
              (pr-str x)
              x))]
    (update data :vargs (partial mapv ->str))))

(def config
  "Basic timbre config."
  {:level        :debug
   :configured   "cues"
   :ns-whitelist ["user" "runway.*" "cues.*"]
   :middleware   [realise-lazy-seqs]
   :appenders    {:std-appender (std-appender)}})

;; Set logging if not already set, but override known dependency
;; configurations.
(let [c (:configured log/*config*)]
  (when (or (nil? c) (contains? #{"cues" "runway"} c))
    (log/set-config! config)))

(alter-var-root #'s/*explain-out* (constantly expound/printer))
(s/check-asserts true)
