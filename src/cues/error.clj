(ns cues.error
  (:require [cinch.core :as util]))

(defn- error-message
  [context e]
  (or (:err/message context)
      (ex-message e)))

(defn- error-cause
  [e]
  (let [data (ex-data e)]
    (if (:q/type data)
      (ex-cause e)
      e)))

(defn- error-data
  [context e]
  (let [data (ex-data e)]
    (if (:q/type data)
      (merge context data)
      context)))

(defn error
  "Wraps any kind of exception in the Cues error data model, merging
  additional context in a transparent way. The intent is to be able to
  successively merge in Cues error data as some processor error
  bubbles up the implementation stack. Error context is merged, rather
  chained.

  Given an exception that is raised in user code:

                   exception         cause
  domain exception e                 -
  cues level 1     (merge c1)        e
  cues level 2     (merge c1 c2)     e
  cues level 3     (merge c1 c2 c3)  e
  ..."
  ([e]
   (error nil e))
  ([context e]
   (let [cause (error-cause e)
         m     (Throwable->map cause)
         data  (error-data context e)]
     (ex-info (error-message context e)
              (merge {:q/type    :q.type.err/error
                      :err/cause m}
                     data)
              cause))))

(defmacro wrap-error
  [context & body]
  `(try
     (do ~@body)
     (catch InterruptedException e#
       (throw e#))
     (catch Exception e#
       (throw (error ~context e#)))))

