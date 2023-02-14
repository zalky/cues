(ns cues.error
  (:require [cinch.core :as util]))

(defn- error-message
  [e caught]
  (or (ex-message caught)
      (:err/message e)))

(defn- error-cause
  [caught]
  (or (ex-cause caught) caught))

(defn- error-merge
  [e caught]
  (let [data (ex-data caught)]
    (if (:kr/type data)
      (merge e data)
      e)))

(defn error
  ([caught]
   (error nil caught))
  ([e caught]
   (let [cause (error-cause caught)
         e     (error-merge e caught)]
     (ex-info (error-message e caught)
              (->> cause
                   (Throwable->map)
                   (util/assoc-nil e :err/cause))
              cause))))

(defmacro on-error
  [error-expr & body]
  `(try
     (do ~@body)
     (catch Throwable e#
       (throw (error ~error-expr e#)))))

