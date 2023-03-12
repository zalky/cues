(ns cues.util
  (:require [clojure.java.io :as io]
            [clojure.walk :as walk])
  (:import java.io.File
           java.util.UUID))

(defn some-entries
  "Returns the given map with nil entries removed."
  [m]
  (reduce-kv
   (fn [m k v]
     (cond-> m
       (some? v) (assoc k v)))
   {}
   m))

(defn into-once
  "Like into, but throws an error on duplicate keys."
  [m coll]
  (reduce
   (fn [m [k v]]
     (if (contains? m k)
       (throw (ex-info "Already added" {:key k}))
       (assoc m k v)))
   (or m {})
   (seq coll)))

(defn prompt-delete!
  [p]
  (println "Delete data (yes/no)?" p)
  (case (read-line)
    "yes" true
    "no"  false
    (println "Must be yes/no")))

(defn assert-path
  "Ensure path is relative and in project."
  [^File f]
  (let [p          (.getCanonicalPath f)
        project-p  (System/getProperty "user.dir")
        project-re (re-pattern (str "^" project-p))]
    (io/as-relative-path f)
    (when-not (re-find project-re p)
      (throw (ex-info "Path not in project" {:path p})))))

(defn delete-file
  "Deletes files in project only, recursively if directory."
  [^File f]
  (assert-path f)
  (when (.isDirectory f)
    (doseq [file (.listFiles f)]
      (delete-file file)))
  (io/delete-file f))

(defn list-files
  "Lists files in the directory, not recursive."
  [^File f]
  (->> f
       (.listFiles)
       (filter #(.isFile %))))

(defn uuid
  []
  (UUID/randomUUID))

(defn map-vals
  [f m]
  (reduce-kv
   (fn [m k v]
     (assoc m k (f v)))
   {}
   m))

(defn distinct-by
  [f coll]
  (->> coll
       (group-by f)
       (map (comp first second))
       (doall)))

;; Catalogs

(defn bind-catalog
  [catalog bindings]
  (walk/postwalk
   (fn [x]
     (get bindings x x))
   catalog))

(defn merge-catalogs
  [& catalogs]
  (->> catalogs
       (reverse)
       (apply concat)
       (distinct-by :id)))
