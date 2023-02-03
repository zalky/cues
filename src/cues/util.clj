(ns cues.util
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.walk :as walk])
  (:import java.io.File
           java.util.UUID))

(defn collection?
  "Returns true in the sense of a java.util.Collection."
  [x]
  (or (sequential? x) (set? x)))

(defn seqify
  "Coerces x into vector if isn't already sequential."
  [x]
  (if (collection? x)
    x
    [x]))

(defn setify
  "Coerces x into set if isn't already."
  [x]
  (cond
    (sequential? x) (set x)
    (set? x)        x
    :else           #{x}))

(defn conjs
  "Like conj but always set."
  [set & args]
  (apply conj (or set #{}) args))

(defn conj-any
  "Given two values, performs to-many conj."
  [v1 v2]
  (cond
    (coll? v1) (conj v1 v2)
    (nil? v1)  v2
    :else      (hash-set v1 v2)))

(defn some-entries
  [m]
  (reduce-kv
   (fn [m k v]
     (cond-> m
       (some? v) (assoc k v)))
   {}
   m))

(defn assoc-nil
  "Only assoc if existing value is `nil`."
  [e attr v]
  (cond-> e
    (nil? (get e attr)) (assoc attr v)))

(defn assoc-in-nil
  "Only assoc if existing value is `nil`."
  [e attrs v]
  (cond-> e
    (nil? (get-in e attrs)) (assoc-in attrs v)))

(defn update-some
  "Only update if value exists at the given attr."
  [e attr f & args]
  (if (contains? e attr)
    (apply update e attr f args)
    e))

(defn reverse-map
  [m]
  (into {} (map (fn [[k v]] [v k]) m)))

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

(defn map-all
  "Like map but exhausts all colls."
  [f pad & colls]
  (letfn [(pick [xs]
            (if (seq xs)
              (first xs)
              pad))]
    (lazy-seq
     (when (some seq colls)
       (cons
        (apply f (map pick colls))
        (apply map-all f pad (map rest colls)))))))

(defn merge-deep
  "Like merge, but merges recusively. Maps are merged via merge
  semantics, and vectors are merged positionally."
  [& args]
  (letfn [(mf [& args]
            (if (every? map? args)
              (apply merge-with mf args)
              (if (every? sequential? args)
                (apply map-all (partial merge-with mf) nil args)
                (last args))))]
    (apply mf (remove nil? args))))

(defn prompt-delete-data!
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
  "Lists files in the directory, not recurisve."
  [^File f]
  (->> f
       (.listFiles)
       (filter #(.isFile %))))

(defn unformed
  [spec]
  (s/conformer
   (fn [x]
     (let [form (s/conform spec x)]
       (case form
         ::s/invalid form
         (second form))))))

(defn conform-to
  "Given a spec, returns a version of that spec that additionally
  applies f to the conformed value."
  [spec f]
  (s/conformer
   (fn [x]
     (let [form (s/conform spec x)]
       (if (= form ::s/invalid)
         form
         (f form))))))

(defn parse
  [spec expr]
  (let [form (s/conform spec expr)]
    (case form
      ::s/invalid (throw
                   (ex-info
                    (s/explain-str spec expr)
                    (s/explain-data spec expr)))
      form)))

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
