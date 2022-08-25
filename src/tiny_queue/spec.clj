(ns tiny-queue.spec
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]))

(defn- string-uuid? [string]
  (try (-> string java.util.UUID/fromString uuid?)
       (catch Exception _ false)))

(s/def ::processor-uuid
  (s/with-gen (s/and string? string-uuid?)
              #(gen/fmap str (s/gen uuid?))))

(s/def ::original-interval-in-ns
  (s/and integer? #(> % 0)))

(s/def ::job-processor-failed-interval-in-s
  (s/and integer? #(> % 0)))

(s/def ::max-process-job-time-in-s
  (s/and integer? #(> % 0)))

(s/def ::tiny-queue-processors (s/map-of keyword? fn?))

(s/def :db/id (s/and any? (complement nil?)))

(s/def ::job (s/keys :req [:db/id]))

(s/def ::status
  #{:grab/fail :grab/init :grab/success :process/fail :process/success})

(s/def ::exception (s/with-gen
                     #(instance? java.lang.Throwable %)
                     #(s/gen #{(Exception. "error")})))

(defmulti log-params :status)

(defmethod log-params :grab/fail [_]
  (s/keys :req-un [::job
                   ::status
                   ::processor-uuid
                   ::exception]))

(defmethod log-params :process/fail [_]
  (s/keys :req-un [::job
                   ::status
                   ::processor-uuid
                   ::exception]))

(defmethod log-params :default [_]
  (s/keys :req-un [::job
                   ::status
                   ::processor-uuid]))

(s/def ::status keyword?)

(s/def ::log-params (s/multi-spec log-params ::status))

(s/def ::log (s/fspec :args (s/cat :log-params ::log-params)))

(s/def ::q any?)

(s/def ::conn any?)

(s/def ::db any?)

(s/def ::transact any?)

(s/def ::config
  (s/keys :req-un [::conn
                   ::q
                   ::db
                   ::transact
                   ::tiny-queue-processors]
          :opt-un [::processor-uuid
                   ::job-processor-failed-interval-in-s
                   ::max-process-job-time-in-s
                   ::original-interval-in-ns
                   ::log]))