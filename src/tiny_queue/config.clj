(ns tiny-queue.config
  (:require [clojure.spec.alpha :as s]
            [tiny-queue.utils :as u]
            [tiny-queue.spec]))

(defmulti default-log :status)

(defmethod default-log :grab/init [log-params]
  (format "Processor: %s tries to grab the job %s."
          (-> log-params :processor-uuid)
          (-> log-params :job :db/id)))

(defmethod default-log :grab/fail [log-params] 
  (format "Processor: %s failed to grab the job %s. Exception: %s."
          (-> log-params :processor-uuid)
          (-> log-params :job :db/id)
          (some-> log-params :exception u/exception-description)))

(defmethod default-log :grab/success [log-params]
  (format "Processor: %s grabbed the job %s."
          (-> log-params :processor-uuid)
          (-> log-params :job :db/id)))

(defmethod default-log :process/fail [log-params]
  (format "Processor: %s failed the job %s. Exception: %s."
          (-> log-params :processor-uuid)
          (-> log-params :job :db/id)
          (some-> log-params :exception u/exception-description)))

(defmethod default-log :process/success [log-params]
  (format "Processor: %s processed the job %s."
          (-> log-params :processor-uuid)
          (-> log-params :job :db/id)))

(defmethod default-log :default [log-params]
  (format "Processor: %s, job: %s, state: %s"
          (-> log-params :processor-uuid)
          (-> log-params :job :db/id)
          (-> log-params :status)))

(defn get-default-config []
  {:processor-uuid (str (java.util.UUID/randomUUID))
   :job-processor-failed-interval-in-s 120
   :max-process-job-time-in-s 300
   :original-interval-in-ns 5000000
   :log (comp println default-log)})

(defn check-config [config]
  (let [final-config (merge (get-default-config) config)]
    (if (s/valid? :tiny-queue.spec/config final-config)
      (assoc final-config :validated? true)
      (do
        (s/explain :tiny-queue.spec/config final-config)
        (throw (Exception. "Invalid tiny-queue config."))))))