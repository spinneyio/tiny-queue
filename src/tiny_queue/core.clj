(ns tiny-queue.core
  (:require [tiny-queue.config :as config]
            [tiny-queue.db.transaction :as db-transaction]
            [tiny-queue.db.query :as db-query]
            [tiny-queue.db.schema :as db-schema]
            [clj-time.core :as time]
            [tiny-queue.utils :as u]))

(defn create-schema [config]
  ((:transact config) (:tiny-queue-db-conn config) db-schema/qmessage-schema))

(defn create-datomic-cloud-schema [config]
  ((:transact config) (:tiny-queue-db-conn config) db-schema/datomic-cloud-qmessage-schema))

(defn failed? [job]
  (= false (:qmessage/success job)))

(defn job-failure-count [job]
  (or (:qmessage/exponential-backoff-factor job) 0))

(defn associated-uuid [job]
  (:qmessage/object-uuid job))

(defn data-from-job [job]
  (if (string? (:qmessage/data job))
    (read-string (:qmessage/data job))
    (:qmessage/data job)))

(defn get-new-job
  [{:keys [command data object date periodic blocked maximum-retry-count]}]
  (let [job {:qmessage/qcommand command
             :qmessage/data (if (string? data) data (prn-str data))
             :qmessage/status :qmessage-status/unprocessed
             :qmessage/execution-date (or date (java.util.Date.))}]
    (-> job
        (cond-> periodic (assoc :qmessage/periodic periodic))
        (cond-> blocked (assoc :qmessage/blocked blocked))
        (cond-> object (assoc :qmessage/object-uuid object))
        (cond-> maximum-retry-count (assoc :qmessage/maximum-retry-count maximum-retry-count)))))

(defn get-new-job-transaction [job-params]
  [(get-new-job job-params)])

(defn create-new-job
  [config job-params]
  ((:transact config) (:tiny-queue-db-conn config) (get-new-job-transaction job-params)))

(defn define-new-job
  [config job-ident job-docs]
  (let [job-schema [{:db/ident job-ident
                     :db/doc   job-docs}]]
    ((:transact config) (:tiny-queue-db-conn config) job-schema)))

(defn grab-job [config job]
  (let [{:keys [tiny-queue-db-conn
                processor-uuid
                transact
                log]} config
        get-grab-job-transaction (if (failed? job)
                                   db-transaction/grab-failed-job-transaction
                                   db-transaction/grab-unprocessed-job-transaction)
        grab-job-transaction (get-grab-job-transaction job processor-uuid (time/now))]
    (try
      (when log
        (log {:job job
              :processor-uuid processor-uuid
              :status :grab/init}))
      (transact tiny-queue-db-conn grab-job-transaction)
      (when log
        (log {:job job
              :processor-uuid processor-uuid
              :status :grab/success}))
      true
      (catch Throwable e
        (when log
          (log {:job job
                :processor-uuid processor-uuid
                :exception e
                :status :grab/fail}))
        false))))

(defn- try-transact
  ([config job processor-uuid transaction]
   (try-transact config job processor-uuid transaction 3))
  ([config job processor-uuid transaction retry-count]
   (let [{:keys [tiny-queue-db-conn transact log]} config]
    (try
      (transact tiny-queue-db-conn transaction)
      (catch Throwable e
        (if (> retry-count 0)
          (try-transact
           config
           job
           processor-uuid
           transaction
           (dec retry-count))
          (throw e)))))))

(defn process-job
  "A job is a function that does not perform any transactions to tiny queue db. Instead,
   it returns a transaction to be performed. This limitation is intentional."
  ([config tiny-queue-db-snapshot job]
   (process-job config tiny-queue-db-snapshot job false))
  ([config tiny-queue-db-snapshot job testing?]
   (let [{:keys [tiny-queue-db-conn
                 processor-uuid
                 log
                 job-processor-failed-interval-in-s
                 tiny-queue-processors
                 transact]} config
         processor-id (-> job
                          :qmessage/qcommand
                          :db/ident)
         processor (processor-id tiny-queue-processors)]
     (try      
       (assert processor (str "No processor found for command: " processor-id))
       (let [tiny-queue-db-transaction (u/with-timeout 60000 (processor
                                                              tiny-queue-db-snapshot
                                                              job
                                                              processor-uuid))
             success-transaction (db-transaction/success-transaction
                                  job
                                  processor-uuid
                                  (time/now)
                                  "OK")
             final-transaction (concat
                                tiny-queue-db-transaction
                                success-transaction)]
         (try-transact config job processor-uuid final-transaction))
       (when log
         (log {:job job
               :processor-uuid processor-uuid
               :status :process/success}))
       (catch Throwable e
         (when log
           (log {:job job
                 :processor-uuid processor-uuid
                 :exception e
                 :status :process/fail}))
         (transact
          tiny-queue-db-conn
          (db-transaction/fail-transaction
           job
           processor-uuid
           (time/now)
           (u/exception-description e)
           job-processor-failed-interval-in-s))
         (when testing? (throw e)))))))

(defn grab-process-job [config tiny-queue-db-snapshot job]
  (when (grab-job config job)
    (process-job config tiny-queue-db-snapshot job)))

(defn test-grab-process-job 
  "A version of grab-process-job that throws if a job cannot be grabbed or ends with an exception."
  [config tiny-queue-db-snapshot job]
  (assert (grab-job config job) (str "Cannot grab the job: " job))
  (process-job config tiny-queue-db-snapshot job true))

(defn wrap-background-job
  [config ^Long remaining-time]
  (let [config (if (not (:validated? config))
                 (config/check-config config)
                 config)
        {:keys [tiny-queue-db-conn
                processor-uuid
                original-interval-in-ns
                db
                log]} config
        sleep-nans #(Thread/sleep (unchecked-divide-int % (int 1e3)) (mod % 1e3))
        start-time (System/nanoTime)]
    (try
      (let [tiny-queue-db-snapshot (db tiny-queue-db-conn)
            unprocessed-job (or (db-query/get-single-unprocessed-job config tiny-queue-db-snapshot)
                                (db-query/get-single-failed-job config tiny-queue-db-snapshot))]
        (if (and (> remaining-time 0) unprocessed-job)
          (grab-process-job config tiny-queue-db-snapshot unprocessed-job)
          (when (> remaining-time 0)
            (sleep-nans remaining-time))))
      (catch Exception e
        (when log
          (log {:processor-uuid processor-uuid
                :exception e
                :status :wrap-background-job/fail}))))
    (let [end-time (System/nanoTime)
          remaining-subtracted (- remaining-time (- end-time start-time))
          remaining-final (if (> remaining-subtracted 0)
                            remaining-subtracted
                            original-interval-in-ns)]
      (recur config remaining-final))))
