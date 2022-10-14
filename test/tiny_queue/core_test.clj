(ns tiny-queue.core-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [datomic.api :as d]
            [clj-time.core :as time]
            [tiny-queue.core :as tq]
            [tiny-queue.db.query :as tq-query]
            [tiny-queue.db.transaction :as tq-transaction]
            [tiny-queue.utils :as u]))

(def datomic-uri "datomic:mem://mocked")

(defn job-processor [_tiny-queue-db-snapshot _job uuid]
  (println "Processing job" uuid)
  [])

(defn get-default-test-config [conn]
  {:tiny-queue-db-conn conn
   :q d/q
   :db d/db
   :transact (fn [conn transaction]
               @(d/transact conn transaction))
   :tiny-queue-processors {:qcommand/job job-processor}
   :processor-uuid (java.util.UUID/randomUUID)
   :job-processor-failed-interval-in-s 120
   :max-process-job-time-in-s 300
   :original-interval-in-ns 5000000
   :log (constantly nil)})

(defn setup-test-environment [f]
  (let [conn (do (d/create-database datomic-uri)
                 (d/connect datomic-uri))
        config (get-default-test-config conn)]
    (tq/create-schema config)
    (tq/define-new-job config :qcommand/job "Job")
    (f)
    (d/delete-database datomic-uri)))

(use-fixtures :each setup-test-environment)

(deftest grab-process-job 
  (let [conn (d/connect datomic-uri)
        processors {:qcommand/job job-processor}
        config (-> conn
                   get-default-test-config
                   (assoc :tiny-queue-processors processors))] 
    (testing "Add first job"
      (let [five-minutes-before (-> (time/now)
                                    (time/minus (time/minutes 5))
                                    u/to-database-date)]
        (tq/create-new-job
         config
         {:command :qcommand/job
          :data {:sample "sample-data"}
          :date five-minutes-before})))
    
    (testing "Grab job"
      (let [db-with-job (d/db conn)
            job (tq-query/get-single-unprocessed-job config db-with-job)
            grab-result (tq/grab-job config job)]
        (is grab-result)))
    
    (testing "Check db with grabbed job"
      (let [db-with-grabbed-job (d/db conn)]
        (is (nil? (tq-query/get-single-unprocessed-job config db-with-grabbed-job)))
        (is (tq-query/get-single-status-job config db-with-grabbed-job :qmessage-status/pending))))
    
    (testing "Process job"
      (let [db-with-grabbed-job (d/db conn)
            grabbed-job (tq-query/get-single-status-job
                         config
                         db-with-grabbed-job
                         :qmessage-status/pending)]
       (tq/process-job
        config 
        db-with-grabbed-job
        grabbed-job)))
    
    (testing "Check db with completed job"
      (let [db-with-completed-job (d/db conn)]
        (is (nil? (tq-query/get-single-unprocessed-job config db-with-completed-job)))
        (is (nil? (tq-query/get-single-status-job config db-with-completed-job :qmessage-status/pending)))
        (is (tq-query/get-single-status-job config db-with-completed-job :qmessage-status/succeeded))))))

(deftest successful-job-processing
  (let [conn (d/connect datomic-uri)
        config (get-default-test-config conn)]
    (testing "Check empty db"
      (let [empty-db (d/db conn)]
        (is (= 0 (count (tq-query/get-unprocessed-jobs config empty-db))))
        (is (= 0 (count (tq-query/get-failed-jobs config empty-db))))
        (is (= 0 (count (tq-query/get-processing-jobs config empty-db))))
        (is (nil? (tq-query/get-single-unprocessed-job config empty-db)))
        (is (nil? (tq-query/get-single-failed-job config empty-db)))))

    (testing "Add first job"
      (let [five-minutes-before (-> (time/now)
                                    (time/minus (time/minutes 5))
                                    u/to-database-date)]
        (tq/create-new-job
         config
         {:command :qcommand/job
          :data {:sample "sample-data"}
          :date five-minutes-before})))

    (testing "Check db with first unprocessed job"
      (let [db-with-job (d/db conn)
            first-job (tq-query/get-single-unprocessed-job config db-with-job)]
        (is (= 1 (count (tq-query/get-unprocessed-jobs config db-with-job))))
        (is (= 0 (count (tq-query/get-failed-jobs config db-with-job))))
        (is (= 0 (count (tq-query/get-processing-jobs config db-with-job))))
        (is first-job)
        (is (not (tq/failed? first-job)))
        (is (nil? (tq-query/get-single-failed-job config db-with-job)))))

    (testing "Grab the job"
      (let [db-with-job (d/db conn)
            first-job (tq-query/get-single-unprocessed-job config db-with-job)
            grab-job-transaction (tq-transaction/grab-unprocessed-job-transaction
                                  first-job
                                  (:processor-uuid config)
                                  (time/now))]
        @(d/transact conn grab-job-transaction)
        ;; Job can't be grabbed twice!
        (is (thrown? Exception @(d/transact conn grab-job-transaction)))))

    (testing "Check db with grabbed job"
      (let [grabbed-db (d/db conn)]
        (is (= 0 (count (tq-query/get-unprocessed-jobs config grabbed-db))))
        (is (= 0 (count (tq-query/get-failed-jobs config grabbed-db))))
        (is (= 1 (count (tq-query/get-processing-jobs config grabbed-db))))))

    (testing "Complete processing job with success transaction"
      (let [grabbed-db (d/db conn)
            grabbed-job (tq-query/get-single-status-job
                         config
                         grabbed-db
                         :qmessage-status/pending)
            success-transaction (tq-transaction/success-transaction
                                 grabbed-job
                                 (:processor-uuid config)
                                 (time/now)
                                 {:success "OK"})]
        @(d/transact conn success-transaction)))

    (testing "Check db with succeeded job"
      (let [success-db (d/db conn)]
        (is (= 0 (count (tq-query/get-unprocessed-jobs config success-db))))
        (is (= 0 (count (tq-query/get-failed-jobs config success-db))))
        (is (= 0 (count (tq-query/get-processing-jobs config success-db))))
        (is (nil? (tq-query/get-single-status-job config success-db :qmessage-status/pending)))
        (is (tq-query/get-single-status-job config success-db :qmessage-status/succeeded))))))

(deftest unsuccessful-job-processing
  (let [conn (d/connect datomic-uri)
        config (get-default-test-config conn)]
    (testing "Check empty db"
      (let [empty-db (d/db conn)]
        (is (= 0 (count (tq-query/get-unprocessed-jobs config empty-db))))
        (is (= 0 (count (tq-query/get-failed-jobs config empty-db))))
        (is (= 0 (count (tq-query/get-processing-jobs config empty-db))))
        (is (nil? (tq-query/get-single-unprocessed-job config empty-db)))
        (is (nil? (tq-query/get-single-failed-job config empty-db)))))

    (testing "Add first job"
      (let [five-minutes-before (-> (time/now)
                                    (time/minus (time/minutes 5))
                                    u/to-database-date)]
        (tq/create-new-job
         config
         {:command :qcommand/job
          :data {:sample "sample-data"}
          :date five-minutes-before})))

    (testing "Check db with first unprocessed job"
      (let [db-with-job (d/db conn)
            first-job (tq-query/get-single-unprocessed-job config db-with-job)]
        (is (= 1 (count (tq-query/get-unprocessed-jobs config db-with-job))))
        (is (= 0 (count (tq-query/get-failed-jobs config db-with-job))))
        (is (= 0 (count (tq-query/get-processing-jobs config db-with-job))))
        (is first-job)
        (is (not (tq/failed? first-job)))
        (is (nil? (tq-query/get-single-failed-job config db-with-job)))))

    (testing "Grab the job"
      (let [db-with-job (d/db conn)
            first-job (tq-query/get-single-unprocessed-job config db-with-job)
            grab-job-transaction (tq-transaction/grab-unprocessed-job-transaction
                                  first-job
                                  (:processor-uuid config)
                                  (time/now))]
        @(d/transact conn grab-job-transaction)
        ;; Job can't be grabbed twice!
        (is (thrown? Exception @(d/transact conn grab-job-transaction)))))

    (testing "Check db with grabbed job"
      (let [grabbed-db (d/db conn)]
        (is (= 0 (count (tq-query/get-unprocessed-jobs config grabbed-db))))
        (is (= 0 (count (tq-query/get-failed-jobs config grabbed-db))))
        (is (= 1 (count (tq-query/get-processing-jobs config grabbed-db))))))

    (testing "Complete processing job with fail transaction"
      (let [grabbed-db (d/db conn)
            grabbed-job (tq-query/get-single-status-job
                         config
                         grabbed-db
                         :qmessage-status/pending)
            fail-transaction (tq-transaction/fail-transaction
                              grabbed-job
                              (:processor-uuid config)
                              (time/now)
                              {:success "NO"}
                              0)]
        @(d/transact conn fail-transaction)))

    (testing "Check db with failed job"
      (let [failure-db (d/db conn)]
        (is (= 0 (count (tq-query/get-unprocessed-jobs config failure-db))))
        (is (= 1 (count (tq-query/get-failed-jobs config failure-db))))
        (is (= 0 (count (tq-query/get-processing-jobs config failure-db))))
        (is (nil? (tq-query/get-single-status-job config failure-db :qmessage-status/pending)))
        (is (nil? (tq-query/get-single-status-job config failure-db :qmessage-status/succeeded)))
        (is (tq-query/get-single-status-job config failure-db :qmessage-status/failed))))

    (testing "Try to grab failed job"
      (let [failure-db (d/db conn)
            failed-transaction (first (tq-query/get-failed-jobs config failure-db))
            grab-failed-job-transaction (tq-transaction/grab-failed-job-transaction
                                         failed-transaction
                                         (:processor-uuid config)
                                         (time/now))]
        @(d/transact conn grab-failed-job-transaction)))

    (testing "Check db with grabbed job"
      (let [failure-grabbed-db (d/db conn)]
        (is (= 0 (count (tq-query/get-unprocessed-jobs config  failure-grabbed-db))))
        (is (= 0 (count (tq-query/get-failed-jobs config failure-grabbed-db))))
        (is (= 1 (count (tq-query/get-processing-jobs config failure-grabbed-db))))))))