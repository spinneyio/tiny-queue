(ns tiny-queue.db.limbo
  (:require [clj-time.core :as time]
            [tiny-queue.utils :as u]))

(defn get-limbo-jobs
  [config database]
  (let [{:keys [q max-process-job-time-in-s]} config
        minimum-start (-> (time/now)
                          (time/minus (time/seconds max-process-job-time-in-s))
                          u/to-database-date)]
    (map first
         (q '[:find (pull ?message [:* {:qmessage/qcommand [:db/ident]}])
              :in $ ?minimum-start
              :where [?message :qmessage/status :qmessage-status/pending]
              (not [?message :qmessage/processed-at])
              (not [?message :qmessage/blocked true])
              [?message :qmessage/started-processing-at ?start]
              [(>= ?minimum-start ?start)]]
            database minimum-start))))

(defn fix-limbo-transaction [job fail-time processor-uuid time-increment]
  (let [backoff-factor (or (:qmessage/exponential-backoff-factor job) 0)
        new-backoff-factor (+ 1 backoff-factor)
        permanently-failed (some-> job :qmessage/maximum-retry-count (< new-backoff-factor))
        failed-status (if permanently-failed :qmessage-status/permanently-failed :qmessage-status/failed)
        retry-date (u/get-retry-date fail-time time-increment backoff-factor)
        id (:db/id job)
        orig-uuid (:qmessage/processor-uuid job)]
    [[:db/cas id :qmessage/success nil false]
     [:db/cas id :qmessage/processed-at nil (u/to-database-date fail-time)]
     [:db/cas id :qmessage/processor-uuid orig-uuid processor-uuid]
     [:db/cas id :qmessage/result nil "Job failed - limbo."]
     [:db/add    id :qmessage/exponential-backoff-factor new-backoff-factor]
     [:db/add    id :qmessage/retry-date retry-date]
     [:db/add    id :qmessage/status failed-status]]))
