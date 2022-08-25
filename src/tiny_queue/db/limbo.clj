(ns tiny-queue.db.limbo
  (:require [clj-time.core :as time]
            [tiny-queue.utils :as u]))

(defn get-limbo-jobs
  [config database]
  (let [{:keys [q max-process-job-time]} config
        minimum-start (-> (time/now)
                          (time/minus (time/seconds max-process-job-time))
                          u/to-database-date)]
    (q '[:find [(pull ?message [:* {:qmessage/qcommand [:db/ident]}]) ...]
         :in $ ?minimum-start
         :where [?message :qmessage/status :qmessage-status/pending]
         (not [?message :qmessage/processed-at])
         (not [?message :qmessage/blocked true])
         [?message :qmessage/started-processing-at ?start]
         [(.after ^java.util.Date ?minimum-start ?start)]]
       database minimum-start)))

(defn fix-limbo-transaction [job fail-time processor-uuid time-increment]
  (let [backoff-factor (or (:qmessage/exponential-backoff-factor job) 0)
        retry-date (u/get-retry-date fail-time time-increment backoff-factor)
        id (:db/id job)
        orig-uuid (:qmessage/processor-uuid job)]
    [[:db.fn/cas id :qmessage/success nil false]
     [:db.fn/cas id :qmessage/processed-at nil (u/to-database-date fail-time)]
     [:db.fn/cas id :qmessage/processor-uuid orig-uuid processor-uuid]
     [:db.fn/cas id :qmessage/result nil "Job failed - limbo."]
     [:db/add    id :qmessage/exponential-backoff-factor (+ 1 backoff-factor)]
     [:db/add    id :qmessage/retry-date retry-date]
     [:db/add    id :qmessage/status :qmessage-status/failed]]))