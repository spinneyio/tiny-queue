(ns tiny-queue.db.transaction
  (:require [tiny-queue.utils :as u]))

(defn fail-transaction [job processor-id fail-time result time-increment]
  (let [backoff-factor (or (:qmessage/exponential-backoff-factor job) 0)
        retry-date (u/get-retry-date fail-time time-increment backoff-factor)
        id (:db/id job)]
    [[:db/add id :qmessage/success nil false]
     [:db/add id :qmessage/processed-at nil (u/to-database-date fail-time)]
     [:db/add id :qmessage/processor-uuid processor-id processor-id]
     [:db/add id :qmessage/result nil (u/result->string result)]
     [:db/add id :qmessage/exponential-backoff-factor (+ 1 backoff-factor)]
     [:db/add id :qmessage/retry-date retry-date]
     [:db/add id :qmessage/status :qmessage-status/failed]]))

(defn grab-unprocessed-job-transaction [job processor-uuid start-time]
  (let [id (:db/id job)]
   [[:db/cas id :qmessage/processor-uuid nil processor-uuid]
    [:db/cas id :qmessage/started-processing-at nil (u/to-database-date start-time)]
    [:db/add    id :qmessage/status :qmessage-status/pending]]))

(defn grab-failed-job-transaction [job processor-uuid start-time]
  (let [id (:db/id job)
        orig-uuid (:qmessage/processor-uuid job)
        orig-time (-> job 
                      :qmessage/started-processing-at 
                      u/to-database-date) 
        orig-processed (:qmessage/processed-at job)
        orig-result (:qmessage/result job)
        orig-retry  (:qmessage/retry-date job)]
    [[:db/cas id :qmessage/processor-uuid orig-uuid processor-uuid]
     [:db/retract id :qmessage/success false]
     [:db/cas id :qmessage/started-processing-at orig-time (u/to-database-date start-time)]
     [:db/add id :qmessage/status :qmessage-status/pending]
     [:db/retract id :qmessage/processed-at orig-processed]
     [:db/retract id :qmessage/result orig-result]
     [:db/retract id :qmessage/retry-date orig-retry]]))

(defn success-transaction [job processor-id success-time result]
  (let [id (:db/id job)]
   [[:db/add id :qmessage/success nil true]
    [:db/add id :qmessage/processed-at nil (u/to-database-date success-time)]
    [:db/add id :qmessage/processor-uuid processor-id processor-id]
    [:db/add id :qmessage/result nil (u/result->string result)]
    [:db/add id :qmessage/status :qmessage-status/succeeded]]))