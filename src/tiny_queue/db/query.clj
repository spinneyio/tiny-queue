(ns tiny-queue.db.query)

(defn get-single-unprocessed-job
  [config snapshot]
  (ffirst
   ((:q config)
    '[:find (pull ?message [:* {:qmessage/qcommand [:db/ident]}])
      :in $ ?current-date
      :where
      [?message :qmessage/status :qmessage-status/unprocessed]
      [?message :qmessage/execution-date ?ex-date]
      [(.after ^java.util.Date ?current-date ?ex-date)]
      (not [?message :qmessage/blocked true])]
    snapshot (java.util.Date.))))

(defn get-single-failed-job
  [config snapshot] 
  (ffirst
   ((:q config)
    '[:find (pull ?message [:* {:qmessage/qcommand [:db/ident]}])
      :in $ ?current-date
      :where
      [?message :qmessage/status :qmessage-status/failed]
      [?message :qmessage/retry-date ?rdate]
      [(.after ^java.util.Date ?current-date ?rdate)]
      (not [?message :qmessage/blocked true])]
    snapshot (java.util.Date.))))

(defn get-single-unprocessed-future-command-job
  [config snapshot command]
  (ffirst
   ((:q config)
    '[:find (pull ?message [:* {:qmessage/qcommand [:db/ident]}])
      :in $ ?command ?current-date
      :where
      [?message :qmessage/status :qmessage-status/unprocessed]
      [?message :qmessage/qcommand ?command]
      [?message :qmessage/execution-date ?ex-date]
      [(.after ^java.util.Date ?ex-date ?current-date)]
      (not [?message :qmessage/blocked true])]
    snapshot command (java.util.Date.))))

(defn get-processing-jobs
  [config snapshot]
  (map first
       ((:q config)
        '[:find (pull ?message [:* {:qmessage/qcommand [:db/ident]}])
          :where
          [?message :qmessage/status :qmessage-status/pending]
          (not [?message :qmessage/processed-at])
          [?message :qmessage/started-processing-at]]
        snapshot)))

(defn get-unprocessed-jobs
  [config snapshot]
  (map first
       ((:q config)
        '[:find (pull ?message [:* {:qmessage/qcommand [:db/ident]}])
          :where
          [?message :qmessage/status :qmessage-status/unprocessed]
          (not [?message :qmessage/processed-at])
          (not [?message :qmessage/started-processing-at])
          (not [?message :qmessage/blocked true])]
        snapshot)))

(defn get-failed-jobs
  [config snapshot]
  (map first
       ((:q config)
        '[:find (pull ?message [:* {:qmessage/qcommand [:db/ident]}])
          :where [?message :qmessage/status :qmessage-status/failed]
          [?message :qmessage/started-processing-at]
          [?message :qmessage/processed-at]
          [?message :qmessage/processor-uuid]
          [?message :qmessage/success false]]
        snapshot)))

(defn get-all-command-job-ids-from-object
  [config snapshot object command]
  (map first
       ((:q config)
        '[:find ?message
          :in $ ?object ?command
          :where
          [?message :qmessage/object-uuid ?object]
          [?message :qmessage/qcommand ?command]]
        snapshot object command)))

(defn get-all-command-job-ids-from-start-date-with-object
  [config snapshot object command start-date]
  (map first
       ((:q config)
        '[:find ?message
          :in $ ?object ?command ?start-date
          :where
          [?message :qmessage/object-uuid ?object]
          [?message :qmessage/qcommand ?command ?t]
          [?t :db/txInstant ?date]
          [(<= ?start-date ?date)]]
        snapshot object command start-date)))

(defn count-command-jobs [config snapshot command]
  (or (ffirst
       ((:q config)
        '[:find (count ?message)
          :in $ ?command
          :where
          [?message :qmessage/qcommand ?command]]
        snapshot command))
      0))