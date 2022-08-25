(ns tiny-queue.db.query)

(defn get-single-unprocessed-job
  [config database]
  ((:q config)
   '[:find (pull ?message [:* {:qmessage/qcommand [:db/ident]}]) .
     :in $ ?current-date
     :where
     [?message :qmessage/status :qmessage-status/unprocessed]
     [?message :qmessage/execution-date ?ex-date]
     [(.after ^java.util.Date ?current-date ?ex-date)]
     (not [?message :qmessage/blocked true])]
   database (java.util.Date.)))

(defn get-single-failed-job
  [config database] 
  ((:q config)
   '[:find (pull ?message [:* {:qmessage/qcommand [:db/ident]}]) .
     :in $ ?current-date
     :where
     [?message :qmessage/status :qmessage-status/failed]
     [?message :qmessage/retry-date ?rdate]
     [(.after ^java.util.Date ?current-date ?rdate)]
     (not [?message :qmessage/blocked true])]
   database (java.util.Date.)))

(defn get-single-unprocessed-command-job
  [config snapshot command]
  ((:q config)
   '[:find (pull ?message [:* {:qmessage/qcommand [:db/ident]}]) .
     :in $ ?command ?current-date
     :where
     [?message :qmessage/status :qmessage-status/unprocessed]
     [?message :qmessage/qcommand ?command]
     [?message :qmessage/execution-date ?ex-date]
     [(.after ^java.util.Date ?current-date ?ex-date)]
     (not [?message :qmessage/blocked true])]
   snapshot command (java.util.Date.)))