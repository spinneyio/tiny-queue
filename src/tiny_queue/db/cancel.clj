(ns tiny-queue.db.cancel)

(defn get-jobs-from-object [config object snapshot] 
  (map first
       ((:q config)
        '[:find (pull ?message [:* {:qmessage/qcommand [:db/ident]}])
          :in $ ?object ?current-date
          :where
          [?message :qmessage/object-uuid ?object]
          (not [?message :qmessage/processed-at])
          (not [?message :qmessage/started-processing-at])
          (or-join [?message ?current-date]
                   (not [?message :qmessage/execution-date])
                   (and [?message :qmessage/execution-date ?ex-date]
                        [(< ?current-date ?ex-date)]))]
        snapshot object (java.util.Date.))))

(defn- cancel-transaction [job]
  [[:db/retractEntity (:db/id job)]])

(defn cancel-object-jobs
  ([config object snapshot]
   (let [jobs (get-jobs-from-object config object snapshot)]
     (mapcat cancel-transaction jobs)))
  ([config object snapshot command]
   (let [jobs (get-jobs-from-object config object snapshot)]
     (->> jobs
          (filter #(= (-> % :qmessage/qcommand :db/ident) command))
          (mapcat cancel-transaction))))
  ([config object snapshot command type]
   (let [jobs (get-jobs-from-object config object snapshot)]
     (->> jobs
          (filter #(= (-> % :qmessage/qcommand :db/ident) command))
          (filter #(= (-> % :qmessage/data read-string :type) type))
          (mapcat cancel-transaction)))))