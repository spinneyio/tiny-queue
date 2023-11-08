(ns tiny-queue.db.schema)

(def qmessage-schema
  [{:db/ident              :qmessage-status/transaction-failed
    :db/doc                "The qmessage status"}
   {:db/ident              :qmessage-status/permanently-failed
    :db/doc                "The qmessage status"}
   {:db/ident              :qmessage-status/failed
    :db/doc                "The qmessage status"}
   {:db/ident              :qmessage-status/succeeded
    :db/doc                "The qmessage status"}
   {:db/ident              :qmessage-status/unprocessed
    :db/doc                "The qmessage status"}
   {:db/ident              :qmessage-status/pending
    :db/doc                "The qmessage status"}

   {:db/ident              :qmessage/qcommand
    :db/valueType          :db.type/ref
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "The command to be processed"}

   {:db/ident              :qmessage/status
    :db/valueType          :db.type/ref
    :db/cardinality        :db.cardinality/one
    :db/index              true
    :db/noHistory          true
    :db/doc                "Status - one of: failed, succeeded, unprocessed."}

   {:db/ident              :qmessage/data
    :db/valueType          :db.type/string
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "Data to be processed, stored in EDN"}

   {:db/ident              :qmessage/object
    :db/valueType          :db.type/ref
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "Object involved"}

   {:db/ident              :qmessage/object-id
    :db/valueType          :db.type/long
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "Database-unique entity id for an object (stored in an external database)."}

   {:db/ident              :qmessage/object-uuid
    :db/valueType          :db.type/uuid
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "UUID for an object (stored in an external database)."}

   {:db/ident              :qmessage/processed-at
    :db/valueType          :db.type/instant
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "Time the message was processed"}

   {:db/ident              :qmessage/started-processing-at
    :db/valueType          :db.type/instant
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "Time the message processing started"}

   {:db/ident              :qmessage/processor-uuid
    :db/valueType          :db.type/uuid
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "The uuid of the processor thread"}

   {:db/ident              :qmessage/result
    :db/valueType          :db.type/string
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "Computation result, stored in EDN"}

   {:db/ident              :qmessage/success
    :db/valueType          :db.type/boolean
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "Indicates success/failure of the last try"}

   {:db/ident              :qmessage/exponential-backoff-factor
    :db/valueType          :db.type/long
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "Used to compute how long should we wait before the retry"}

   {:db/ident              :qmessage/retry-count
    :db/valueType          :db.type/long
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "How many times should the job be retried after failure (if not present, there is no limit on retries)"}

   {:db/ident              :qmessage/retry-date
    :db/valueType          :db.type/instant
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "Used to quickly find out if a retry is due."}

   {:db/ident              :qmessage/execution-date
    :db/valueType          :db.type/instant
    :db/index              true
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "Time of future command execution."}

   {:db/ident              :qmessage/periodic
    :db/valueType          :db.type/boolean
    :db/index              true
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "Confirms that task is (or is not) periodic."}

   {:db/ident              :qmessage/blocked
    :db/valueType          :db.type/boolean
    :db/index              true
    :db/cardinality        :db.cardinality/one
    :db/noHistory          true
    :db/doc                "Confirms that task is (or is not) blocked."}])

(def datomic-cloud-qmessage-schema (mapv #(dissoc % :db/index) qmessage-schema))

(def example-partition-schema
  [{:db/id "tiny-queue"
    :db/ident :tiny-queue}
   [:db/add :db.part/db :db.install/partition "tiny-queue"]])
