To start message queue You should do following steps:
1. Create `config`:
```clojure 
(require '[datomic.api :as d])
(def conn (d/connect "your-datomic-db-uri"))
(def config 
  {:conn conn
   :q d/q
   :db d/db
   :transact d/transact-async
   :tiny-queue-processors {}})
```
2. Transact `tiny-queue` schema:
```clojure
(tq/create-schema config)
```
3. Define your first custom job (first argument is a keyword - `:db/ident`, second argument is a doc string):
```clojure
(tq/define-new-job config :qcommand/first-job "My first job!")
```
4. Define first job processor (processor should return Datomic transaction):
```clojure
(defn first-job-processor [[snapshot job uuid]]
  (println "Processing first job" uuid)
  [])
```
5. Define proper `tiny-queue-processors`:
```clojure
(def tiny-queue-processors {:qcommand/first-job first-job-processor})
```
6. Start tiny-queue using `wrap-background-job`:
```clojure
(require '[tiny-queue.core :as tq])
(def final-config (assoc config :tiny-queue-processors tiny-queue-processors))
(def background-processor (atom nil))
(reset! background-processor (future (wrap-background-job final-config 0)))
```
7. Add new job to the tiny-queue:
```clojure
(tq/create-new-job final-config {:command :qcommand/first-job
                                 :data "first-job-data"})
```