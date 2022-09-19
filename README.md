# tiny-queue 
A Clojure library that implements message queue with Datomic. <br> <br>
[![Build Status](https://app.travis-ci.com/spinneyio/tiny-queue.svg?branch=master)](https://app.travis-ci.com/spinneyio/tiny-queue)
## Installation

Leiningen coordinates:
```clojure
[io.spinney/tiny-queue "1.0.5"]
```

## Usage

The library provides `wrap-background-job` function, which can be used to start the message queue. 
This function requires your configuration in the form:
```clojure 
 {:conn your-datomic-db-connection
  :q d/q
  :db d/db
  :transact d/transact-async
  :tiny-queue-processors {}}
```
Where `d` is one of `datomic.api`, `datomic.client.api`. The most important part of the configuration is `tiny-queue-processors`.
It should be a map with db idents as keys and functions as values.

## Examples

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


## License

Copyright © 2022 Spinney

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
