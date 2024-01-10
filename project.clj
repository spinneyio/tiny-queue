(defproject io.spinney/tiny-queue "1.0.15"
  :description "A Clojure implementation of message queue that is based on datomic."
  :url "https://github.com/spinneyio/tiny-queue"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/spec.alpha "0.3.218"]
                 [org.clojure/math.numeric-tower "0.0.5"]
                 [org.clojure/test.check "1.1.1"]
                 [clj-time "0.15.2"]]
  :profiles {:test {:dependencies [[com.datomic/datomic-free "0.9.5697"]]}}
  :repl-options {:init-ns tiny-queue.core})
