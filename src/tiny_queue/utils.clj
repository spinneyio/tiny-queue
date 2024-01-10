(ns tiny-queue.utils
  (:require [clj-time.coerce :as c]
            [clojure.math.numeric-tower :as math]
            [clj-time.core :as time]))

(defn to-database-date [date]
  (if (instance? java.util.Date date)
    date
    (c/to-date date)))

(defn get-retry-date [fail-time time-increment backoff-factor]
  (to-database-date
   (time/plus
    fail-time
    (time/seconds
     (* time-increment (math/expt 2 backoff-factor))))))

(defn result->string [result]
  (if (string? result)
    result
    (prn-str result)))

(defn stacktrace [e]
  (let [sw (java.io.StringWriter.)
        pw (java.io.PrintWriter. sw)]
    (.printStackTrace e pw)
    (.toString sw)))

(defn exception-description [e]
  (str (class e) " received in: " (stacktrace e) ": " (.getMessage e)))

(defmacro with-timeout [millis & body]
    `(let [future# (future ~@body)]
      (try
        (.get future# ~millis java.util.concurrent.TimeUnit/MILLISECONDS)
        (catch java.util.concurrent.TimeoutException x# 
          (do
            (future-cancel future#)
            nil)))))
