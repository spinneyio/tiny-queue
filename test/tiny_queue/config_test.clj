(ns tiny-queue.config-test
  (:require [clojure.test :refer [deftest testing is]]
            [tiny-queue.config :as config]))

(deftest check-config
  (testing "Check valid config without optional fields."
    (let [validated-config (config/check-config
                            {:conn "some-connection"
                             :db (constantly nil)
                             :q (constantly nil)
                             :transact (constantly nil)
                             :tiny-queue-processors {}})]
      (is (:validated? validated-config))
      (is (:processor-uuid validated-config))
      (is (:job-processor-failed-interval-in-s validated-config))
      (is (:max-process-job-time-in-s validated-config))
      (is (:original-interval-in-ns validated-config))
      (is (:log validated-config))))

  (testing "Check valid config with optional fields."
    (let [validated-config (config/check-config
                            {:conn "some-connection"
                             :db (constantly nil)
                             :q (constantly nil)
                             :transact (constantly nil)
                             :tiny-queue-processors {}
                             :processor-uuid (str (java.util.UUID/randomUUID))
                             :job-processor-failed-interval-in-s 5
                             :max-process-job-time-in-s 13
                             :original-interval-in-ns 17
                             :log println})]
      (is (:validated? validated-config))
      (is (:processor-uuid validated-config))
      (is (:job-processor-failed-interval-in-s validated-config))
      (is (:max-process-job-time-in-s validated-config))
      (is (:original-interval-in-ns validated-config))
      (is (:log validated-config))))

  (testing "Check invalid config."
    (let [validated-config (try
                             (config/check-config
                              {:conn "some-connection"
                               :db (constantly nil)
                               :q (constantly nil)
                               :transact (constantly nil)
                               :tiny-queue-processors 5})
                             (catch Exception _ nil))]
      (is (nil? validated-config)))))