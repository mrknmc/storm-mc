(ns
  ^{:author mark}
  backtype.storm.nimbus-test
  (:use [clojure test])
  (:use [backtype.storm config cluster testing])
  (:use [backtype.storm.daemon nimbus])
  (:import [backtype.storm.daemon.common StormBase]))


(defn mk-inimbus
  []
  (nimbus-data (read-storm-config) (standalone-nimbus)))


(deftest test-nimbus-data
  ;; Test that nimbus-data returns expected map.
  (with-local-cluster [cluster]
    (let [data (nimbus-data (cluster :daemon-conf)
                              (cluster :nimbus))]
      (is (= (data :conf) (cluster :daemon-conf)))
      (is (= (data :inimbus) (cluster :nimbus)))
      (is (= @(data :submitted-count) 0))
      )))


(deftest test-set-topology-status
  ;; Test that setting a status for a topo works.
  (let [nimbus (mk-inimbus)
        state (:storm-cluster-state nimbus)
        base1 (StormBase. "/tmp/storm1" 1 {:type :active} 2 {})]
    (.activate-storm! state "storm1" base1)

    (set-topology-status! nimbus "storm1" :active)
    (is (= (topology-status nimbus "storm1") :active))

    (set-topology-status! nimbus "storm1" :inactive)
    (is (= (topology-status nimbus "storm1") :inactive))
    ))


(deftest test-get-storm-id
  ;; Test that getting an id works.
  (with-local-cluster [cluster]
    (let [state (mk-storm-cluster-state (read-storm-config))
          base1 (StormBase. "/tmp/storm1" 1 {:type :active} 2 {})]
      (.activate-storm! state "storm1" base1)
      (is (= (get-storm-id state "/tmp/storm1") "storm1"))
      )))


(deftest test-check-storm-active
  ;; Test that checking whether a topo is active works.
  (let [nimbus (mk-inimbus)
        state (:storm-cluster-state nimbus)
        base1 (StormBase. "/tmp/storm1" 1 {:type :active} 2 {})]

    ;; check exception not thrown
    (is (= nil (check-storm-active! nimbus "/tmp/storm1" false)))

    (.activate-storm! state "storm1" base1)
    ;; check exception not thrown
    (is (= nil (check-storm-active! nimbus "/tmp/storm1" true)) (topology-status nimbus "storm1"))
    ))


;(deftest test-normalize-conf
;  ;; Test whether normalizing conf works
;  (let [normed-conf (normalize-conf (read-storm-config) topology)]
;    )
;  )
