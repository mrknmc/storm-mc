(ns
  ^{:author mark}
  backtype.storm.cluster-test
  (:use [clojure test])
  (:use [backtype.storm config cluster testing])
  (:import [backtype.storm.daemon.common StormBase]))


(deftest test-storm-cluster-state-basics
  (let [state (mk-storm-cluster-state (read-storm-config))
;        assignment1 (Assignment. "/aaa" {} {1 [2 2002 1]} {})
;        assignment2 (Assignment. "/aaa" {} {1 [2 2002]} {})
        base1 (StormBase. "/tmp/storm1" 1 {:type :active} 2 {})
        base2 (StormBase. "/tmp/storm2" 2 {:type :active} 2 {})]
;    (is (= [] (.assignments state nil)))
;    (.set-assignment! state "storm1" assignment1)
;    (is (= assignment1 (.assignment-info state "storm1" nil)))
;    (is (= nil (.assignment-info state "storm3" nil)))
;    (.set-assignment! state "storm1" assignment2)
;    (.set-assignment! state "storm3" assignment1)
;    (is (= #{"storm1" "storm3"} (set (.assignments state nil))))
;    (is (= assignment2 (.assignment-info state "storm1" nil)))
;    (is (= assignment1 (.assignment-info state "storm3" nil)))

    (is (= [] (.active-storms state)))
    (.activate-storm! state "storm1" base1)
    (is (= ["storm1"] (.active-storms state)))
    (is (= base1 (.storm-base state "storm1" nil)))
    (is (= nil (.storm-base state "storm2" nil)))
    (.activate-storm! state "storm2" base2)
    (is (= base1 (.storm-base state "storm1" nil)))
    (is (= base2 (.storm-base state "storm2" nil)))
    (is (= #{"storm1" "storm2"} (set (.active-storms state))))
    (.remove-storm-base! state "storm1")
    (is (= base2 (.storm-base state "storm2" nil)))
    (is (= #{"storm2"} (set (.active-storms state))))
    ))
