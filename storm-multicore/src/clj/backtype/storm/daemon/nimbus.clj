;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns backtype.storm.daemon.nimbus
  (:import [java.util Map])
  (:import [backtype.storm.daemon Shutdownable])
  (:import [backtype.storm.generated StormTopology InvalidTopologyException])
  (:import [backtype.storm.generated Nimbus Nimbus$Iface])
  (:import [backtype.storm.utils ThriftTopologyUtils])
  (:require [backtype.storm.daemon.worker :as worker])
  (:use [backtype.storm util log config])
  (:use [backtype.storm.daemon common])
  (:gen-class))


(defn- to-executor-id
  "Executor id is the first and last task it will run."
  [task-ids]
  [(first task-ids) (last task-ids)])


(defn- compute-executors
  "Creates executor ids. Return [[task-id1 task-id2] [task-id3 task-id4]]"
  [nimbus topology storm-conf]
  (let [conf (:conf nimbus)
        component->executors (->> (all-components topology) (map-val num-start-executors))
        task->component (storm-task-info topology storm-conf)]
    (->>
      ;; task-id -> component-id map
      task->component
      ;; component-id -> [task-id1, task-id2, ...] map
      reverse-map
      ;; sort by task-ids per component
      (map-val sort)
      ;; join this with what is in the base
      ;; so we get component-id -> (num-executors [task-id1, task-id2, ...])
      (join-maps component->executors)
      ;; split the list of tasks into 'num-executors' chunks of same size
      ;; component-id -> [(task-id1, task-id2), (task-id3, task-id4), ...]
      (map-val (partial apply partition-fixed))
      ;; now drop the component-id
      (mapcat second)
      ;; and convert each group of tasks into executor-id
      (map to-executor-id)
      )))


(defn normalize-conf [conf storm-conf ^StormTopology topology]
  ;; ensure that serializations are same for all tasks no matter what's on
  ;; the supervisors. this also allows you to declare the serializations as a sequence
  (let [component-confs (map
                          ; This is a bit unnecesary
                          #(-> (ThriftTopologyUtils/getComponentCommon topology %)
                             .getConf)
                          (ThriftTopologyUtils/getComponentIds topology))
        total-conf (merge conf storm-conf)

        get-merged-conf-val (fn [k merge-fn]
                              (merge-fn
                                (concat
                                  (mapcat #(get % k) component-confs)
                                  (or (get storm-conf k)
                                    (get conf k)))))]
    ;; append component conf to storm-conf
    (merge storm-conf
      {TOPOLOGY-MAX-TASK-PARALLELISM (total-conf TOPOLOGY-MAX-TASK-PARALLELISM)})))


(defn- component-parallelism
  "Number of parallel executions of a component."
  [storm-conf component]
  (let [storm-conf (merge storm-conf (component-conf component))
        ; take conf if set or parallelismHint otherwise
        num-tasks (or (storm-conf TOPOLOGY-TASKS) (num-start-executors component))
        ; take max parallelism from config
        max-parallelism (storm-conf TOPOLOGY-MAX-TASK-PARALLELISM)
        ]
    ; return max if num-tasks is more than max
    (if max-parallelism
      (min max-parallelism num-tasks)
      num-tasks)))


;; This is mutable for now, until I figure out how to do it immutably.
(defn normalize-topology!
  "Normalizes a topology."
  [storm-conf ^StormTopology topology]
  ;; Make sure that StormTopology const below makes copy.
  (doseq [[_ component] (all-components topology)]
    (.setConf
      (.getCommon component)
      ;; this shit updates the conf of a component with {TOPOLOGY-TASKS = max(TOPOLOGY-MAX-TASK-PARALLELISM, num-tasks)
      (merge (component-conf component) {TOPOLOGY-TASKS (component-parallelism storm-conf component)})))
  topology)


(def DISALLOWED-TOPOLOGY-NAME-STRS #{"/" "." ":" "\\"})

(defn validate-topology-name!
  "Check that the name of the topology is valid."
  [name]
  (if (some #(.contains name %) DISALLOWED-TOPOLOGY-NAME-STRS)
    (throw (InvalidTopologyException.
             (str "Topology name cannot contain any of the following: " (pr-str DISALLOWED-TOPOLOGY-NAME-STRS))))
    (if (clojure.string/blank? name)
      (throw (InvalidTopologyException.
               ("Topology name cannot be blank"))))))


(defn nimbus-data
  "Map containing Nimbus data."
  [conf]
  {:conf conf
   :uptime (uptime-computer)
   ; by default this is DefaultTopologyValidator
   :validator (new-instance (conf NIMBUS-TOPOLOGY-VALIDATOR))})


(defserverfn service-handler
  [conf]
  (log-message "Starting Nimbus with conf " conf)
  (let [nimbus (nimbus-data conf)
        workers (atom [])
        shutdown* (fn []
                    (log-message "Shutting down master")
                    (doseq [worker @workers] (.shutdown worker))
                    (log-message "Shut down master"))]
    ; prepare the validator
    (.prepare ^backtype.storm.nimbus.ITopologyValidator (:validator nimbus) conf)
    (reify Nimbus$Iface
      (^void submitTopology
        [this ^String storm-name ^Map topo-conf ^StormTopology topology]
        (let [topo-conf (clojurify-structure topo-conf)]
          (try
            (validate-topology-name! storm-name)
            (try
              (validate-configs-with-schemas topo-conf)
              (catch IllegalArgumentException ex
                (throw (InvalidTopologyException. (.getMessage ex)))))
            (.validate ^backtype.storm.nimbus.ITopologyValidator (:validator nimbus)
              storm-name
              topo-conf
              topology)
            ; here we create a "unique" storm-id, set it up and start it, also makes assignments
            (let [storm-id (str storm-name "-" (current-time-secs))
                  storm-conf (normalize-conf
                               conf
                               (-> topo-conf
                                 (assoc STORM-ID storm-id)
                                 (assoc TOPOLOGY-NAME storm-name))
                               topology)
                  total-storm-conf (merge conf storm-conf)
                  topology (normalize-topology! total-storm-conf topology)]
              (system-topology! total-storm-conf topology) ;; this validates the structure of the topology
              (log-message "Received topology submission for " storm-name " with conf " storm-conf)
              (let [executors (compute-executors nimbus topology total-storm-conf)
                    worker (worker/mk-worker conf total-storm-conf storm-id topology executors)]
                (swap! workers conj worker)))
            (catch Throwable e
              (log-warn-error e "Topology submission exception. (topology name='" storm-name "')")
              (throw e)))))

      (^void killTopology [this ^String name]
        (shutdown*))

      (^Map getNimbusConf [this]
        (:conf nimbus))

      Shutdownable
      (shutdown [this]
        (shutdown*))

      DaemonCommon
      (waiting? [this]
        ))))