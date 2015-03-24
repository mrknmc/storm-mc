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
(ns backtype.storm.scheduler.EvenScheduler
  (:use [backtype.storm util log config])
  (:require [clojure.set :as set])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot ExecutorDetails])
  (:gen-class
    :implements [backtype.storm.scheduler.IScheduler]))


(defn -prepare [this conf]
  )


(defn schedule-topology [^TopologyDetails topology ^Cluster cluster]
  (let [topology-id (.getId topology)
        available-slots (->> (.getAvailableSlots cluster) (map #(.getUUID %)))
        all-executors (->> topology
                        .getExecutors
                        (map #(vector (.getStartTask %) (.getEndTask %)))
                        set)
        total-slots-to-use (min (.getNumWorkers topology) (count available-slots))
        reassign-slots (take total-slots-to-use available-slots)
        reassignment (into {}
                       (map vector
                         all-executors
                         ;; for some reason it goes into infinite loop without limiting the repeat-seq
                         (repeat-seq (count all-executors) reassign-slots)))
        slot-uuid->executors (reverse-map reassignment)]
    (doseq [[slot-uuid executors] slot-uuid->executors
            :let [slot (WorkerSlot.)
                   executors (for [[start-task end-task] executors]
                              (ExecutorDetails. start-task end-task))]]
      (.assign cluster slot topology-id executors))
    (when-not (empty? reassignment)
      (log-message "Available slots: " (pr-str available-slots))
      )
    reassignment))


(defn -schedule [this ^TopologyDetails topology ^Cluster cluster]
  (schedule-topology topology cluster))
