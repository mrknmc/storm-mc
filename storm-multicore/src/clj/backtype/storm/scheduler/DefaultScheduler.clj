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
(ns backtype.storm.scheduler.DefaultScheduler
  (:use [backtype.storm util config])
  (:require [backtype.storm.scheduler.EvenScheduler :as EvenScheduler])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot SchedulerAssignment
            EvenScheduler ExecutorDetails]
           [backtype.storm.generated StormTopology])
  (:gen-class
    :implements [backtype.storm.scheduler.IScheduler]))


(defn -prepare [this conf]
  )


(defn default-schedule [^TopologyDetails topology ^Cluster cluster]
  (EvenScheduler/schedule-topology topology cluster))


(defn -schedule [this ^TopologyDetails topology ^Cluster cluster]
  (default-schedule topology cluster))
