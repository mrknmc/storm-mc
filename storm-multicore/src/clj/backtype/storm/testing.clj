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

(ns backtype.storm.testing
;  (:require [backtype.storm.daemon.nimbus :as nimbus])
  (:use [backtype.storm config]))


;(defnk mk-local-storm-cluster
;  "Reads the daemon config, creates a nimbus thread."
;  [:daemon-conf {} :inimbus nil]
;  (let [daemon-conf (merge (read-storm-config)
;                           {TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS false
;                            TOPOLOGY-TRIDENT-BATCH-EMIT-INTERVAL-MILLIS 50}
;                           daemon-conf)
;        nimbus (nimbus/service-handler
;                 daemon-conf
;                 (if inimbus inimbus (nimbus/standalone-nimbus)))
;        storm-map {:nimbus nimbus
;                   :daemon-conf daemon-conf}]
;    storm-map))
