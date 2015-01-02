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

(ns backtype.storm.cluster
  (:import [backtype.storm.utils Utils])
  (:use [backtype.storm util log config]))


(defprotocol StormClusterState
  (assignments [this callback])
  (assignment-info [this storm-id callback])
  (active-storms [this])
  (storm-base [this storm-id callback])
  (activate-storm! [this storm-id storm-base])
  (update-storm! [this storm-id new-elems])
  (remove-storm-base! [this storm-id])
  (remove-storm! [this storm-id])
  (set-assignment! [this storm-id info]))


(defn mk-storm-cluster-state
  "Make state for one instance of a 'storm'."
  [conf]
  (let [
        assignment-info-callback (atom {})
        assignments-callback (atom nil)
        storm-base-callback (atom {})
        ;; store active storms in a map
        active-storms (atom {})
        assignments (atom {})
        state-id (uuid)]
    (reify
      StormClusterState

      (assignments
        [this callback]
        (when callback
          (reset! assignments-callback callback))
        (or (keys @assignments) []))

      (assignment-info
        [this storm-id callback]
        (when callback
          (swap! assignment-info-callback assoc storm-id callback))
        (@assignments storm-id))

      (active-storms
        [this]
        ;; Return storm ids that are running
        ;; or empty if none are running.
        (or (keys @active-storms) []))

      (activate-storm!
        [this storm-id storm-base]
        ; Map the storm id to StormBase in active map
        (swap! active-storms assoc storm-id storm-base))

      (update-storm!
        [this storm-id new-elems]
        ; Update storm with e.g. a new status.
        (let [base (storm-base this storm-id nil)
              executors (:component->executors base)
              ;; new-elems' :component->executors map is updated with merge
              new-elems (update new-elems :component->executors (partial merge executors))]
          (swap! active-storms assoc storm-id (-> base (merge new-elems)))))

      (storm-base
        [this storm-id callback]
        ; Retrieve StormBase obj and optionally set a callback
        ; which will be ran when change is made
        (when callback
          (swap! storm-base-callback assoc storm-id callback))
        (@active-storms storm-id))

      (remove-storm-base!
        [this storm-id]
        (swap! active-storms dissoc storm-id))

      (set-assignment!
        [this storm-id info]
        ;; associates an assignment with a topology
        (swap! assignments assoc storm-id info))

      (remove-storm!
        [this storm-id]
        ;; removes assignments and stormbase of a topology
        (swap! assignments dissoc storm-id)
        (remove-storm-base! this storm-id)))))