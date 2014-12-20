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


(defn- issue-callback!
  "Set callback to null and execute it if was not null."
  [cb-atom]
  (let [cb @cb-atom]
    (reset! cb-atom nil)
    (when cb
      (cb))))


(defn- issue-map-callback!
  "Remove callback from a map and execute it if not null."
  [cb-atom id]
  (let [cb (@cb-atom id)]
    (swap! cb-atom dissoc id)
    (when cb
      (cb id))))


(defn mk-storm-cluster-state
  "Make state for one instance of a 'storm'."
  [conf]
  (let [
        assignment-info-callback (atom {})
        supervisors-callback (atom nil)
        assignments-callback (atom nil)
        assignments (atom {})
        storm-base-callback (atom {})
        ;; store active storms in a set
        active-storms-map (atom {})
        state-id (uuid)]
    (reify
      StormClusterState

      (assignments
        [this callback]
        (when callback
          (reset! assignments-callback callback))
        (or (keys @assignments) []))
;        (get-children cluster-state ASSIGNMENTS-SUBTREE (not-nil? callback)))

      (assignment-info
        [this storm-id callback]
        (when callback
          (swap! assignment-info-callback assoc storm-id callback))
        (@assignments storm-id))
;        (maybe-deserialize (get-data cluster-state (assignment-path storm-id) (not-nil? callback))))

      (active-storms
        [this]
        ;; Return storm ids that are running
        ;; or empty if none are running.
        (or (keys @active-storms-map) []))

      (activate-storm!
        [this storm-id storm-base]
        ; Map the storm id to StormBase in active map
        (swap! active-storms-map assoc storm-id storm-base))

      (update-storm!
        [this storm-id new-elems]
        ; Update storm with e.g. a new status.
        (let [base (storm-base this storm-id nil)
              executors (:component->executors base)
              ;; new-elems' :component->executors map is updated with merge
              new-elems (update new-elems :component->executors (partial merge executors))]
          (swap! active-storms-map assoc storm-id (-> base (merge new-elems)))))

      (storm-base
        [this storm-id callback]
        ; Retrieve StormBase obj and optionally set a callback
        ; which will be ran when change is made
        (when callback
          (swap! storm-base-callback assoc storm-id callback))
        (@active-storms-map storm-id))

      (remove-storm-base!
        [this storm-id]
        (swap! active-storms-map dissoc storm-id))
;        (delete-node cluster-state (storm-path storm-id)))

      (set-assignment!
        [this storm-id info]
        (swap! assignments assoc storm-id info))
;        (set-data cluster-state (assignment-path storm-id) (Utils/serialize info)))

      (remove-storm!
        [this storm-id]
        (swap! assignments dissoc storm-id)
;        (delete-node cluster-state (assignment-path storm-id))
        (remove-storm-base! this storm-id)))))


;; daemons have a single thread that will respond to events
;; start with initialize event
;; callbacks add events to the thread's queue

;; keeps in memory cache of the state, only for what client subscribes to. Any subscription is automatically kept in sync, and when there are changes, client is notified.
;; master gives orders through state, and client records status in state (ephemerally)

;; master tells nodes what workers to launch

;; master writes this. supervisors and workers subscribe to this to understand complete topology. each storm is a map from nodes to workers to tasks to ports whenever topology changes everyone will be notified
;; master includes timestamp of each assignment so that appropriate time can be given to each worker to start up
;; /assignments/{storm id}

;; which tasks they talk to, etc. (immutable until shutdown)
;; everyone reads this in full to understand structure
;; /tasks/{storm id}/{task id} ; just contains bolt id

;; supervisors send heartbeats here, master doesn't subscribe but checks asynchronously
;; /supervisors/status/{ephemeral node ids}  ;; node metadata such as port ranges are kept here

;; tasks send heartbeats here, master doesn't subscribe, just checks asynchronously
;; /taskbeats/{storm id}/{ephemeral task id}

;; contains data about whether it's started or not, tasks and workers subscribe to specific storm here to know when to shutdown
;; master manipulates
;; /storms/{storm id}

;; Zookeeper flows:

;; Master:
;; job submit:
;; 1. read which nodes are available
;; 2. set up the worker/{storm}/{task} stuff (static)
;; 3. set assignments
;; 4. start storm - necessary in case master goes down, when goes back up can remember to take down the storm (2 states: on or off)

;; Monitoring (or by checking when nodes go down or heartbeats aren't received):
;; 1. read assignment
;; 2. see which tasks/nodes are up
;; 3. make new assignment to fix any problems
;; 4. if a storm exists but is not taken down fully, ensure that storm takedown is launched (step by step remove tasks and finally remove assignments)

;; masters only possible watches is on ephemeral nodes and tasks, and maybe not even

;; Supervisor:
;; 1. monitor /storms/* and assignments
;; 2. local state about which workers are local
;; 3. when storm is on, check that workers are running locally & start/kill if different than assignments
;; 4. when storm is off, monitor tasks for workers - when they all die or don't hearbeat, kill the process and cleanup

;; Worker:
;; 1. On startup, start the tasks if the storm is on

;; Task:
;; 1. monitor assignments, reroute when assignments change
;; 2. monitor storm (when storm turns off, error if assignments change) - take down tasks as master turns them off

;; locally on supervisor: workers write pids locally on startup, supervisor deletes it on shutdown (associates pid with worker name)
;; supervisor periodically checks to make sure processes are alive
;; {rootdir}/workers/{storm id}/{worker id}   ;; contains pid inside

;; all tasks in a worker share the same cluster state
;; workers, supervisors, and tasks subscribes to storm to know when it's started or stopped
;; on stopped, master removes records in order (tasks need to subscribe to themselves to see if they disappear)
;; when a master removes a worker, the supervisor should kill it (and escalate to kill -9)
;; on shutdown, tasks subscribe to tasks that send data to them to wait for them to die. when node disappears, they can die
