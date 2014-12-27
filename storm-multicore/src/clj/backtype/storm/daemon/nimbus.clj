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
;  (:import [org.apache.thrift.server THsHaServer THsHaServer$Args])
;  (:import [org.apache.thrift.protocol TBinaryProtocol TBinaryProtocol$Factory])
;  (:import [org.apache.thrift.exception])
;  (:import [org.apache.thrift.transport TNonblockingServerTransport TNonblockingServerSocket])
  (:import [org.apache.commons.io FileUtils])
  (:import [java.nio ByteBuffer])
  (:import [java.util Map])
  (:import [java.io File FileNotFoundException])
  (:import [java.nio.channels Channels WritableByteChannel])
;  (:use [backtype.storm.scheduler.DefaultScheduler])
  (:require [backtype.storm.cluster :as cluster])
  (:import [backtype.storm.daemon Shutdownable])
  (:import [backtype.storm.generated StormTopology
            NotAliveException AlreadyAliveException
            InvalidTopologyException SubmitOptions
            SubmitOptions$TopologyInitialStatus KillOptions])
  (:import [backtype.storm.scheduler INimbus])
  (:import [backtype.storm.generated Nimbus Nimbus$Iface])
  (:import [backtype.storm.utils Utils ThriftTopologyUtils])
  (:import [backtype.storm.daemon.common StormBase])
  (:import [backtype.storm.scheduler Topologies TopologyDetails ExecutorDetails])
;  (:import [backtype.storm.scheduler INimbus SupervisorDetails WorkerSlot TopologyDetails
;            Cluster Topologies SchedulerAssignment SchedulerAssignmentImpl DefaultScheduler ExecutorDetails])
  (:use [backtype.storm util timer log config])
;  (:use [backtype.storm bootstrap util timer config])
;  (:use [backtype.storm.config :only [validate-configs-with-schemas]])
  (:use [backtype.storm.daemon common])
  (:gen-class
   :methods [^{:static true} [launch [backtype.storm.scheduler.INimbus] void]]))

;(bootstrap)
;
;(defn file-cache-map [conf]
;  (TimeCacheMap.
;    (int (conf NIMBUS-FILE-COPY-EXPIRATION-SECS))
;    (reify TimeCacheMap$ExpiredCallback
;      (expire [this id stream]
;        (.close stream)
;        ))
;    ))
;
;
;
;(declare delay-event)
;(declare mk-assignments)
;
;
;;; active -> reassign in X secs
;
;;; killed -> wait kill time then shutdown
;;; active -> reassign in X secs
;;; inactive -> nothing
;;; rebalance -> wait X seconds then rebalance
;;; swap... (need to handle kill during swap, etc.)
;;; event transitions are delayed by timer... anything else that comes through (e.g. a kill) override the transition? or just disable other transitions during the transition?
;
;
;(defmulti setup-jar cluster-mode)
;(defmulti clean-inbox cluster-mode)
;
;;; swapping design
;;; -- need 2 ports per worker (swap port and regular port)
;;; -- topology that swaps in can use all the existing topologies swap ports, + unused worker slots
;;; -- how to define worker resources? port range + number of workers?
;
;
;;; Monitoring (or by checking when nodes go down or heartbeats aren't received):
;;; 1. read assignment
;;; 2. see which executors/nodes are up
;;; 3. make new assignment to fix any problems
;;; 4. if a storm exists but is not taken down fully, ensure that storm takedown is launched (step by step remove executors and finally remove assignments)
;
;(defn- assigned-slots
;  "Returns a map from node-id to a set of ports"
;  [storm-cluster-state]
;  (let [assignments (.assignments storm-cluster-state nil)
;        ]
;    (defaulted
;      (apply merge-with set/union
;                        (for [a assignments
;                              [_ [node port]] (-> (.assignment-info storm-cluster-state a nil) :executor->node+port)]
;                          {node #{port}}
;                          ))
;      {})
;    ))
;


(defn- all-supervisor-info
  "Supervisor infos from cluster state."
  ([storm-cluster-state] (all-supervisor-info storm-cluster-state nil))
  ([storm-cluster-state callback]
   (let [supervisor-ids (.supervisors storm-cluster-state callback)]
     (into {}
       (mapcat
         (fn [id]
           (if-let [info (.supervisor-info storm-cluster-state id)]
             [[id info]]
             ))
         supervisor-ids))
     )))


(defn- all-scheduling-slots
  "All slots (node-id, port) available for scheduling."
  [nimbus topologies missing-assignment-topologies]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        ^INimbus inimbus (:inimbus nimbus)

        supervisor-infos (all-supervisor-info storm-cluster-state nil)

        supervisor-details (dofor [[id info] supervisor-infos]
                             (SupervisorDetails. id (:meta info)))

        ret (.allSlotsAvailableForScheduling inimbus
              supervisor-details
              topologies
              (set missing-assignment-topologies)
              )
        ]
    (dofor [^WorkerSlot slot ret]
      [(.getNodeId slot) (.getPort slot)]
      )))
;
;
;(defn- compute-supervisor->dead-ports [nimbus existing-assignments topology->executors topology->alive-executors]
;  (let [dead-slots (into [] (for [[tid assignment] existing-assignments
;                                  :let [all-executors (topology->executors tid)
;                                        alive-executors (topology->alive-executors tid)
;                                        dead-executors (set/difference all-executors alive-executors)
;                                        dead-slots (->> (:executor->node+port assignment)
;                                                     (filter #(contains? dead-executors (first %)))
;                                                     vals)]]
;                              dead-slots))
;        supervisor->dead-ports (->> dead-slots
;                                 (apply concat)
;                                 (map (fn [[sid port]] {sid #{port}}))
;                                 (apply (partial merge-with set/union)))]
;    (or supervisor->dead-ports {})))
;
(defn- compute-topology->scheduler-assignment
  "Convert assignment information in zk to SchedulerAssignment, so it can be used by scheduler api."
  [nimbus existing-assignments topology->alive-executors]
  (into {} (for [[tid assignment] existing-assignments
                 ;; for each existing assignment, get the alive executors
                 :let [alive-executors (topology->alive-executors tid)
                       ;; get a mapping from an executor to a node+port
                       executor->node+port (:executor->node+port assignment)
                       ;; and a mapping from ExecutorDetails to WorkerSlots
                       executor->slot (into {} (for [[executor [node port]] executor->node+port]
                                                 ;; filter out the dead executors
                                                 (if (contains? alive-executors executor)
                                                   {(ExecutorDetails. (first executor)
                                                      (second executor))
                                                    (WorkerSlot. node port)}
                                                   {})))]]
             {tid (SchedulerAssignmentImpl. tid executor->slot)})))


(defn- read-all-supervisor-details [nimbus all-scheduling-slots supervisor->dead-ports]
  "return a map: {topology-id SupervisorDetails}"
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        supervisor-infos (all-supervisor-info storm-cluster-state)
        nonexistent-supervisor-slots (apply dissoc all-scheduling-slots (keys supervisor-infos))
        all-supervisor-details (into {} (for [[sid supervisor-info] supervisor-infos
                                              :let [hostname (:hostname supervisor-info)
                                                    scheduler-meta (:scheduler-meta supervisor-info)
                                                    dead-ports (supervisor->dead-ports sid)
                                                    ;; hide the dead-ports from the all-ports
                                                    ;; these dead-ports can be reused in next round of assignments
                                                    all-ports (-> (get all-scheduling-slots sid)
                                                                (set/difference dead-ports)
                                                                ((fn [ports] (map int ports))))
                                                    supervisor-details (SupervisorDetails. sid hostname scheduler-meta all-ports)]]
                                          {sid supervisor-details}))]
    (merge all-supervisor-details
      (into {}
        (for [[sid ports] nonexistent-supervisor-slots]
          [sid (SupervisorDetails. sid nil ports)]))
      )))
;
;(defn- compute-topology->executor->node+port [scheduler-assignments]
;  "convert {topology-id -> SchedulerAssignment} to
;           {topology-id -> {executor [node port]}}"
;  (map-val (fn [^SchedulerAssignment assignment]
;             (->> assignment
;               .getExecutorToSlot
;               (#(into {} (for [[^ExecutorDetails executor ^WorkerSlot slot] %]
;                            {[(.getStartTask executor) (.getEndTask executor)]
;                             [(.getNodeId slot) (.getPort slot)]})))))
;    scheduler-assignments))
;
;;; NEW NOTES
;;; only assign to supervisors who are there and haven't timed out
;;; need to reassign workers with executors that have timed out (will this make it brittle?)
;;; need to read in the topology and storm-conf from disk
;;; if no slots available and no slots used by this storm, just skip and do nothing
;;; otherwise, package rest of executors into available slots (up to how much it needs)
;
;;; in the future could allocate executors intelligently (so that "close" tasks reside on same machine)
;
;;; TODO: slots that have dead executor should be reused as long as supervisor is active
;
;
;;; (defn- assigned-slots-from-scheduler-assignments [topology->assignment]
;;;   (->> topology->assignment
;;;        vals
;;;        (map (fn [^SchedulerAssignment a] (.getExecutorToSlot a)))
;;;        (mapcat vals)
;;;        (map (fn [^WorkerSlot s] {(.getNodeId s) #{(.getPort s)}}))
;;;        (apply merge-with set/union)
;;;        ))
;
;(defn num-used-workers [^SchedulerAssignment scheduler-assignment]
;  (if scheduler-assignment
;    (count (.getSlots scheduler-assignment))
;    0 ))
;
;
;(defn changed-executors [executor->node+port new-executor->node+port]
;  (let [slot-assigned (reverse-map executor->node+port)
;        new-slot-assigned (reverse-map new-executor->node+port)
;        brand-new-slots (map-diff slot-assigned new-slot-assigned)]
;    (apply concat (vals brand-new-slots))
;    ))
;
;(defn newly-added-slots [existing-assignment new-assignment]
;  (let [old-slots (-> (:executor->node+port existing-assignment)
;                    vals
;                    set)
;        new-slots (-> (:executor->node+port new-assignment)
;                    vals
;                    set)]
;    (set/difference new-slots old-slots)))
;
;
;(defn basic-supervisor-details-map [storm-cluster-state]
;  (let [infos (all-supervisor-info storm-cluster-state)]
;    (->> infos
;      (map (fn [[id info]]
;             [id (SupervisorDetails. id (:hostname info) (:scheduler-meta info) nil)]))
;      (into {}))))
;
;(defn- to-worker-slot [[node port]]
;  (WorkerSlot. node port))
;

;; Master:
;; job submit:
;; 1. read which nodes are available
;; 2. set assignments
;; 3. start storm - necessary in case master goes down, when goes back up can remember to take down the storm (2 states: on or off)
;
;
;
;(defn extract-status-str [base]
;  (let [t (-> base :status :type)]
;    (.toUpperCase (name t))
;    ))
;
;
;; Not sure what significance errors have, this can probably go
;(defn- get-errors [storm-cluster-state storm-id component-id]
;  (->> (.errors storm-cluster-state storm-id component-id)
;    (map #(ErrorInfo. (:error %) (:time-secs %)))))


(defn- setup-storm-code
  "Create files for storm and topology conf."
  [conf storm-id storm-conf topology]
  (let [stormroot (master-stormdist-root conf storm-id)]
    (FileUtils/forceMkdir (File. stormroot))
    (FileUtils/cleanDirectory (File. stormroot))
    ;; no need for this locally
    ;; (setup-jar conf tmp-jar-location stormroot)
    (FileUtils/writeByteArrayToFile (File. (master-stormcode-path stormroot)) (Utils/serialize topology))
    (FileUtils/writeByteArrayToFile (File. (master-stormconf-path stormroot)) (Utils/serialize storm-conf))
    ))


(defn- read-storm-conf
  "Read the storm config file."
  [conf storm-id]
  (let [stormroot (master-stormdist-root conf storm-id)]
    (merge conf
      (Utils/deserialize
        (FileUtils/readFileToByteArray
          (File. (master-stormconf-path stormroot))
          )))))


(defn- read-storm-topology
  "Read the topology config file."
  [conf storm-id]
  (let [stormroot (master-stormdist-root conf storm-id)]
    (Utils/deserialize
      (FileUtils/readFileToByteArray
        (File. (master-stormcode-path stormroot))
        ))))


(defn- try-read-storm-conf [conf storm-id]
  "Try to read storm conf from conf."
  (try-cause
    (read-storm-conf conf storm-id)
    (catch FileNotFoundException e
      (throw (NotAliveException. storm-id)))
    )
  )


(defn- try-read-storm-topology [conf storm-id]
  "Try to read the topology from conf."
  (try-cause
    (read-storm-topology conf storm-id)
    (catch FileNotFoundException e
      (throw (NotAliveException. storm-id)))
    )
  )


;; declared here because of cyclic dependency
(declare compute-executor->component)


(defn read-topology-details
  "Create TopologyDetails from the topology file."
  [nimbus storm-id]
  (let [conf (:conf nimbus)
        storm-base (.storm-base (:storm-cluster-state nimbus) storm-id nil)
        topology-conf (read-storm-conf conf storm-id)
        topology (read-storm-topology conf storm-id)
        executor->component (->> (compute-executor->component nimbus storm-id)
                              (map-key (fn [[start-task end-task]]
                                         (ExecutorDetails. (int start-task) (int end-task)))))]
    (TopologyDetails. storm-id
      topology-conf
      topology
      (:num-workers storm-base)
      executor->component
      )))


(defn- to-executor-id
  "Executor id is the first and last task it will run."
  [task-ids]
  [(first task-ids) (last task-ids)])


(defn- compute-executors
  "Creates executor ids."
  [nimbus storm-id]
  (let [conf (:conf nimbus)
        storm-base (.storm-base (:storm-cluster-state nimbus) storm-id nil)
        component->executors (:component->executors storm-base)
        storm-conf (read-storm-conf conf storm-id)
        topology (read-storm-topology conf storm-id)
        task->component (storm-task-info topology storm-conf)]
    ;; TODO: this thing below is just repeated right?
    (->>
      ;; task-id -> component-id map
      (storm-task-info topology storm-conf)
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


(defn- compute-executor->component
  "Compute executor-id -> component map."
  [nimbus storm-id]
  (let [conf (:conf nimbus)
        executors (compute-executors nimbus storm-id)
        topology (read-storm-topology conf storm-id)
        storm-conf (read-storm-conf conf storm-id)
        task->component (storm-task-info topology storm-conf)
        executor->component (into {} (for [executor executors
                                           :let [start-task (first executor)
                                                 component (task->component start-task)]]
                                       {executor component}))]
    executor->component))


(defn- compute-topology->executors
  "Compute a topology-id -> executors map."
  [nimbus storm-ids]
  (into {} (for [tid storm-ids]
             {tid (set (compute-executors nimbus tid))})))


(defn- alive-executors
  "Checks which executors are alive based on hearbeats cache."
  [nimbus ^TopologyDetails topology-details all-executors existing-assignment]
  (log-debug "Computing alive executors for " (.getId topology-details) "\n"
                                              "Executors: " (pr-str all-executors) "\n"
                                              "Assignment: " (pr-str existing-assignment) "\n"
                                              "Heartbeat cache: " (pr-str (@(:heartbeats-cache nimbus) (.getId topology-details)))
                                              )
  ;; TODO: need to consider all executors associated with a dead executor (in same slot) dead as well,
  ;; don't just rely on heartbeat being the same
  (let [conf (:conf nimbus)
        storm-id (.getId topology-details)
        executor-start-times (:executor->start-time-secs existing-assignment)
        heartbeats-cache (@(:heartbeats-cache nimbus) storm-id)]
    (->> all-executors
      (filter (fn [executor]
                (let [start-time (get executor-start-times executor)
                      is-timed-out (-> heartbeats-cache (get executor) :is-timed-out)]
                  (if (and start-time
                        (or
                          (< (time-delta start-time)
                            (conf NIMBUS-TASK-LAUNCH-SECS))
                          (not is-timed-out)
                          ))
                    true
                    (do
                      (log-message "Executor " storm-id ":" executor " not alive")
                      false))
                  )))
      doall)))


(defn- compute-topology->alive-executors
  "Compute a topology-id -> alive executors map."
  [nimbus existing-assignments topologies topology->executors scratch-topology-id]
  (into {} (for [[tid assignment] existing-assignments
                 :let [topology-details (.getById topologies tid)
                       all-executors (topology->executors tid)
                       alive-executors (if (and scratch-topology-id (= scratch-topology-id tid))
                                         all-executors
                                         (set (alive-executors nimbus topology-details all-executors assignment)))]]
             {tid alive-executors})))


;;; public so it can be mocked out
(defn compute-new-topology->executor->node+port [nimbus existing-assignments topologies scratch-topology-id]
  (let [conf (:conf nimbus)
        storm-cluster-state (:storm-cluster-state nimbus)
        ;; in local environment topology->executors == topology->alive-executors
        topology->executors (compute-topology->executors nimbus (keys existing-assignments))
        topology->alive-executors topology->executors
;        topology->alive-executors (compute-topology->alive-executors nimbus
;                                    existing-assignments
;                                    topologies
;                                    topology->executors
;                                    scratch-topology-id)
        ;; in local environment there are no dead ports or supervisors
;        supervisor->dead-ports (compute-supervisor->dead-ports nimbus
;                                 existing-assignments
;                                 topology->executors
;                                 topology->alive-executors)
        topology->scheduler-assignment (compute-topology->scheduler-assignment nimbus
                                         existing-assignments
                                         topology->alive-executors)

;        missing-assignment-topologies (->> topologies
;                                        .getTopologies
;                                        (map (memfn getId))
;                                        (filter (fn [t]
;                                                  (let [alle (get topology->executors t)
;                                                        alivee (get topology->alive-executors t)]
;                                                    (or (empty? alle)
;                                                      (not= alle alivee)
;                                                      (< (-> topology->scheduler-assignment
;                                                           (get t)
;                                                           num-used-workers )
;                                                        (-> topologies (.getById t) .getNumWorkers)
;                                                        ))
;                                                    ))))
        ;; create a map of available node-id -> #{port1, port2, ...}
;        all-scheduling-slots (->> (all-scheduling-slots nimbus topologies missing-assignment-topologies)
;                               (map (fn [[node-id port]] {node-id #{port}}))
;                               (apply merge-with set/union))

;        supervisors (read-all-supervisor-details nimbus all-scheduling-slots supervisor->dead-ports)
;        cluster (Cluster. (:inimbus nimbus) supervisors topology->scheduler-assignment)

        ;; call scheduler.schedule to schedule all the topologies
        ;; the new assignments for all the topologies are in the cluster object.
        _ (.schedule (:scheduler nimbus) topologies cluster)
        new-scheduler-assignments (.getAssignments cluster)
        ;; add more information to convert SchedulerAssignment to Assignment
        new-topology->executor->node+port (compute-topology->executor->node+port new-scheduler-assignments)]
    ;; print some useful information.
    (doseq [[topology-id executor->node+port] new-topology->executor->node+port
            :let [old-executor->node+port (-> topology-id
                                            existing-assignments
                                            :executor->node+port)
                  reassignment (filter (fn [[executor node+port]]
                                         (and (contains? old-executor->node+port executor)
                                           (not (= node+port (old-executor->node+port executor)))))
                                 executor->node+port)]]
      (when-not (empty? reassignment)
        (let [new-slots-cnt (count (set (vals executor->node+port)))
              reassign-executors (keys reassignment)]
          (log-message "Reassigning " topology-id " to " new-slots-cnt " slots")
          (log-message "Reassign executors: " (vec reassign-executors)))))

    new-topology->executor->node+port))


;; get existing assignment (just the executor->node+port map) -> default to {}
;; filter out ones which have a executor timeout
;; figure out available slots on cluster. add to that the used valid slots to get total slots. figure out how many executors should be in each slot (e.g., 4, 4, 4, 5)
;; only keep existing slots that satisfy one of those slots. for rest, reassign them across remaining slots
;; edge case for slots with no executor timeout but with supervisor timeout... just treat these as valid slots that can be reassigned to. worst comes to worse the executor will timeout and won't assign here next time around
(defnk mk-assignments [nimbus :scratch-topology-id nil]
  (let [conf (:conf nimbus)
        storm-cluster-state (:storm-cluster-state nimbus)
        ^INimbus inimbus (:inimbus nimbus)
        ;; read all the topologies
        topology-ids (.active-storms storm-cluster-state)
        topologies (into {} (for [tid topology-ids]
                              {tid (read-topology-details nimbus tid)}))
        topologies (Topologies. topologies)
        ;; read all the assignments
        assigned-topology-ids (.assignments storm-cluster-state nil)
        existing-assignments (into {} (for [tid assigned-topology-ids]
                                        ;; for the topology which wants rebalance (specified by the scratch-topology-id)
                                        ;; we exclude its assignment, meaning that all the slots occupied by its assignment
                                        ;; will be treated as free slot in the scheduler code.
                                        (when (or (nil? scratch-topology-id) (not= tid scratch-topology-id))
                                          {tid (.assignment-info storm-cluster-state tid nil)})))
        ;; make the new assignments for topologies
        topology->executor->node+port (compute-new-topology->executor->node+port
                                        nimbus
                                        existing-assignments
                                        topologies
                                        scratch-topology-id)


        now-secs (current-time-secs)

        basic-supervisor-details-map (basic-supervisor-details-map storm-cluster-state)

        ;; construct the final Assignments by adding start-times etc into it
        new-assignments (into {} (for [[topology-id executor->node+port] topology->executor->node+port
                                       :let [existing-assignment (get existing-assignments topology-id)
                                             all-nodes (->> executor->node+port vals (map first) set)
                                             node->host (->> all-nodes
                                                          (mapcat (fn [node]
                                                                    (if-let [host (.getHostName inimbus basic-supervisor-details-map node)]
                                                                      [[node host]]
                                                                      )))
                                                          (into {}))
                                             all-node->host (merge (:node->host existing-assignment) node->host)
                                             reassign-executors (changed-executors (:executor->node+port existing-assignment) executor->node+port)
                                             start-times (merge (:executor->start-time-secs existing-assignment)
                                                           (into {}
                                                             (for [id reassign-executors]
                                                               [id now-secs]
                                                               )))]]
                                   {topology-id (Assignment.
                                                  (master-stormdist-root conf topology-id)
                                                  (select-keys all-node->host all-nodes)
                                                  executor->node+port
                                                  start-times)}))]

    ;; tasks figure out what tasks to talk to by looking at topology at runtime
    ;; only log/set when there's been a change to the assignment
    (doseq [[topology-id assignment] new-assignments
            :let [existing-assignment (get existing-assignments topology-id)
                  topology-details (.getById topologies topology-id)]]
      (if (= existing-assignment assignment)
        (log-debug "Assignment for " topology-id " hasn't changed")
        (do
          (log-message "Setting new assignment for topology id " topology-id ": " (pr-str assignment))
          (.set-assignment! storm-cluster-state topology-id assignment)
          )))
    (->> new-assignments
      (map (fn [[topology-id assignment]]
             (let [existing-assignment (get existing-assignments topology-id)]
               [topology-id (map to-worker-slot (newly-added-slots existing-assignment assignment))]
               )))
      (into {})
      (.assignSlots inimbus topologies))
    ))


; commented out until I port Cluster, Topologies etc.
;(defn mk-scheduler
;  "Make a scheduler for a Nimbus obj and prepare it."
;  [conf inimbus]
;  (let [forced-scheduler (.getForcedScheduler inimbus)
;        scheduler (cond
;                    forced-scheduler
;                    (do (log-message "Using forced scheduler from INimbus " (class forced-scheduler))
;                      forced-scheduler)
;
;                    (conf STORM-SCHEDULER)
;                    (do (log-message "Using custom scheduler: " (conf STORM-SCHEDULER))
;                      (-> (conf STORM-SCHEDULER) new-instance))
;
;                    :else (do (log-message "Using default scheduler")
;                            (DefaultScheduler.)))]
;    (.prepare scheduler conf)
;    scheduler
;    ))


;; declared here because cyclic dependency
(declare delay-event)


(defn get-storm-id
  "Retrieve an id of a topology, given a name."
  [storm-cluster-state storm-name]
  (let [active-storms (.active-storms storm-cluster-state)]
    (find-first
      #(= storm-name (:storm-name (.storm-base storm-cluster-state % nil)))
      active-storms)
    ))


(defn do-rebalance
  "Re-makes assignments."
  [nimbus storm-id status]
  (.update-storm! (:storm-cluster-state nimbus)
    storm-id
    (assoc-non-nil
      {:component->executors (:executor-overrides status)}
      :num-workers (:num-workers status))))
;; commented out for now
;  (mk-assignments nimbus :scratch-topology-id storm-id))


(defn rebalance-transition
  "Produces a rebalance transition function."
  [nimbus storm-id status]
  (fn [time num-workers executor-overrides]
    (let [delay (if time
                  time
                  (get (read-storm-conf (:conf nimbus) storm-id)
                    TOPOLOGY-MESSAGE-TIMEOUT-SECS))]
      (delay-event nimbus
        storm-id
        delay
        :do-rebalance)
      {:type :rebalancing
       :delay-secs delay
       :old-status status
       :num-workers num-workers
       :executor-overrides executor-overrides
       })))


(defn kill-transition
  "Produces a kill transition function."
  [nimbus storm-id]
  (fn [kill-time]
    (let [delay (if kill-time
                  kill-time
                  ;; do we need to read the file from disk here?
                  (get (read-storm-conf (:conf nimbus) storm-id)
                    TOPOLOGY-MESSAGE-TIMEOUT-SECS))]
      (delay-event nimbus
        storm-id
        delay
        :remove)
      {:type :killed
       :kill-time-secs delay})
    ))


(defn topology-status
  "Get status of a topology."
  [nimbus storm-id]
  (-> nimbus :storm-cluster-state (.storm-base storm-id nil) :status))


(defn set-topology-status!
  "Set the status of a topology."
  [nimbus storm-id status]
  (let [storm-cluster-state (:storm-cluster-state nimbus)]
    (.update-storm! storm-cluster-state
      storm-id
      {:status status})
    (log-message "Updated " storm-id " with status " status)
    ))


(defn state-transitions
  "Map of states to a map of events to states."
  [nimbus storm-id status]
  {:active {:inactivate :inactive
            :activate nil
            :rebalance (rebalance-transition nimbus storm-id status)
            :kill (kill-transition nimbus storm-id)
            }
   :inactive {:activate :active
              :inactivate nil
              :rebalance (rebalance-transition nimbus storm-id status)
              :kill (kill-transition nimbus storm-id)
              }
   :killed {:startup (fn [] (delay-event nimbus
                              storm-id
                              (:kill-time-secs status)
                              :remove)
                       nil)
            :kill (kill-transition nimbus storm-id)
            :remove (fn []
                      (log-message "Killing topology: " storm-id)
                      (.remove-storm! (:storm-cluster-state nimbus)
                        storm-id)
                      nil)
            }
   :rebalancing {:startup (fn [] (delay-event nimbus
                                   storm-id
                                   (:delay-secs status)
                                   :do-rebalance)
                            nil)
                 :kill (kill-transition nimbus storm-id)
                 :do-rebalance (fn []
                                 (do-rebalance nimbus storm-id status)
                                 (:old-status status))
                 }})


(defn transition!
  "Transition Storm with storm-id to a state."
  ([nimbus storm-id event]
   (transition! nimbus storm-id event false))
  ([nimbus storm-id event error-on-no-transition?]
   ;; lock the submit-lock so only one submission at a time
   (locking (:submit-lock nimbus)
     (let [system-events #{:startup}
           ;; event can be a keyword (:startup) or a list/vector
           [event & event-args] (if (keyword? event) [event] event)
           status (topology-status nimbus storm-id)]
       (if-not status
         ;; handles the case where event was scheduled but topology has been removed
         (log-message "Cannot apply event " event " to " storm-id " because topology no longer exists")
         ;; m and e stand for map and event here
         (let [get-event (fn [m e]
                           (if (contains? m e)
                             ;; if the event is reachable return it
                             (m e)
                             ;; if not, log or not depending on conf
                             (let [msg (str "No transition for event: " event
                                         ", status: " status,
                                         " storm-id: " storm-id)]
                               (if error-on-no-transition?
                                 (throw-runtime msg)
                                 (do (when-not (contains? system-events event)
                                       (log-message msg))
                                   nil))
                               )))
               transition (->
                            ;; get all possible transitions
                            (state-transitions nimbus storm-id status)
                            ;; get transitions reachable from current state
                            (get (:type status))
                            ;; get form corresponding to current event
                            (get-event event))
               transition (if (or (nil? transition)
                                (keyword? transition))
                            ;; if it's nil or a keyword make a fn that returns it
                            (fn [] transition)
                            ;; otherwise use it
                            transition)
               ;; new status is whatever we get when we apply the transition function
               new-status (apply transition event-args)
               ;; if the new status is a keyword, we wrap it in a map
               new-status (if (keyword? new-status)
                            {:type new-status}
                            new-status)]
           ;; if the new status it not nil, we update the topology
           (when new-status
             (set-topology-status! nimbus storm-id new-status)))))
     )))


(defn delay-event
  "Delays an event (transition) for some number of seconds."
  [nimbus storm-id delay-secs event]
  (log-message "Delaying event " event " for " delay-secs " secs for " storm-id)
  (schedule (:timer nimbus)
    delay-secs
    #(transition! nimbus storm-id event false)
    ))


(defn transition-name!
  "Transition a topology with a given name."
  [nimbus storm-name event & args]
  (let [storm-id (get-storm-id (:storm-cluster-state nimbus) storm-name)]
    (when-not storm-id
      (throw (NotAliveException. storm-name)))
    (apply transition! nimbus storm-id event args)))


;; TODO: this can be probably removed.
(defn mapify-serializations [sers]
  (->> sers
    (map (fn [e] (if (map? e) e {e nil})))
    (apply merge)
    ))


(defn- start-storm
  "Starts the StormTopology on the Nimbus."
  [nimbus topology storm-conf storm-name storm-id topology-initial-status]
  {:pre [(#{:active :inactive} topology-initial-status)]}
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        conf (:conf nimbus)
        topology (system-topology! storm-conf topology)
        num-executors (->> (all-components topology) (map-val num-start-executors))]
    (log-message "Activating " storm-name ": " storm-id)
    (.activate-storm! storm-cluster-state
      storm-id
      (StormBase. storm-name
        (current-time-secs)
        {:type topology-initial-status}
        (storm-conf TOPOLOGY-WORKERS)
        num-executors))))


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
    ;; topology level serialization registrations take priority
    ;; that way, if there's a conflict, a user can force which serialization to use
    ;; append component conf to storm-conf
    (merge storm-conf
      {TOPOLOGY-KRYO-DECORATORS (get-merged-conf-val TOPOLOGY-KRYO-DECORATORS distinct)
       TOPOLOGY-KRYO-REGISTER (get-merged-conf-val TOPOLOGY-KRYO-REGISTER mapify-serializations)
       TOPOLOGY-ACKER-EXECUTORS (total-conf TOPOLOGY-ACKER-EXECUTORS)
       TOPOLOGY-MAX-TASK-PARALLELISM (total-conf TOPOLOGY-MAX-TASK-PARALLELISM)})))


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


;; Commented out until I think of how to do this nicely
;(defn normalize-topology
;  "Normalizes a topology."
;  [storm-conf ^StormTopology topology]
;  ;; Make sure that StormTopology const below makes copy.
;  (let [ret (StormTopology. topology)]
;    (doseq [[_ component] (all-components ret)]
;      (.setParallelismHint (.getCommon component) (component-parallelism storm-conf component))
;      (.setConf
;        (.getCommon component)
;          ;; this shit updates the conf of a component with {TOPOLOGY-TASKS = max(TOPOLOGY-MAX-TASK-PARALLELISM, num-tasks)
;          (merge (component-conf component) {TOPOLOGY-TASKS (component-parallelism storm-conf component)})))
;    ret ))
;(defn normalize-topology
;  "Normalizes a topology."
;  [storm-conf ^StormTopology topology]
;  (let [[set-parallelism (fn [c] (component-parallelism storm-conf c))]
;        [newSpouts (map-val set-parallelism (.getSpouts topology))]
;        [newBolts (map-val set-parallelism (.getBolts topology))]]
;  (StormTopology. newSpouts newBolts)))


(defn storm-active?
  "Checks whether storm is active by checking whether id is nil."
  [storm-cluster-state storm-name]
  ; get-storm-id is in common.clj
  (not-nil? (get-storm-id storm-cluster-state storm-name)))


(defn check-storm-active!
  "Checks that the status of the Storm is equal to what is passed in."
  [nimbus storm-name active?]
  (if (= (not active?)
        (storm-active? (:storm-cluster-state nimbus)
          storm-name))
    (if active?
      (throw (NotAliveException. (str storm-name " is not alive")))
      (throw (AlreadyAliveException. (str storm-name " is already active"))))
    ))


(def DISALLOWED-TOPOLOGY-NAME-STRS #{"/" "." ":" "\\"})

(defn validate-topology-name! [name]
  "Check that the name of the topology is valid."
  (if (some #(.contains name %) DISALLOWED-TOPOLOGY-NAME-STRS)
    (throw (InvalidTopologyException.
             (str "Topology name cannot contain any of the following: " (pr-str DISALLOWED-TOPOLOGY-NAME-STRS))))
    (if (clojure.string/blank? name)
      (throw (InvalidTopologyException.
               ("Topology name cannot be blank"))))))


(defn nimbus-data
  "Map containing Nimbus data."
  [conf inimbus]
  {:conf conf
   :inimbus inimbus
   :storm-cluster-state (cluster/mk-storm-cluster-state conf)
   :submitted-count (atom 0)
   :submit-lock (Object.)
   :uptime (uptime-computer)
   ; by default this is DefaultTopologyValidator
   :validator (new-instance (conf NIMBUS-TOPOLOGY-VALIDATOR))
   ; not sure if we need a timer.
   :timer (mk-timer :kill-fn (fn [t]
                               (log-error t "Error when processing event")
                               (halt-process! 20 "Error when processing an event")
                               ))
   :scheduler nil})
;   :scheduler (mk-scheduler conf inimbus)})


(defserverfn service-handler
  [conf inimbus]
  ; standalone Nimbus doesn't prepare - maybe get rid of this
  (.prepare inimbus conf)
  (log-message "Starting Nimbus with conf " conf)
  (let [nimbus (nimbus-data conf inimbus)]
    ; prepare the validator
    (.prepare ^backtype.storm.nimbus.ITopologyValidator (:validator nimbus) conf)
    ; schedule a recurring function to do reassignments
;    (schedule-recurring (:timer nimbus)
;      0
;      (conf NIMBUS-MONITOR-FREQ-SECS)
;      (fn []
;        (when (conf NIMBUS-REASSIGN)
;          (locking (:submit-lock nimbus)
;            (mk-assignments nimbus)))
;        (do-cleanup nimbus)
;        ))
    (reify Nimbus$Iface
      (^void submitTopologyWithOpts
       [this ^String storm-name ^Map topo-conf ^StormTopology topology
        ^SubmitOptions submitOptions]
       (try
         (assert (not-nil? submitOptions))
         (validate-topology-name! storm-name)
         ; check that the storm is not running
         (check-storm-active! nimbus storm-name false)
         ; there is no need to unserialize the conf, it should not be at this point
         (try
           (validate-configs-with-schemas topo-conf)
           (catch IllegalArgumentException ex
                                           (throw (InvalidTopologyException. (.getMessage ex)))))
         (.validate ^backtype.storm.nimbus.ITopologyValidator (:validator nimbus)
                                                              storm-name
                                                              topo-conf
                                                              topology)
         ; here un-serializing ends, with validation
         ; increment number of submitted
         (swap! (:submitted-count nimbus) inc)
         ; here we create a "unique" storm-id, set it up and start it, also makes assignments
         (let [storm-id (str storm-name "-" @(:submitted-count nimbus) "-" (current-time-secs))
               storm-conf (normalize-conf
                            conf
                            (-> topo-conf
                              (assoc STORM-ID storm-id)
                              (assoc TOPOLOGY-NAME storm-name))
                            topology)
               total-storm-conf (merge conf storm-conf)
               ; commented out until I find a good way to do it
;               topology (normalize-topology total-storm-conf topology)
               storm-cluster-state (:storm-cluster-state nimbus)]
           (system-topology! total-storm-conf topology) ;; this validates the structure of the topology
           (log-message "Received topology submission for " storm-name " with conf " storm-conf)
           ;; lock protects against multiple topologies being submitted at once and
           ;; cleanup thread killing topology in b/w assignment and starting the topology
           (locking (:submit-lock nimbus)
             (setup-storm-code conf storm-id storm-conf topology)
             ;; this is a map from Thrift status names to keyword statuses
             (let [thrift-status->kw-status {SubmitOptions$TopologyInitialStatus/INACTIVE :inactive
                                             SubmitOptions$TopologyInitialStatus/ACTIVE :active}]
               (start-storm nimbus topology storm-conf storm-name storm-id (thrift-status->kw-status (.getInitialStatus submitOptions))))
             ))
            ; let's leave this out for now
;             (mk-assignments nimbus)))
         (catch Throwable e
                          (log-warn-error e "Topology submission exception. (topology name='" storm-name "')")
                          (throw e))))

      (^void submitTopology
       [this ^String storm-name ^Map serializedConf ^StormTopology topology]
       (.submitTopologyWithOpts this storm-name serializedConf topology
                                     (SubmitOptions. SubmitOptions$TopologyInitialStatus/ACTIVE)))

      (^void killTopology [this ^String name]
       (.killTopologyWithOpts this name (KillOptions.)))

      (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
        (check-storm-active! nimbus storm-name true)
        (let [wait-amt (.getWaitSecs options)]
          (transition-name! nimbus storm-name [:kill wait-amt] true)
          ))

;      (^void rebalance [this ^String storm-name ^RebalanceOptions options]
;       (check-storm-active! nimbus storm-name true)
;       (let [wait-amt (if (.is_set_wait_secs options)
;                        (.get_wait_secs options))
;             num-workers (if (.is_set_num_workers options)
;                           (.get_num_workers options))
;             executor-overrides (if (.is_set_num_executors options)
;                                  (.get_num_executors options)
;                                  {})]
;         (doseq [[c num-executors] executor-overrides]
;           (when (<= num-executors 0)
;             (throw (InvalidTopologyException. "Number of executors must be greater than 0"))
;             ))
;         (transition-name! nimbus storm-name [:rebalance wait-amt num-workers executor-overrides] true)
;         ))

      (activate [this storm-name]
        )
;        (transition-name! nimbus storm-name :activate true))

      (deactivate [this storm-name]
        )
;        (transition-name! nimbus storm-name :inactivate true))

      (getNimbusConf [this]
        (:conf nimbus))

      (^String getTopologyConf [this ^String id]
        )
;       (to-json (try-read-storm-conf conf id)))

      (^StormTopology getTopology [this ^String id]
        )
;       (system-topology! (try-read-storm-conf conf id) (try-read-storm-topology conf id)))

      (^StormTopology getUserTopology [this ^String id]
        )
;       (try-read-storm-topology conf id))

;      (^ClusterSummary getClusterInfo [this]
;       (let [storm-cluster-state (:storm-cluster-state nimbus)
;             supervisor-infos (all-supervisor-info storm-cluster-state)
;             ;; TODO: need to get the port info about supervisors...
;             ;; in standalone just look at metadata, otherwise just say N/A?
;             supervisor-summaries (dofor [[id info] supervisor-infos]
;                                    (let [ports (set (:meta info)) ;;TODO: this is only true for standalone
;                                          ]
;                                      (SupervisorSummary. (:hostname info)
;                                        (:uptime-secs info)
;                                        (count ports)
;                                        (count (:used-ports info))
;                                        id)
;                                      ))
;             nimbus-uptime ((:uptime nimbus))
;             bases (topology-bases storm-cluster-state)
;             topology-summaries (dofor [[id base] bases]
;                                  (let [assignment (.assignment-info storm-cluster-state id nil)]
;                                    (TopologySummary. id
;                                      (:storm-name base)
;                                      (->> (:executor->node+port assignment)
;                                        keys
;                                        (mapcat executor-id->tasks)
;                                        count)
;                                      (->> (:executor->node+port assignment)
;                                        keys
;                                        count)
;                                      (->> (:executor->node+port assignment)
;                                        vals
;                                        set
;                                        count)
;                                      (time-delta (:launch-time-secs base))
;                                      (extract-status-str base))
;                                    ))]
;         (ClusterSummary. supervisor-summaries
;           nimbus-uptime
;           topology-summaries)
;         ))

;      (^TopologyInfo getTopologyInfo [this ^String storm-id]
;       (let [storm-cluster-state (:storm-cluster-state nimbus)
;             task->component (storm-task-info (try-read-storm-topology conf storm-id) (try-read-storm-conf conf storm-id))
;             base (.storm-base storm-cluster-state storm-id nil)
;             assignment (.assignment-info storm-cluster-state storm-id nil)
;             beats (.executor-beats storm-cluster-state storm-id (:executor->node+port assignment))
;             all-components (-> task->component reverse-map keys)
;             errors (->> all-components
;                      (map (fn [c] [c (get-errors storm-cluster-state storm-id c)]))
;                      (into {}))
;             executor-summaries (dofor [[executor [node port]] (:executor->node+port assignment)]
;                                  (let [host (-> assignment :node->host (get node))
;                                        heartbeat (get beats executor)
;                                        stats (:stats heartbeat)
;                                        stats (if stats
;                                                (stats/thriftify-executor-stats stats))]
;                                    (doto
;                                      (ExecutorSummary. (thriftify-executor-id executor)
;                                        (-> executor first task->component)
;                                        host
;                                        port
;                                        (nil-to-zero (:uptime heartbeat)))
;                                      (.set_stats stats))
;                                    ))
;             ]
;         (TopologyInfo. storm-id
;           (:storm-name base)
;           (time-delta (:launch-time-secs base))
;           executor-summaries
;           (extract-status-str base)
;           errors
;           )
;         ))

      Shutdownable
      (shutdown [this]
        (log-message "Shutting down master")
        (cancel-timer (:timer nimbus))
;        (.disconnect (:storm-cluster-state nimbus))
;        (.cleanup (:downloaders nimbus))
;        (.cleanup (:uploaders nimbus))
        (log-message "Shut down master")
        )
      DaemonCommon
      (waiting? [this]
        (timer-waiting? (:timer nimbus))))))


(defn standalone-nimbus []
  (reify INimbus
    (prepare [this conf]
      )))


(defn launch-server! [conf nimbus]
  (let [service-handler (service-handler conf nimbus)
;        options (-> (TNonblockingServerSocket. (int (conf NIMBUS-THRIFT-PORT)))
;                  (THsHaServer$Args.)
;                  (.workerThreads 64)
;                  (.protocolFactory (TBinaryProtocol$Factory. false true (conf NIMBUS-THRIFT-MAX-BUFFER-SIZE)))
;                  (.processor (Nimbus$Processor. service-handler))
;                  )
;        server (THsHaServer. (do (set! (. options maxReadBufferBytes)(conf NIMBUS-THRIFT-MAX-BUFFER-SIZE)) options))]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.shutdown service-handler) )))
    (log-message "Starting Nimbus server...")
    (.serve server)))


(defn -launch [nimbus]
  (launch-server! (read-storm-config) nimbus))


(defn -main []
  (-launch (standalone-nimbus)))