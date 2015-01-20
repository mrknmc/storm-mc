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
  (:import [org.apache.commons.io FileUtils])
  (:import [java.nio ByteBuffer])
  (:import [java.util Map])
  (:import [java.io File FileNotFoundException])
  (:import [java.nio.channels Channels WritableByteChannel])
  (:require [backtype.storm.cluster :as cluster])
  (:import [backtype.storm.daemon Shutdownable])
  (:import [backtype.storm.generated StormTopology
            NotAliveException AlreadyAliveException
            InvalidTopologyException SubmitOptions
            SubmitOptions$TopologyInitialStatus KillOptions])
  (:import [backtype.storm.generated Nimbus Nimbus$Iface])
  (:import [backtype.storm.utils Utils ThriftTopologyUtils])
  (:require [backtype.storm.cluster :as cluster])
  (:require [backtype.storm.daemon.worker :as worker])
  ;; this is required for the StormBase import to work
  (:require [backtype.storm.daemon.common])
  (:import [backtype.storm.daemon.common StormBase Assignment])
  ;; this is required for the DefaultScheduler import to work
  (:use [backtype.storm.scheduler.DefaultScheduler])
  (:import [backtype.storm.scheduler INimbus SupervisorDetails WorkerSlot Topologies TopologyDetails ExecutorDetails
            Cluster SchedulerAssignment SchedulerAssignmentImpl DefaultScheduler])
  (:use [backtype.storm util timer log config])
  (:use [backtype.storm.daemon common])
  (:gen-class
   :methods [^{:static true} [launch [backtype.storm.scheduler.INimbus] void]]))


(defn compute-available-cpus
  "All CPU slots available for scheduling."
  []
  ;; for now just return number of cores
  (.availableProcessors (Runtime/getRuntime)))


;; declared here because of cyclic dependency
(declare compute-executor->component)


(defn- compute-executor->worker-uuid
  "convert {executor -> slot} to
           {executor [uuid]}"
  [scheduler-assignment]
  (->> scheduler-assignment
    .getExecutorToSlot
    (#(into {} (for [[^ExecutorDetails executor ^WorkerSlot slot] %]
                 {[(.getStartTask executor) (.getEndTask executor)] (.getUUID slot)})))))


(defn- to-executor-id
  "Executor id is the first and last task it will run."
  [task-ids]
  [(first task-ids) (last task-ids)])


(defn get-storm-id
  "Retrieve an id of a topology, given a name."
  [storm-cluster-state storm-name]
  (let [active-storms (.active-storms storm-cluster-state)]
    (find-first
      #(= storm-name (:storm-name (.storm-base storm-cluster-state % nil)))
      active-storms)
    ))


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


(defn- compute-executor->component
  "Compute executor-id -> component map."
  [nimbus topology storm-conf]
  (let [conf (:conf nimbus)
        executors (compute-executors nimbus topology storm-conf)
        task->component (storm-task-info topology storm-conf)
        executor->component (into {} (for [executor executors
                                           :let [start-task (first executor)
                                                 component (task->component start-task)]]
                                       {executor component}))]
    executor->component))


(defn mk-topology-details
  "Initialises a TopologyDetails object."
  ;; TODO: Consider getting rid of this, it is not needed as there is access to topology and all the details
  [storm-id storm-conf topology executor->component]
  (TopologyDetails.
    storm-id
    storm-conf
    topology
    (storm-conf TOPOLOGY-WORKERS)
    executor->component))


;;; public so it can be mocked out
(defn compute-new-executor->worker-uuid
  [nimbus topology storm-id storm-conf]
  (let [;; map of executor -> component it's going to use
        executor->component (->> (compute-executor->component nimbus topology storm-conf)
                              (map-key (fn [[start-task end-task]]
                                         (ExecutorDetails. (int start-task) (int end-task)))))
        ;; create a slot for each cpu
        ;; TODO: is it right to limit by number of CPUs here?
        all-scheduling-slots (repeatedly (compute-available-cpus) #(WorkerSlot.))
        topology-details (mk-topology-details storm-id storm-conf topology executor->component)
        cluster (Cluster. (:inimbus nimbus) all-scheduling-slots)
;        ;; call scheduler.schedule to schedule all the topologies
;        ;; the new assignments for all the topologies are in the cluster object.
        _ (.schedule (:scheduler nimbus) topology-details cluster)

        new-scheduler-assignment (.getAssignment cluster)
;        ;; add more information to convert SchedulerAssignment to Assignment
        new-topology->executor->worker-uuid (compute-executor->worker-uuid new-scheduler-assignment)]
    new-topology->executor->worker-uuid))


(defn mk-assignments
  [nimbus topology storm-id storm-conf]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        ;; make the new assignments for topologies
        executor->worker-uuid (compute-new-executor->worker-uuid nimbus topology storm-id storm-conf)
        ;; construct the final Assignments by adding start-times etc into it
        start-times (map-val (fn [& x] (current-time-secs)) executor->worker-uuid )
        assignment (Assignment. executor->worker-uuid start-times)]

    ;; tasks figure out what tasks to talk to by looking at topology at runtime
    ;; only log/set when there's been a change to the assignment
    (log-message "Setting new assignment for topology id " storm-id ": " (pr-str assignment))
    ;; TODO: there is only one Topo => no need for this to be a dict
    (.set-assignment! storm-cluster-state storm-id assignment)
    assignment))


(defn mk-scheduler
  "Make a scheduler for a Nimbus obj and prepare it:
    1. take forced one if set
    2. take custom one if set
    3. fallback to DefaultScheduler
  "
  [conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)
        scheduler (cond
                    forced-scheduler
                    (do (log-message "Using forced scheduler from INimbus " (class forced-scheduler))
                      forced-scheduler)

                    (conf STORM-SCHEDULER)
                    (do (log-message "Using custom scheduler: " (conf STORM-SCHEDULER))
                      (-> (conf STORM-SCHEDULER) new-instance))

                    :else (do (log-message "Using default scheduler")
                            (DefaultScheduler.)))]
    (.prepare scheduler conf)
    scheduler
    ))


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


(defn storm-active?
  "Checks whether storm is active by checking whether id is nil."
  [storm-cluster-state storm-name]
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
   :scheduler (mk-scheduler conf inimbus)})


(defserverfn service-handler
  [conf inimbus]
  ; standalone Nimbus doesn't prepare - maybe get rid of this
  (.prepare inimbus conf)
  (log-message "Starting Nimbus with conf " conf)
  (let [nimbus (nimbus-data conf inimbus)]
    ; prepare the validator
    (.prepare ^backtype.storm.nimbus.ITopologyValidator (:validator nimbus) conf)
    (reify Nimbus$Iface
      (^void submitTopologyWithOpts
       [this ^String storm-name ^Map topo-conf ^StormTopology topology
        ^SubmitOptions submitOptions]
        (let [topo-conf (clojurify-structure topo-conf)]
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
                 topology (normalize-topology! total-storm-conf topology)
                 storm-cluster-state (:storm-cluster-state nimbus)]
             (system-topology! total-storm-conf topology) ;; this validates the structure of the topology
             (log-message "Received topology submission for " storm-name " with conf " storm-conf)
             ;; lock protects against multiple topologies being submitted at once and
             ;; cleanup thread killing topology in b/w assignment and starting the topology
             (locking (:submit-lock nimbus)
               ;; this is a map from Thrift status names to keyword statuses
               (let [thrift-status->kw-status {SubmitOptions$TopologyInitialStatus/INACTIVE :inactive
                                               SubmitOptions$TopologyInitialStatus/ACTIVE :active}
                     _ (start-storm nimbus topology total-storm-conf storm-name storm-id (thrift-status->kw-status (.getInitialStatus submitOptions)))
                     assignment (mk-assignments nimbus topology storm-id total-storm-conf)
                     executors (keys (:executor->worker-uuid assignment ))]
                 (worker/mk-worker conf total-storm-conf storm-id topology executors))))
           (catch Throwable e
                            (log-warn-error e "Topology submission exception. (topology name='" storm-name "')")
                            (throw e)))))

      (^void submitTopology
       [this ^String storm-name ^Map serializedConf ^StormTopology topology]
       (.submitTopologyWithOpts this storm-name serializedConf topology
                                     (SubmitOptions. SubmitOptions$TopologyInitialStatus/ACTIVE)))

      (^void killTopology [this ^String name]
       (.killTopologyWithOpts this name (KillOptions.)))

      (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
        (check-storm-active! nimbus storm-name true)
        (let [wait-amt (.getWaitSecs options)]
          ;; (transition-name! nimbus storm-name [:kill wait-amt] true)
          ))

      (getNimbusConf [this]
        (:conf nimbus))

      Shutdownable
      (shutdown [this]
        (log-message "Shutting down master")
        (cancel-timer (:timer nimbus))
        (log-message "Shut down master")
        )
      DaemonCommon
      (waiting? [this]
        (timer-waiting? (:timer nimbus))))))


(defn standalone-nimbus []
  (reify INimbus
    (prepare [this conf]
      )
    (assignSlots [this topology slots]
      )
    (getForcedScheduler [this]
      )))
