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
(ns backtype.storm.daemon.worker
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm util config log timer])
  (:require [backtype.storm.cluster :as cluster])
  (:require [backtype.storm.disruptor :as disruptor])
  (:require [backtype.storm.daemon [executor :as executor]])
  (:import [backtype.storm.daemon Shutdownable])
  (:import [backtype.storm.generated StormTopology])
  (:import [backtype.storm.tuple Fields])
  (:import [backtype.storm.task WorkerTopologyContext])
  (:import [backtype.storm.utils ThriftTopologyUtils])
  (:import [java.util.concurrent Executors])
  (:import [java.util ArrayList HashMap])
  (:gen-class))


(defn- mk-default-resources [worker]
  (let [conf (:conf worker)
        thread-pool-size (int (conf TOPOLOGY-WORKER-SHARED-THREAD-POOL-SIZE))]
    {WorkerTopologyContext/SHARED_EXECUTOR (Executors/newFixedThreadPool thread-pool-size)}
    ))


(defn- mk-user-resources [worker]
  ;;TODO: need to invoke a hook provided by the topology, giving it a chance to create user resources.
  ;; this would be part of the initialization hook
  ;; need to separate workertopologycontext into WorkerContext and WorkerUserContext.
  ;; actually just do it via interfaces. just need to make sure to hide setResource from tasks
  {})


(defn mk-transfer-fn
  "Make a transfer function."
  [worker]
  (let [short-executor-receive-queue-map (:short-executor-receive-queue-map worker)
        task->short-executor (:task->short-executor worker)
        task-getter #(get task->short-executor %)]
    (fn [task tuple]
      ;; find which executor and queue correspond to target-task
      (let [short-executor (task-getter task)
            q (short-executor-receive-queue-map short-executor)]
        (if q
          ;; publish the tuple
          (disruptor/publish q [task tuple])
          (log-warn "Received invalid messages for unknown tasks. Dropping... ")
          )))))


(defn- mk-receive-queue-map
  "Creates a map of {executor -> disruptor-queue}."
  [storm-conf executors]
  (->> executors
    ;; TODO: this depends on the type of executor
    (map (fn [e] [e (disruptor/disruptor-queue (str "receive-queue" e)
                      (storm-conf TOPOLOGY-EXECUTOR-RECEIVE-BUFFER-SIZE)
                      :wait-strategy (storm-conf TOPOLOGY-DISRUPTOR-WAIT-STRATEGY))]))
    (into {})
    ))


(defn- stream->fields
  "Makes a map {stream -> fields}."
  [^StormTopology topology component]
  (->> (ThriftTopologyUtils/getComponentCommon topology component)
    .getStreams
    (map (fn [[s info]] [s (Fields. (.getOutputFields info))]))
    (into {})
    (HashMap.)))


(defn component->stream->fields
  "Makes a map {component -> {stream -> fields}}"
  [^StormTopology topology]
  (->> (ThriftTopologyUtils/getComponentIds topology)
    (map (fn [c] [c (stream->fields topology c)]))
    (into {})
    (HashMap.)))


(defn mk-suicide-fn
  [conf]
  (fn [] (halt-process! 1 "Worker died")))


(defn mk-halting-timer [timer-name]
  (mk-timer :kill-fn (fn [t]
                       (log-error t "Error when processing event")
                       (halt-process! 20 "Error when processing an event")
                       )
    :timer-name timer-name))


(defn worker-data [conf storm-conf storm-id topology executors]
  (let [
         ;; state of cluster
         storm-cluster-state (cluster/mk-storm-cluster-state conf)
         ;; conf and executors read from zk
;         storm-conf (read-supervisor-storm-conf conf storm-id)
;         executors (set (read-worker-executors storm-conf storm-cluster-state storm-id assignment-id port))
         ;; queue that sends data further along
;         transfer-queue (disruptor/disruptor-queue "worker-transfer-queue" (storm-conf TOPOLOGY-TRANSFER-BUFFER-SIZE)
;                          :wait-strategy (storm-conf TOPOLOGY-DISRUPTOR-WAIT-STRATEGY))
         ;; create a disruptor queue for every executor
         executor-receive-queue-map (mk-receive-queue-map storm-conf executors)
         ;; create a mapping for every task to queue of the executor
         receive-queue-map (->> executor-receive-queue-map
                             (mapcat (fn [[e queue]] (for [t (executor-id->tasks e)] [t queue])))
                             (into {}))]
    (recursive-map
      :conf conf
      :storm-id storm-id
      :assignment-id (uuid)
      :worker-id (uuid)
      :storm-cluster-state storm-cluster-state
      :storm-active-atom (atom true)
      :executors executors
      :task-ids (->> receive-queue-map keys (map int) sort)
      :storm-conf storm-conf
      :topology topology
      :system-topology (system-topology! storm-conf topology)
      :task->component (HashMap. (storm-task-info topology storm-conf)) ; for optimized access when used in tasks later on
      :component->stream->fields (component->stream->fields (:system-topology <>))
      :component->sorted-tasks (->> (:task->component <>) reverse-map (map-val sort))
      :executor-receive-queue-map executor-receive-queue-map
      :user-timer (mk-halting-timer "user-timer")
      :short-executor-receive-queue-map (map-key first executor-receive-queue-map)
      :task->short-executor (->> executors
                              (mapcat (fn [e] (for [t (executor-id->tasks e)] [t (first e)])))
                              (into {})
                              (HashMap.))
      :suicide-fn (mk-suicide-fn conf)
      :uptime (uptime-computer)
      :default-shared-resources (mk-default-resources <>)
      :user-shared-resources (mk-user-resources <>)
      :transfer-fn (mk-transfer-fn <>)
      )))


(defserverfn mk-worker [conf storm-conf storm-id topology executors]
  (log-message "Launching worker for " storm-id " with conf " conf)
  (let [worker (worker-data conf storm-conf storm-id topology executors)
        worker-id (:worker-id worker)
        assignment-id (:assignment-id worker)
        executors (atom nil)

        ;; initialize executors
        _ (reset! executors (dofor [e (:executors worker)] (executor/mk-executor worker e)))
        shutdown* (fn []
                    (log-message "Shutting down worker " storm-id " " assignment-id)
                    (log-message "Shutting down executors")
                    (doseq [executor @executors] (.shutdown executor))
                    (log-message "Shut down executors")

                    ;; TODO: here need to invoke the "shutdown" method of WorkerHook

                    (log-message "Shut down worker " storm-id " " assignment-id))
        ret (reify
              Shutdownable
              (shutdown
                [this]
                (shutdown*))
              DaemonCommon
              (waiting? [this]
                )
              )]

    (log-message "Worker has topology config " (:storm-conf worker))
    (log-message "Worker " worker-id " for storm " storm-id " on " assignment-id " has finished loading")
    ret
    ))