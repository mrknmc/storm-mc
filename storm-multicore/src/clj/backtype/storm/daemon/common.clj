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
(ns backtype.storm.daemon.common
  (:use [backtype.storm log config util])
  (:import [backtype.storm.generated StormTopology InvalidTopologyException
            GlobalStreamId Grouping Grouping$GroupingType])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.task WorkerTopologyContext])
  (:require [clojure.set :as set])
  )


(def SYSTEM-STREAM-ID "__system")


(defn has-ackers? [storm-conf]
  (or (nil? (storm-conf TOPOLOGY-ACKER-EXECUTORS)) (> (storm-conf TOPOLOGY-ACKER-EXECUTORS) 0)))


(defrecord Assignment [executor->worker-uuid executor->start-time-secs])


(defprotocol DaemonCommon
  (waiting? [this]))


(defmacro defserverfn [name & body]
  `(let [exec-fn# (fn ~@body)]
    (defn ~name [& args#]
      (try-cause
        (apply exec-fn# args#)
      (catch InterruptedException e#
        (throw e#))
      (catch Throwable t#
        (log-error t# "Error on initialization of server " ~(str name))
        (halt-process! 13 "Error on initialization")
        )))))


(defn worker-context [worker]
  (WorkerTopologyContext. (:system-topology worker)
    (:storm-conf worker)
    (:task->component worker)
    (:component->sorted-tasks worker)
    (:component->stream->fields worker)
    (:storm-id worker)
    (resources-path)
    (worker-pids-root (:conf worker) (:worker-id worker))
    (int 0)
    (:task-ids worker)
    (:default-shared-resources worker)
    (:user-shared-resources worker)
    ))


(defn executor-id->tasks
  "[1 ] -> [1 2 3 4]"
  [[first-task-id last-task-id]]
  (->> (range first-task-id (inc last-task-id))
       (map int)))

(defn num-start-executors
  "Returns the number of executors to start with."
  [component]
  (-> component .getCommon .getParallelismHint))


(defn component-conf
  "Get configuration of a component."
  [component]
;  (clojurify-structure (.getConf (.getCommon component))))
  (clojurify-structure (-> component .getCommon .getConf)))


(defn all-components
  "Retrieves all components of a topology in a id->component map."
  [^StormTopology topology]
  (apply merge {} (.getBolts topology) (.getSpouts topology)))


;; component->executors is a map from spout/bolt id to number of executors for that component
(defrecord StormBase [storm-name launch-time-secs status num-workers component->executors])


(def grouping-constants
  {Grouping$GroupingType/FIELDS :fields
   Grouping$GroupingType/SHUFFLE :shuffle
   Grouping$GroupingType/ALL :all
   Grouping$GroupingType/NONE :none
   Grouping$GroupingType/CUSTOM_OBJECT :custom-object
   Grouping$GroupingType/DIRECT :direct
   Grouping$GroupingType/LOCAL_OR_SHUFFLE :local-or-shuffle})


(defn grouping-type
  "Converts a GroupingType enum into a keyword."
  [grouping]
  (grouping-constants (.getType grouping)))


(defn field-grouping
  "Return fields of a grouping."
  [^Grouping grouping]
  (when-not (= (grouping-type grouping) :fields)
    (throw (IllegalArgumentException. "Tried to get grouping fields from non fields grouping")))
  (.getFields grouping))


(defn global-grouping?
  "Global grouping if fields but empty."
  [^Grouping grouping]
  (and (= :fields (grouping-type grouping))
    (empty? (field-grouping grouping))))


(defn validate-structure! [^StormTopology topology]
  ;; validate all the component subscribe from component+stream which actually exists in the topology
  ;; and if it is a fields grouping, validate the corresponding field exists
  (let [all-components (all-components topology)]
    (doseq [[id comp] all-components
            :let [inputs (.. comp getCommon getInputs)]]
      (doseq [[global-stream-id grouping] inputs
              :let [source-component-id (.getComponentId global-stream-id)
                    source-stream-id    (.getStreamId global-stream-id)]]
        (if-not (contains? all-components source-component-id)
          (throw (InvalidTopologyException. (str "Component: [" id "] subscribes from non-existent component [" source-component-id "]")))
          (let [source-streams (-> all-components (get source-component-id) .getCommon .getStreams)]
            (if-not (contains? source-streams source-stream-id)
              (throw (InvalidTopologyException. (str "Component: [" id "] subscribes from non-existent stream: [" source-stream-id "] of component [" source-component-id "]")))
              (if (= :fields (grouping-type grouping))
                (let [grouping-fields (set (.getFields grouping))
                      source-stream-fields (-> source-streams (get source-stream-id) .getOutputFields set)
                      diff-fields (set/difference grouping-fields source-stream-fields)]
                  (when-not (empty? diff-fields)
                    (throw (InvalidTopologyException. (str "Component: [" id "] subscribes from stream: [" source-stream-id "] of component [" source-component-id "] with non-existent fields: " diff-fields)))))))))))))


(defn system-id?
  "System ids start with __."
  [id]
  (Utils/isSystemId id))


(defn- validate-ids!
  "Validates IDs."
  [^StormTopology topology]
  ; make separate sets of bolts and spouts
  (let [sets [(.getBolts topology) (.getSpouts topology)]
        ; see if any of components is in both sets
        offending (apply any-intersection sets)]
    (if-not (empty? offending)
      (throw (InvalidTopologyException.
               (str "Duplicate component ids: " offending))))
    (doseq [[comp-key comp] (all-components topology)
            :let [stream-keys (-> comp .getCommon .getStreams keys)]]
      ; check if id of the component is a system id
      (if (system-id? comp-key)
        (throw (InvalidTopologyException.
                 (str comp-key " is not a valid component id"))))
      ; check if any of the stream ids is a system id
      (doseq [stream-key stream-keys]
        (if (system-id? stream-key)
          (throw (InvalidTopologyException.
                   (str stream-key " is not a valid stream id"))))))
    ))


(defn validate-basic!
  "Does basic validation on the Topology."
  [^StormTopology topology]
  (validate-ids! topology)
  (doseq [spout (-> topology .getSpouts vals)]
    ; inputs of spouts should be empty
    (if-not (empty? (-> spout .getCommon .getInputs))
      (throw (InvalidTopologyException. "May not declare inputs for a spout"))))
  (doseq [[comp-key comp] (all-components topology)
          :let [conf (component-conf comp)
                parallelism (-> comp .getCommon .getParallelismHint)]]
    ; if number of tasks is more than 0
    ; and parallelismHint is set
    ; parallelism hint must be more than 0
    (when (and
      ; TODO: figure out why this doesn't work
;            (> (conf TOPOLOGY-TASKS) 0))
            parallelism
            (<= parallelism 0))
      (throw (InvalidTopologyException. "Number of executors must be greater than 0 when number of tasks is greater than 0")))))
;  (doseq [f thrift/SPOUT-FIELDS
;          obj (->> f (.getFieldValue topology) vals)]
;    (if-not (empty? (-> obj .get_common .get_inputs))
;      (throw (InvalidTopologyException. "May not declare inputs for a spout"))))
;  (doseq [[comp-id comp] (all-components topology)
;          :let [conf (component-conf comp)
;                p (-> comp .get_common thrift/parallelism-hint)]]
;    (when (and (> (conf TOPOLOGY-TASKS) 0)
;            p
;            (<= p 0))
;      (throw (InvalidTopologyException. "Number of executors must be greater than 0 when number of tasks is greater than 0"))
;      )))


(defn system-topology!
  "Returns a system topology. (ackers etc. added to original)"
  [storm-conf ^StormTopology topology]
  (validate-basic! topology)
  topology)


(defn storm-task-info
  "Returns map from task -> component id"
  [^StormTopology user-topology storm-conf]
  (->> (system-topology! storm-conf user-topology)
    ;; get all components
    all-components
    ;; make a map of id -> num-tasks
    ;; TODO: I think TOPOLOGY-TASKS is causing a nullpointer exception here
    (map-val (comp #(get % TOPOLOGY-TASKS) component-conf))
    ;; sorts by the key (id of the component)
    (sort-by first)
    ;; for each component id creates num-tasks copies of it
    ;; and concats the whole thing together
    (mapcat (fn [[c num-tasks]] (repeat num-tasks c)))
    ;; adds an int from 1..infinity as a key for the component ids
    (map (fn [id comp] [id comp]) (iterate (comp int inc) (int 1)))
    (into {})
    ))