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
;  (:import [backtype.storm.task WorkerTopologyContext])
;  (:import [backtype.storm Constants])
;  (:import [backtype.storm.metric SystemBolt])
  (:require [clojure.set :as set])  
;  (:require [backtype.storm.daemon.acker :as acker])
;  (:require [backtype.storm.thrift :as thrift])
  )


;(def ACKER-COMPONENT-ID acker/ACKER-COMPONENT-ID)
;(def ACKER-INIT-STREAM-ID acker/ACKER-INIT-STREAM-ID)
;(def ACKER-ACK-STREAM-ID acker/ACKER-ACK-STREAM-ID)
;(def ACKER-FAIL-STREAM-ID acker/ACKER-FAIL-STREAM-ID)
;
;(def SYSTEM-STREAM-ID "__system")
;
;(def SYSTEM-COMPONENT-ID Constants/SYSTEM_COMPONENT_ID)
;(def SYSTEM-TICK-STREAM-ID Constants/SYSTEM_TICK_STREAM_ID)
;(def METRICS-STREAM-ID Constants/METRICS_STREAM_ID)
;(def METRICS-TICK-STREAM-ID Constants/METRICS_TICK_STREAM_ID)
;
;
;
;
;(defrecord SupervisorInfo [time-secs hostname assignment-id used-ports meta scheduler-meta uptime-secs])
;
;(def LS-WORKER-HEARTBEAT "worker-heartbeat")
;
;;; LocalState constants
;(def LS-ID "supervisor-id")
;(def LS-LOCAL-ASSIGNMENTS "local-assignments")
;(def LS-APPROVED-WORKERS "approved-workers")
;
;
;
;(defrecord WorkerHeartbeat [time-secs storm-id executors port])
;
;(defrecord ExecutorStats [^long processed
;                          ^long acked
;                          ^long emitted
;                          ^long transferred
;                          ^long failed])
;
;(defn new-executor-stats []
;  (ExecutorStats. 0 0 0 0 0))
;
;
;(defn topology-bases [storm-cluster-state]
;  (let [active-topologies (.active-storms storm-cluster-state)]
;    (into {}
;          (dofor [id active-topologies]
;                 [id (.storm-base storm-cluster-state id nil)]
;                 ))
;    ))
;
;(defn validate-distributed-mode! [conf]
;  (if (local-mode? conf)
;      (throw
;        (IllegalArgumentException. "Cannot start server in local mode!"))))
;
;
;
;(defn acker-inputs [^StormTopology topology]
;  (let [bolt-ids (.. topology get_bolts keySet)
;        spout-ids (.. topology get_spouts keySet)
;        spout-inputs (apply merge
;                            (for [id spout-ids]
;                              {[id ACKER-INIT-STREAM-ID] ["id"]}
;                              ))
;        bolt-inputs (apply merge
;                           (for [id bolt-ids]
;                             {[id ACKER-ACK-STREAM-ID] ["id"]
;                              [id ACKER-FAIL-STREAM-ID] ["id"]}
;                             ))]
;    (merge spout-inputs bolt-inputs)))
;
;(defn add-acker! [storm-conf ^StormTopology ret]
;  (let [num-executors (if (nil? (storm-conf TOPOLOGY-ACKER-EXECUTORS)) (storm-conf TOPOLOGY-WORKERS) (storm-conf TOPOLOGY-ACKER-EXECUTORS))
;        acker-bolt (thrift/mk-bolt-spec* (acker-inputs ret)
;                                         (new backtype.storm.daemon.acker)
;                                         {ACKER-ACK-STREAM-ID (thrift/direct-output-fields ["id"])
;                                          ACKER-FAIL-STREAM-ID (thrift/direct-output-fields ["id"])
;                                          }
;                                         :p num-executors
;                                         :conf {TOPOLOGY-TASKS num-executors
;                                                TOPOLOGY-TICK-TUPLE-FREQ-SECS (storm-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)})]
;    (dofor [[_ bolt] (.get_bolts ret)
;            :let [common (.get_common bolt)]]
;           (do
;             (.put_to_streams common ACKER-ACK-STREAM-ID (thrift/output-fields ["id" "ack-val"]))
;             (.put_to_streams common ACKER-FAIL-STREAM-ID (thrift/output-fields ["id"]))
;             ))
;    (dofor [[_ spout] (.get_spouts ret)
;            :let [common (.get_common spout)
;                  spout-conf (merge
;                               (component-conf spout)
;                               {TOPOLOGY-TICK-TUPLE-FREQ-SECS (storm-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)})]]
;      (do
;        ;; this set up tick tuples to cause timeouts to be triggered
;        (.set_json_conf common (to-json spout-conf))
;        (.put_to_streams common ACKER-INIT-STREAM-ID (thrift/output-fields ["id" "init-val" "spout-task"]))
;        (.put_to_inputs common
;                        (GlobalStreamId. ACKER-COMPONENT-ID ACKER-ACK-STREAM-ID)
;                        (thrift/mk-direct-grouping))
;        (.put_to_inputs common
;                        (GlobalStreamId. ACKER-COMPONENT-ID ACKER-FAIL-STREAM-ID)
;                        (thrift/mk-direct-grouping))
;        ))
;    (.put_to_bolts ret "__acker" acker-bolt)
;    ))
;
;(defn add-metric-streams! [^StormTopology topology]
;  (doseq [[_ component] (all-components topology)
;          :let [common (.get_common component)]]
;    (.put_to_streams common METRICS-STREAM-ID
;                     (thrift/output-fields ["task-info" "data-points"]))))
;
;(defn add-system-streams! [^StormTopology topology]
;  (doseq [[_ component] (all-components topology)
;          :let [common (.get_common component)]]
;    (.put_to_streams common SYSTEM-STREAM-ID (thrift/output-fields ["event"]))))
;
;
;(defn map-occurrences [afn coll]
;  (->> coll
;       (reduce (fn [[counts new-coll] x]
;                 (let [occurs (inc (get counts x 0))]
;                   [(assoc counts x occurs) (cons (afn x occurs) new-coll)]))
;               [{} []])
;       (second)
;       (reverse)))
;
;(defn number-duplicates
;  "(number-duplicates [\"a\", \"b\", \"a\"]) => [\"a\", \"b\", \"a#2\"]"
;  [coll]
;  (map-occurrences (fn [x occurences] (if (>= occurences 2) (str x "#" occurences) x)) coll))
;
;(defn metrics-consumer-register-ids
;  "Generates a list of component ids for each metrics consumer
;   e.g. [\"__metrics_org.mycompany.MyMetricsConsumer\", ..] "
;  [storm-conf]
;  (->> (get storm-conf TOPOLOGY-METRICS-CONSUMER-REGISTER)
;       (map #(get % "class"))
;       (number-duplicates)
;       (map #(str Constants/METRICS_COMPONENT_ID_PREFIX %))))
;
;(defn metrics-consumer-bolt-specs [storm-conf topology]
;  (let [component-ids-that-emit-metrics (cons SYSTEM-COMPONENT-ID (keys (all-components topology)))
;        inputs (->> (for [comp-id component-ids-that-emit-metrics]
;                      {[comp-id METRICS-STREAM-ID] :shuffle})
;                    (into {}))
;
;        mk-bolt-spec (fn [class arg p]
;                       (thrift/mk-bolt-spec*
;                        inputs
;                        (backtype.storm.metric.MetricsConsumerBolt. class arg)
;                        {} :p p :conf {TOPOLOGY-TASKS p}))]
;
;    (map
;     (fn [component-id register]
;       [component-id (mk-bolt-spec (get register "class")
;                                   (get register "argument")
;                                   (or (get register "parallelism.hint") 1))])
;
;     (metrics-consumer-register-ids storm-conf)
;     (get storm-conf TOPOLOGY-METRICS-CONSUMER-REGISTER))))
;
;(defn add-metric-components! [storm-conf ^StormTopology topology]
;  (doseq [[comp-id bolt-spec] (metrics-consumer-bolt-specs storm-conf topology)]
;    (.put_to_bolts topology comp-id bolt-spec)))
;
;(defn add-system-components! [conf ^StormTopology topology]
;  (let [system-bolt-spec (thrift/mk-bolt-spec*
;                          {}
;                          (SystemBolt.)
;                          {SYSTEM-TICK-STREAM-ID (thrift/output-fields ["rate_secs"])
;                           METRICS-TICK-STREAM-ID (thrift/output-fields ["interval"])}
;                          :p 0
;                          :conf {TOPOLOGY-TASKS 0})]
;    (.put_to_bolts topology SYSTEM-COMPONENT-ID system-bolt-spec)))
;
;
;(defn has-ackers? [storm-conf]
;  (or (nil? (storm-conf TOPOLOGY-ACKER-EXECUTORS)) (> (storm-conf TOPOLOGY-ACKER-EXECUTORS) 0)))
;
;
;
;(defn executor-id->tasks [[first-task-id last-task-id]]
;  (->> (range first-task-id (inc last-task-id))
;       (map int)))
;
;(defn worker-context [worker]
;  (WorkerTopologyContext. (:system-topology worker)
;                          (:storm-conf worker)
;                          (:task->component worker)
;                          (:component->sorted-tasks worker)
;                          (:component->stream->fields worker)
;                          (:storm-id worker)
;                          (supervisor-storm-resources-path
;                            (supervisor-stormdist-root (:conf worker) (:storm-id worker)))
;                          (worker-pids-root (:conf worker) (:worker-id worker))
;                          (:port worker)
;                          (:task-ids worker)
;                          (:default-shared-resources worker)
;                          (:user-shared-resources worker)
;                          ))
;
;
;(defn to-task->node+port [executor->node+port]
;  (->> executor->node+port
;       (mapcat (fn [[e node+port]] (for [t (executor-id->tasks e)] [t node+port])))
;       (into {})))


; Stuff below here should be ok


;;; the task id is the virtual port
;;; node->host is here so that tasks know who to talk to just from assignment
;;; this avoid situation where node goes down and task doesn't know what to do information-wise
;(defrecord Assignment [master-code-dir node->host executor->node+port executor->start-time-secs])

(defrecord Assignment [executor->node+port executor->start-time-secs])


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
   ;   Grouping$GroupingType/CUSTOM_SERIALIZED :custom-serialized
   Grouping$GroupingType/CUSTOM_OBJECT :custom-object
   Grouping$GroupingType/DIRECT :direct
   Grouping$GroupingType/LOCAL_OR_SHUFFLE :local-or-shuffle})


(defn grouping-type
  "Converts a GroupingType enum into a keyword."
  [grouping]
  (grouping-constants (.getType grouping)))


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
;  (let [sets (map #(.getFieldValue topology %) thrift/STORM-TOPOLOGY-FIELDS)
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
;    (doseq [f thrift/STORM-TOPOLOGY-FIELDS
;            :let [obj-map (.getFieldValue topology f)]]
;      (doseq [id (keys obj-map)]
;        (if (system-id? id)
;          (throw (InvalidTopologyException.
;                   (str id " is not a valid component id")))))
;      (doseq [obj (vals obj-map)
;              id (-> obj .get_common .get_streams keys)]
;        (if (system-id? id)
;          (throw (InvalidTopologyException.
;                   (str id " is not a valid stream id"))))))
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
;  (let [ret (.deepCopy topology)]
    ; we do not need ackers, but see if there is anything vital there
;    (add-acker! storm-conf ret)
    ; add the rest if vital
;    (add-metric-components! storm-conf ret)
;    (add-system-components! storm-conf ret)
;    (add-metric-streams! ret)
;    (add-system-streams! ret)
;    (validate-structure! ret)


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