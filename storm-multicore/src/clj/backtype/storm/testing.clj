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
  (:require [backtype.storm.daemon.nimbus :as nimbus])
;  (:require [backtype.storm.daemon.supervisor :as supervisor])
  (:import [backtype.storm.grouping CustomStreamGrouping])
  (:import [backtype.storm.generated GlobalStreamId Grouping])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.testing TestPlannerBolt TestPlannerSpout])
  (:import [backtype.storm.topology TopologyBuilder])
  (:use [backtype.storm config util log]))



(defn mk-shuffle-grouping
  []
  (Grouping/shuffle))


(defn mk-local-or-shuffle-grouping
  []
  (Grouping/localOrShuffle))


(defn mk-fields-grouping
  [fields]
  (Grouping/fields fields))


(defn mk-global-grouping
  []
  (mk-fields-grouping []))


(defn mk-none-grouping
  []
  (Grouping/none ))


(defn mk-direct-grouping
  []
  (Grouping/direct))


(defn mk-all-grouping
  []
  (Grouping/all))


(defn- mk-grouping
  "Create a grouping"
  [grouping-spec]
  (cond (nil? grouping-spec)
    (mk-none-grouping)

    (instance? Grouping grouping-spec)
    grouping-spec

;    (instance? CustomStreamGrouping grouping-spec)
;    (Grouping/customObject grouping-spec)

;    (instance? JavaObject grouping-spec)
;    (Grouping/custom_object grouping-spec)

    (sequential? grouping-spec)
    (mk-fields-grouping grouping-spec)

    (= grouping-spec :shuffle)
    (mk-shuffle-grouping)

    (= grouping-spec :local-or-shuffle)
    (mk-local-or-shuffle-grouping)

    (= grouping-spec :none)
    (mk-none-grouping)

    (= grouping-spec :all)
    (mk-all-grouping)

    (= grouping-spec :global)
    (mk-global-grouping)

    (= grouping-spec :direct)
    (mk-direct-grouping)

    true
    (throw (IllegalArgumentException.
             (str grouping-spec " is not a valid grouping")))))


(defn- mk-inputs
  "Create inputs."
  [inputs]
  (into {} (for [[stream-id grouping-spec] inputs]
             [(if (sequential? stream-id)
                (GlobalStreamId. (first stream-id) (second stream-id))
                (GlobalStreamId. stream-id Utils/DEFAULT_STREAM_ID))
              (mk-grouping grouping-spec)])))


(defn- add-inputs
  "Add inputs to a declarer."
  [declarer inputs]
  (doseq [[id grouping] (mk-inputs inputs)]
    (.grouping declarer id grouping)))


(defn mk-topology
  "Creates a topology from spout map and bolt map."
  ([spout-map bolt-map]
    (let [builder (TopologyBuilder.)]
      (doseq [[name {spout :obj p :p conf :conf}] spout-map]
        (-> builder (.setSpout name spout (if-not (nil? p) (int p) p)) (.addConfigurations conf)))
      (doseq [[name {bolt :obj p :p conf :conf inputs :inputs}] bolt-map]
        (-> builder (.setBolt name bolt (if-not (nil? p) (int p) p)) (.addConfigurations conf) (add-inputs inputs)))
      (.createTopology builder)))
  ([spout-map bolt-map state-spout-map]
    (mk-topology spout-map bolt-map)))


(defnk mk-bolt-spec
  [inputs bolt :parallelism-hint nil :p nil :conf nil]
  (let [parallelism-hint (if p p parallelism-hint)]
    {:obj bolt :inputs inputs :p parallelism-hint :conf conf}))


(defnk mk-spout-spec
  [spout :parallelism-hint nil :p nil :conf nil]
  (let [parallelism-hint (if p p parallelism-hint)]
    {:obj spout :p parallelism-hint :conf conf}))


(def topology (mk-topology
           {"1" (mk-spout-spec (TestPlannerSpout. false) :parallelism-hint 3)}
           {"2" (mk-bolt-spec {"1" :none} (TestPlannerBolt.) :parallelism-hint 4)
            "3" (mk-bolt-spec {"2" :none} (TestPlannerBolt.))}))


(def topology2 (mk-topology
            {"1" (mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 12)}
            {"2" (mk-bolt-spec {"1" :none} (TestPlannerBolt.) :parallelism-hint 6)
             "3" (mk-bolt-spec {"1" :global} (TestPlannerBolt.) :parallelism-hint 8)
             "4" (mk-bolt-spec {"1" :global "2" :none} (TestPlannerBolt.) :parallelism-hint 4)}
            ))


(defn local-temp-path
  "Return temp directory on the system."
  []
  (str (System/getProperty "java.io.tmpdir") (if-not on-windows? "/") (uuid)))


;(defnk add-supervisor
;  "Add supervisor to the cluster map."
;  [cluster-map :ports 2]
;  (let [tmp-dir (local-temp-path)
;        port-ids (if (sequential? ports)
;                   ports
;                   (doall (repeatedly ports (:port-counter cluster-map))))
;        supervisor-conf (merge (:daemon-conf cluster-map)
;                          conf
;                          ;; gotta get rid of ports here
;                          {STORM-LOCAL-DIR tmp-dir
;                           SUPERVISOR-SLOTS-PORTS port-ids})
;        id-fn (if id (fn [] id) supervisor/generate-supervisor-id)
;        daemon (with-var-roots [supervisor/generate-supervisor-id id-fn] (supervisor/mk-supervisor supervisor-conf (:shared-context cluster-map) (supervisor/standalone-supervisor)))]
;    (swap! (:supervisors cluster-map) conj daemon)
;    (swap! (:tmp-dirs cluster-map) conj tmp-dir)
;    daemon))


(defn kill-local-storm-cluster [cluster-map]
  ;; TODO: kill worker and executors as well
  (.shutdown (:nimbus cluster-map)))
;  (.close (:state cluster-map))
;  (.disconnect (:storm-cluster-state cluster-map))
;  (doseq [s @(:supervisors cluster-map)]
;    (.shutdown-all-workers s)
;    ;; race condition here? will it launch the workers again?
;    (supervisor/kill-supervisor s))
;  (psim/kill-all-processes)
;  (log-message "Shutting down in process zookeeper")
;  (zk/shutdown-inprocess-zookeeper (:zookeeper cluster-map))
;  (log-message "Done shutting down in process zookeeper")
;  (doseq [t @(:tmp-dirs cluster-map)]
;    (log-message "Deleting temporary path " t)
;    (try
;      (rmr t)
;      ;; on windows, the host process still holds lock on the logfile
;      (catch Exception e (log-message (.getMessage e)))) ))


(defn submit-local-topology
  [nimbus storm-name conf topology]
  (when-not (Utils/isValidConf conf)
    (throw (IllegalArgumentException. "Topology conf is not json-serializable")))
  (.submitTopology nimbus storm-name conf topology))


(defn submit-local-topology-with-opts
  [nimbus storm-name conf topology submit-opts]
  (when-not (Utils/isValidConf conf)
    (throw (IllegalArgumentException. "Topology conf is not json-serializable")))
  (.submitTopologyWithOpts nimbus storm-name conf topology submit-opts))


(defnk mk-local-storm-cluster
  "Reads the daemon config, creates a nimbus thread."
  [:daemon-conf {} :inimbus nil :supervisors 2]
  (let [daemon-conf (merge (read-storm-config)
                           {TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS false
                            TOPOLOGY-TRIDENT-BATCH-EMIT-INTERVAL-MILLIS 50}
                           daemon-conf)
        nimbus (nimbus/service-handler
                 daemon-conf
                 (if inimbus inimbus (nimbus/standalone-nimbus)))
;        nimbus-tmp (local-temp-path)
;        context (mk-shared-context daemon-conf)
;        port-counter (mk-counter supervisor-slot-port-min)
        cluster-map {:nimbus nimbus
                   :daemon-conf daemon-conf}]
                   ;; gotta get rid of port counter here
;                   :port-counter port-counter
                   ;; should get rid of tmp dirs as well
;                   :tmp-dirs (atom [nimbus-tmp])
;                   :supervisors (atom [])
                   ;; shared context may be unneccessary as well
;                   :shared-context nil}]
;    (dotimes [n supervisors]
;      (add-supervisor cluster-map))
    cluster-map))


(defn kill-local-storm-cluster [cluster-map]
  (.shutdown (:nimbus cluster-map)))
;  (.close (:state cluster-map))
;  (.disconnect (:storm-cluster-state cluster-map)))
;  (doseq [s @(:supervisors cluster-map)]
;    (.shutdown-all-workers s)
;    ;; race condition here? will it launch the workers again?
;    (supervisor/kill-supervisor s))


(defmacro with-local-cluster
  [[cluster-sym & args] & body]
  `(let [~cluster-sym (mk-local-storm-cluster ~@args)]
     (try
       ~@body
       (catch Throwable t#
         (log-error t# "Error in cluster")
         (throw t#))
       (finally
         (kill-local-storm-cluster ~cluster-sym)))))
