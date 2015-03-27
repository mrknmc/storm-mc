;;; Licensed to the Apache Software Foundation (ASF) under one
;;; or more contributor license agreements.  See the NOTICE file
;;; distributed with this work for additional information
;;; regarding copyright ownership.  The ASF licenses this file
;;; to you under the Apache License, Version 2.0 (the
;;; "License"); you may not use this file except in compliance
;;; with the License.  You may obtain a copy of the License at
;;;
;;; http://www.apache.org/licenses/LICENSE-2.0
;;;
;;; Unless required by applicable law or agreed to in writing, software
;;; distributed under the License is distributed on an "AS IS" BASIS,
;;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;;; See the License for the specific language governing permissions and
;;; limitations under the License.
(ns backtype.storm.daemon.executor
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm config util log timer])
  (:import [java.util Random List ArrayList LinkedList HashMap])
  (:import [backtype.storm Config Constants])
  (:import [backtype.storm.utils Utils RotatingMap RotatingMap$ExpiredCallback Time MutableLong])
  (:import [com.lmax.disruptor InsufficientCapacityException])
  (:import [backtype.storm.hooks ITaskHook])
  (:import [backtype.storm.daemon Shutdownable])
  (:import [backtype.storm.tuple Tuple TupleImpl Fields MessageId])
  (:import [backtype.storm.generated GlobalStreamId])
  (:import [backtype.storm.grouping CustomStreamGrouping])
  (:import [backtype.storm.task WorkerTopologyContext OutputCollector IOutputCollector IBolt])
  (:import [backtype.storm.spout ISpoutWaitStrategy SpoutOutputCollector ISpoutOutputCollector ISpout])
  (:import [backtype.storm.hooks.info SpoutAckInfo SpoutFailInfo
            EmitInfo BoltFailInfo BoltAckInfo BoltExecuteInfo])
  (:import [backtype.storm.metric.api IMetric IMetricsConsumer$TaskInfo IMetricsConsumer$DataPoint StateMetric])
  (:require [clojure.set :as set])
  (:require [backtype.storm.stats :as stats])
;  (:require [backtype.storm.cluster :as cluster])
  (:require [backtype.storm.disruptor :as disruptor])
  (:require [backtype.storm.tuple :as tuple])
  (:require [backtype.storm.daemon.task :as task]))


(defn executor-selector [executor-data & _] (:type executor-data))

(defmulti close-component executor-selector)
(defmulti mk-executor-stats executor-selector)
(defmulti mk-threads executor-selector)


(defmethod close-component :spout [executor-data spout]
  (.close spout))


(defmethod close-component :bolt [executor-data bolt]
  (.cleanup bolt))


(defn throttled-report-error-fn [executor]
  (let [storm-conf (:storm-conf executor)
        error-interval-secs (storm-conf TOPOLOGY-ERROR-THROTTLE-INTERVAL-SECS)
        max-per-interval (storm-conf TOPOLOGY-MAX-ERROR-REPORT-PER-INTERVAL)
        interval-start-time (atom (current-time-secs))
        interval-errors (atom 0)
        ]
    (fn [error]
      (log-error error)
      (when (> (time-delta @interval-start-time)
               error-interval-secs)
        (reset! interval-errors 0)
        (reset! interval-start-time (current-time-secs)))
      (swap! interval-errors inc))))

;      (when (<= @interval-errors max-per-interval)
;        (cluster/report-error (:storm-cluster-state executor) (:storm-id executor) (:component-id executor) error)
;        ))))


(defn setup-ticks! [worker executor-data]
  (let [storm-conf (:storm-conf executor-data)
        tick-time-secs (storm-conf TOPOLOGY-TICK-TUPLE-FREQ-SECS)
        receive-queue (:receive-queue executor-data)
        context (:worker-context executor-data)]
    (when tick-time-secs
      (if (or (system-id? (:component-id executor-data))
            (and (not (storm-conf TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS))
              (= :spout (:type executor-data))))
        (log-message "Timeouts disabled for executor " (:component-id executor-data) ":" (:executor-id executor-data))
        (schedule-recurring
          (:user-timer worker)
          tick-time-secs
          tick-time-secs
          (fn []
            (disruptor/publish
              receive-queue
              [nil (TupleImpl. context [tick-time-secs] Constants/SYSTEM_TASK_ID Constants/SYSTEM_TICK_STREAM_ID)]
              )))))))


(defprotocol RunningExecutor
  (render-stats [this])
  (get-executor-id [this]))


(defn- mk-fields-grouper
  "Create function that takes task-id and values
  and returns "
  ;; TODO: learn what this actually does.
  [^Fields out-fields ^Fields group-fields ^List target-tasks]
  (let [num-tasks (count target-tasks)
        task-getter (fn [i] (.get target-tasks i))]
    (fn [task-id ^List values]
      (-> (.select out-fields group-fields values)
          tuple/list-hash-code
          (mod num-tasks)
          task-getter))))


(defn- mk-shuffle-grouper
  "Create function that takes task-id and tuple
  and returns a random target-task."
  [^List target-tasks]
  (let [choices (rotating-random-range target-tasks)]
    (fn [task-id tuple]
      (acquire-random-range-id choices))))


(defn- mk-custom-grouper
  "Custom implementation of CustomStreamGrouping."
  [^CustomStreamGrouping grouping ^WorkerTopologyContext context ^String component-id ^String stream-id target-tasks]
  (.prepare grouping context (GlobalStreamId. component-id stream-id) target-tasks)
  (fn [task-id ^List values]
    (.chooseTasks grouping task-id values)
    ))


(defn- mk-grouper
  "Returns a function that returns a vector of which task indices to send tuple to, or just a single task index."
  [^WorkerTopologyContext context component-id stream-id ^Fields out-fields thrift-grouping ^List target-tasks]
  (let [num-tasks (count target-tasks)
        random (Random.)
        target-tasks (vec (sort target-tasks))]
    (condp = (grouping-type thrift-grouping)
      :fields
        (if (global-grouping? thrift-grouping)
          (fn [task-id tuple]
            ;; It's possible for target to have multiple tasks if it reads multiple sources
            (first target-tasks))
          (let [group-fields (Fields. (field-grouping thrift-grouping))]
            (mk-fields-grouper out-fields group-fields target-tasks)
            ))
      :all
        (fn [task-id tuple] target-tasks)
      :shuffle
        (mk-shuffle-grouper target-tasks)
      :local-or-shuffle
        (mk-shuffle-grouper target-tasks)
;        (let [same-tasks (set/intersection
;                           (set target-tasks)
;                           (set (.getThisWorkerTasks context)))]
;          (if-not (empty? same-tasks)
;            (mk-shuffle-grouper (vec same-tasks))
;            (mk-shuffle-grouper target-tasks)))
      :none
        (fn [task-id tuple]
          (let [i (mod (.nextInt random) num-tasks)]
            (.get target-tasks i)
            ))
      :custom-object
        (let [grouping (.getCustomObject thrift-grouping)]
          (mk-custom-grouper grouping context component-id stream-id target-tasks))
      :direct
        :direct
      )))


(defn- outbound-groupings
  [^WorkerTopologyContext worker-context this-component-id stream-id out-fields component->grouping]
  (->> component->grouping
       (filter-key #(-> worker-context
                        (.getComponentTasks %)
                        count
                        pos?))
       (map (fn [[component tgrouping]]
               [component
                (mk-grouper worker-context
                            this-component-id
                            stream-id
                            out-fields
                            tgrouping
                            (.getComponentTasks worker-context component)
                            )]))
       (into {})
       (HashMap.)))


(defn outbound-components
  "Returns map of stream id to component id to grouper"
  [^WorkerTopologyContext worker-context component-id]
  (->> (.getTargets worker-context component-id)
        clojurify-structure
        (map (fn [[stream-id component->grouping]]
               [stream-id
                (outbound-groupings
                  worker-context
                  component-id
                  stream-id
                  (.getComponentOutputFields worker-context component-id stream-id)
                  component->grouping)]))
         (into {})
         (HashMap.)))


(defn mk-task-receiver
  "EventHandler that gets called on an event."
  [executor-data tuple-action-fn]
  (let [task-ids (:task-ids executor-data)
        debug? (= true (-> executor-data :storm-conf (get TOPOLOGY-DEBUG)))]
    (disruptor/clojure-handler
      (fn [[task-id tuple] sequence-id batch-end?]
        (when debug? (log-message "Processing received message " tuple))
        (if task-id
          (tuple-action-fn task-id tuple)
          ;; null task ids are broadcast tuples
          (fast-list-iter [task-id task-ids]
            (tuple-action-fn task-id tuple)
            ))
        ))))


(defn mk-executor-transfer-fn
  "Function that transfers the Tuple produced by this Executor."
  [worker]
  (fn this
    ([task tuple block? ^List overflow-buffer]
      (let [worker-transfer-fn (:transfer-fn worker)]
        (if (and overflow-buffer (not (.isEmpty overflow-buffer)))
          (.add overflow-buffer [task tuple])
          (try-cause
            (worker-transfer-fn task tuple)
;            (disruptor/publish batch-transfer->worker [task tuple] block?)
          (catch InsufficientCapacityException e
            (if overflow-buffer
              (.add overflow-buffer [task tuple])
              (throw e))
            )))))
    ([task tuple overflow-buffer]
      (this task tuple (nil? overflow-buffer) overflow-buffer))
    ([task tuple]
      (this task tuple nil)
      )))


(defn executor-type
  "Return :spout if executor is doing a Spout or
  :bolt if it is doing a Bolt."
  [^WorkerTopologyContext context component-id]
  (let [topology (.getRawTopology context)
        spouts (.getSpouts topology)
        bolts (.getBolts topology)]
    (cond (contains? spouts component-id) :spout
          (contains? bolts component-id) :bolt
          :else (throw-runtime "Could not find " component-id " in topology " topology))))


(defn- normalized-component-conf
  "Normalize component config."
  [storm-conf general-context component-id]
  (let [to-remove (disj (set ALL-CONFIGS)
                        TOPOLOGY-DEBUG
                        TOPOLOGY-MAX-SPOUT-PENDING
                        TOPOLOGY-MAX-TASK-PARALLELISM
                        TOPOLOGY-TICK-TUPLE-FREQ-SECS
                        TOPOLOGY-SLEEP-SPOUT-WAIT-STRATEGY-TIME-MS
                        TOPOLOGY-SPOUT-WAIT-STRATEGY
                        )
        spec-conf (-> general-context
                    (.getComponentCommon component-id)
                    .getConf
                    clojurify-structure)]
    (merge storm-conf (apply dissoc spec-conf to-remove))
    ))


(defn- tuple-execute-time-delta!
  ""
  [^TupleImpl tuple]
  (let [ms (.getExecuteSampleStartTime tuple)]
    (if ms
      (time-delta-ms ms))))


(defn init-spout-wait-strategy
  ""
  [storm-conf]
  (let [ret (-> storm-conf (get TOPOLOGY-SPOUT-WAIT-STRATEGY) new-instance)]
    (.prepare ret storm-conf)
    ret
    ))


(defn executor-max-spout-pending
  ""
  [storm-conf num-tasks]
  (let [p (storm-conf TOPOLOGY-MAX-SPOUT-PENDING)]
    (if p (* p num-tasks))))


(defmethod mk-threads :spout [executor-data task-datas]
  (let [{:keys [storm-conf component-id worker-context transfer-fn report-error open-or-prepare-was-called?]} executor-data
        ^ISpoutWaitStrategy spout-wait-strategy (init-spout-wait-strategy storm-conf)
        max-spout-pending (executor-max-spout-pending storm-conf (count task-datas))
        ^Integer max-spout-pending (if max-spout-pending (int max-spout-pending))
        last-active (atom false)
        spouts (ArrayList. (map :object (vals task-datas)))
        rand (Random. (Utils/secureRandomLong))

        pending (RotatingMap.
                 2 ;; microoptimize for performance of .size method
                 (reify RotatingMap$ExpiredCallback
                   (expire [this msg-id [task-id spout-id tuple-info start-time-ms]]
                     (let [time-delta (if start-time-ms (time-delta-ms start-time-ms))]
;                       (fail-spout-msg executor-data (get task-datas task-id) spout-id tuple-info time-delta)
                       ))))
        tuple-action-fn (fn [task-id ^TupleImpl tuple]
                          (let [stream-id (.getSourceStreamId tuple)]
                            (condp = stream-id
                              Constants/SYSTEM_TICK_STREAM_ID (.rotate pending)
;                              Constants/METRICS_TICK_STREAM_ID (metrics-tick executor-data (get task-datas task-id) tuple)
                              (let [id (.getValue tuple 0)
                                    [stored-task-id spout-id tuple-finished-info start-time-ms] (.remove pending id)]
                                (when spout-id
                                  (when-not (= stored-task-id task-id)
                                    (throw-runtime "Fatal error, mismatched task ids: " task-id " " stored-task-id)))
                                   ;; TODO: consider uncommenting below
;                                  (let [time-delta (if start-time-ms (time-delta-ms start-time-ms))]
;                                    (condp = stream-id
;                                      ACKER-ACK-STREAM-ID (ack-spout-msg executor-data (get task-datas task-id)
;                                                                         spout-id tuple-finished-info time-delta)
;                                      ACKER-FAIL-STREAM-ID (fail-spout-msg executor-data (get task-datas task-id)
;                                                                           spout-id tuple-finished-info time-delta)
;                                      )))
                                ;; TODO: on failure, emit tuple to failure stream
                                ))))
        receive-queue (:receive-queue executor-data)
        event-handler (mk-task-receiver executor-data tuple-action-fn)
        emitted-count (MutableLong. 0)
        empty-emit-streak (MutableLong. 0)

        ;; the overflow buffer is used to ensure that spouts never block when emitting
        ;; this ensures that the spout can always clear the incoming buffer (acks and fails), which
        ;; prevents deadlock from occuring across the topology (e.g. Spout -> Bolt -> Acker -> Spout, and all
        ;; buffers filled up)
        ;; when the overflow buffer is full, spouts stop calling nextTuple until it's able to clear the overflow buffer
        ;; this limits the size of the overflow buffer to however many tuples a spout emits in one call of nextTuple,
        ;; preventing memory issues
        overflow-buffer (LinkedList.)]

    [(async-loop
      (fn []
        ;; If topology was started in inactive state, don't call (.open spout) until it's activated first.
        (while (not @(:storm-active-atom executor-data))
          (log-message "sleeping")
          (Thread/sleep 100))

        (log-message "Opening spout " component-id ":" (keys task-datas))
        (doseq [[task-id task-data] task-datas
                :let [^ISpout spout-obj (:object task-data)
                      tasks-fn (:tasks-fn task-data)
                      send-spout-msg (fn [out-stream-id values message-id out-task-id]
                                       (.increment emitted-count)
                                       (let [out-tasks (if out-task-id
                                                         (tasks-fn out-task-id out-stream-id values)
                                                         (tasks-fn out-stream-id values))]
                                         (fast-list-iter [out-task out-tasks]
                                                         (let [tuple-id (MessageId/makeUnanchored)
                                                               out-tuple (TupleImpl. worker-context
                                                                                     values
                                                                                     task-id
                                                                                     out-stream-id
                                                                                     tuple-id)]
                                                           (transfer-fn out-task
                                                                        out-tuple
                                                                        overflow-buffer)
                                                           ))
                                         (or out-tasks [])
                                         ))]]
;          (builtin-metrics/register-all (:builtin-metrics task-data) storm-conf (:user-context task-data))
;          (builtin-metrics/register-queue-metrics {:sendqueue (:batch-transfer-queue executor-data)
;                                                   :receive receive-queue}
;                                                  storm-conf (:user-context task-data))

          (.open spout-obj
                 storm-conf
                 (:user-context task-data)
                 (SpoutOutputCollector.
                  (reify ISpoutOutputCollector
                    (^List emit [this ^String stream-id ^List tuple ^Object message-id]
                      (send-spout-msg stream-id tuple message-id nil)
                      )
                    (^void emitDirect [this ^int out-task-id ^String stream-id
                                       ^List tuple ^Object message-id]
                      (send-spout-msg stream-id tuple message-id out-task-id)
                      )
                    (reportError [this error]
                      (report-error error))
                      ))))
        (reset! open-or-prepare-was-called? true)
        (log-message "Opened spout " component-id ":" (keys task-datas))
;        (setup-metrics! executor-data)

        (disruptor/consumer-started! (:receive-queue executor-data))
        (fn []
          ;; This design requires that spouts be non-blocking
          (disruptor/consume-batch receive-queue event-handler)

          ;; try to clear the overflow-buffer
          (try-cause
            (while (not (.isEmpty overflow-buffer))
              (let [[out-task out-tuple] (.peek overflow-buffer)]
                (transfer-fn out-task out-tuple false nil)
                (.removeFirst overflow-buffer)))
          (catch InsufficientCapacityException e
            ))

          (let [active? @(:storm-active-atom executor-data)
                curr-count (.get emitted-count)]
            (if (and (.isEmpty overflow-buffer)
                     (or (not max-spout-pending)
                         (< (.size pending) max-spout-pending)))
              (if active?
                (do
                  (when-not @last-active
                    (reset! last-active true)
                    (log-message "Activating spout " component-id ":" (keys task-datas))
                    (fast-list-iter [^ISpout spout spouts] (.activate spout)))

                  (fast-list-iter [^ISpout spout spouts] (.nextTuple spout)))
                (do
                  (when @last-active
                    (reset! last-active false)
                    (log-message "Deactivating spout " component-id ":" (keys task-datas))
                    (fast-list-iter [^ISpout spout spouts] (.deactivate spout)))
                  ;; TODO: log that it's getting throttled
                  (Time/sleep 100))))
            (if (and (= curr-count (.get emitted-count)) active?)
              (do (.increment empty-emit-streak)
                  (.emptyEmit spout-wait-strategy (.get empty-emit-streak)))
              (.set empty-emit-streak 0)
              ))
          0))
      :kill-fn (:report-error-and-die executor-data)
      :factory? true
      :thread-name component-id)]))


(defmethod mk-threads :bolt [executor-data task-datas]
  (let [execute-sampler (mk-stats-sampler (:storm-conf executor-data))
        executor-stats (:stats executor-data)
        {:keys [storm-conf component-id worker-context transfer-fn report-error open-or-prepare-was-called?]} executor-data
        rand (Random. (Utils/secureRandomLong))
        tuple-action-fn (fn [task-id ^TupleImpl tuple]
                          ;; synchronization needs to be done with a key provided by this bolt, otherwise:
                          ;; spout 1 sends synchronization (s1), dies, same spout restarts somewhere else, sends synchronization (s2) and incremental update. s2 and update finish before s1 -> lose the incremental update
                          ;; TODO: for state sync, need to first send sync messages in a loop and receive tuples until synchronization
                          ;; buffer other tuples until fully synchronized, then process all of those tuples
                          ;; then go into normal loop
                          ;; spill to disk?
                          ;; could be receiving incremental updates while waiting for sync or even a partial sync because of another failed task
                          ;; should remember sync requests and include a random sync id in the request. drop anything not related to active sync requests
                          ;; or just timeout the sync messages that are coming in until full sync is hit from that task
                          ;; need to drop incremental updates from tasks where waiting for sync. otherwise, buffer the incremental updates
                          ;; TODO: for state sync, need to check if tuple comes from state spout. if so, update state
                          ;; TODO: how to handle incremental updates as well as synchronizations at same time
                          ;; TODO: need to version tuples somehow

                          ;;(log-debug "Received tuple " tuple " at task " task-id)
                          ;; need to do it this way to avoid reflection
                          (let [stream-id (.getSourceStreamId tuple)]
                            (condp = stream-id
;                              Constants/METRICS_TICK_STREAM_ID (metrics-tick executor-data (get task-datas task-id) tuple)
                              (let [task-data (get task-datas task-id)
                                    ^IBolt bolt-obj (:object task-data)
                                    user-context (:user-context task-data)]
;                                    sampler? (sampler)
;                                    execute-sampler? (execute-sampler)
;                                    now (if (or sampler? execute-sampler?) (System/currentTimeMillis))]
;                                (when sampler?
;                                  (.setProcessSampleStartTime tuple now))
;                                (when execute-sampler?
;                                  (.setExecuteSampleStartTime tuple now))
                                (.execute bolt-obj tuple)
                                (let [delta (tuple-execute-time-delta! tuple)]
;                                  (task/apply-hooks user-context .boltExecute (BoltExecuteInfo. tuple task-id delta))
                                  (when delta
;                                    (builtin-metrics/bolt-execute-tuple! (:builtin-metrics task-data)
;                                                                         executor-stats
;                                                                         (.getSourceComponent tuple)
;                                                                         (.getSourceStreamId tuple)
;                                                                         delta)
                                    (stats/bolt-execute-tuple! executor-stats
                                                               (.getSourceComponent tuple)
                                                               (.getSourceStreamId tuple)
                                                               delta)))))))]

    ;; TODO: can get any SubscribedState objects out of the context now

    [(async-loop
      (fn []
        ;; If topology was started in inactive state, don't call prepare bolt until it's activated first.
        (while (not @(:storm-active-atom executor-data))
          (Thread/sleep 100))

        (log-message "Preparing bolt " component-id ":" (keys task-datas))
        (doseq [[task-id task-data] task-datas
                :let [^IBolt bolt-obj (:object task-data)
                      tasks-fn (:tasks-fn task-data)
                      user-context (:user-context task-data)
                      bolt-emit (fn [stream anchors values task]
                                  (let [out-tasks (if task
                                                    (tasks-fn task stream values)
                                                    (tasks-fn stream values))]
                                    (fast-list-iter [t out-tasks]
                                                    (let [anchors-to-ids (HashMap.)]
;                                                      (fast-list-iter [^TupleImpl a anchors]
;                                                                      (let [root-ids (-> a .getMessageId .getAnchorsToIds .keySet)]
;                                                                        (when (pos? (count root-ids))
;                                                                          (let [edge-id (MessageId/generateId rand)]
;                                                                            (.updateAckVal a edge-id)
;                                                                            (fast-list-iter [root-id root-ids]
;                                                                                            (put-xor! anchors-to-ids root-id edge-id))
;                                                                            ))))
                                                      (transfer-fn t
                                                                   (TupleImpl. worker-context
                                                                               values
                                                                               task-id
                                                                               stream
                                                                               (MessageId/makeId anchors-to-ids)))))
                                    (or out-tasks [])))]]
;          (builtin-metrics/register-all (:builtin-metrics task-data) storm-conf user-context)
;          (if (= component-id Constants/SYSTEM_COMPONENT_ID)
;            (builtin-metrics/register-queue-metrics {:sendqueue (:batch-transfer-queue executor-data)
;                                                     :receive (:receive-queue executor-data)
;                                                     :transfer (:transfer-queue (:worker executor-data))}
;                                                    storm-conf user-context)
;            (builtin-metrics/register-queue-metrics {:sendqueue (:batch-transfer-queue executor-data)
;                                                     :receive (:receive-queue executor-data)}
;                                                    storm-conf user-context)
;            )

          (.prepare bolt-obj
                    storm-conf
                    user-context
                    (OutputCollector.
                     (reify IOutputCollector
                       (emit [this stream anchors values]
                         (bolt-emit stream anchors values nil))
                       (emitDirect [this task stream anchors values]
                         (bolt-emit stream anchors values task))
                       (^void ack [this ^Tuple tuple]
                         )
;                         (let [^TupleImpl tuple tuple
;                               ack-val (.getAckVal tuple)]
;                           (fast-map-iter [[root id] (.. tuple getMessageId getAnchorsToIds)]
;                                          (task/send-unanchored task-data
;                                                                ACKER-ACK-STREAM-ID
;                                                                [root (bit-xor id ack-val)])
;                                          ))
;                         (let [delta (tuple-time-delta! tuple)]
;                           (task/apply-hooks user-context .boltAck (BoltAckInfo. tuple task-id delta))
;                           (when delta
;                             (builtin-metrics/bolt-acked-tuple! (:builtin-metrics task-data)
;                                                                executor-stats
;                                                                (.getSourceComponent tuple)
;                                                                (.getSourceStreamId tuple)
;                                                                delta)
;                             (stats/bolt-acked-tuple! executor-stats
;                                                      (.getSourceComponent tuple)
;                                                      (.getSourceStreamId tuple)
;                                                      delta))))
                       (^void fail [this ^Tuple tuple]
                         )
;                         (fast-list-iter [root (.. tuple getMessageId getAnchors)]
;                                         (task/send-unanchored task-data
;                                                               ACKER-FAIL-STREAM-ID
;                                                               [root])))
;                         (let [delta (tuple-time-delta! tuple)]
;                           (task/apply-hooks user-context .boltFail (BoltFailInfo. tuple task-id delta))
;                           (when delta
;                             (builtin-metrics/bolt-failed-tuple! (:builtin-metrics task-data)
;                                                                 executor-stats
;                                                                 (.getSourceComponent tuple)
;                                                                 (.getSourceStreamId tuple))
;                             (stats/bolt-failed-tuple! executor-stats
;                                                       (.getSourceComponent tuple)
;                                                       (.getSourceStreamId tuple)
;                                                       delta))))
                       (reportError [this error]
                         (report-error error))
                         ))))
        (reset! open-or-prepare-was-called? true)
        (log-message "Prepared bolt " component-id ":" (keys task-datas))
;        (setup-metrics! executor-data)

        (let [receive-queue (:receive-queue executor-data)
              event-handler (mk-task-receiver executor-data tuple-action-fn)]
          (disruptor/consumer-started! receive-queue)
          (fn []
            (disruptor/consume-batch-when-available receive-queue event-handler)
            0)))
      :kill-fn (:report-error-and-die executor-data)
      :factory? true
      :thread-name component-id)]))


(defn mk-executor-data [worker executor-id]
  (let [worker-context (worker-context worker)
        task-ids (executor-id->tasks executor-id)
        component-id (.getComponentId worker-context (first task-ids))
        storm-conf (normalized-component-conf (:storm-conf worker) worker-context component-id)
        executor-type (executor-type worker-context component-id)
        ]
    (recursive-map
     :worker worker
     :worker-context worker-context
     :executor-id executor-id
     :task-ids task-ids
     :component-id component-id
     :open-or-prepare-was-called? (atom false)
     :storm-conf storm-conf
     :receive-queue ((:executor-receive-queue-map worker) executor-id)
     :storm-id (:storm-id worker)
     :conf (:conf worker)
     :shared-executor-data (HashMap.)
     :storm-active-atom (:storm-active-atom worker)
     :transfer-fn (mk-executor-transfer-fn worker)
     :suicide-fn (:suicide-fn worker)
;     :storm-cluster-state (cluster/mk-storm-cluster-state (:cluster-state worker))
     :type executor-type
     ;; TODO: should refactor this to be part of the executor specific map (spout or bolt with :common field)
     :stats (mk-executor-stats <> (sampling-rate storm-conf))
     :interval->task->metric-registry (HashMap.)
     :task->component (:task->component worker)
     :stream->component->grouper (outbound-components worker-context component-id)
     :report-error (throttled-report-error-fn <>)
     :report-error-and-die (fn [error]
                             ((:report-error <>) error)
                             ((:suicide-fn <>)))
;     :deserializer (KryoTupleDeserializer. storm-conf worker-context)
;     :sampler (mk-stats-sampler storm-conf)
     ;; TODO: add in the executor-specific stuff in a :specific... or make a spout-data, bolt-data function?
     )))


(defn mk-executor [worker executor-id]
  (let [executor-data (mk-executor-data worker executor-id)
        _ (log-message "Loading executor " (:component-id executor-data) ":" (pr-str executor-id))
        ;; create task for each id
        ;; TODO: initialize tasks here
        task-datas (->> executor-data
                        :task-ids
                        (map (fn [t] [t (task/mk-task executor-data t)]))
                        (into {})
                        (HashMap.))
        _ (log-message "Loaded executor tasks " (:component-id executor-data) ":" (pr-str executor-id))
        report-error-and-die (:report-error-and-die executor-data)
        component-id (:component-id executor-data)

        handlers (with-error-reaction report-error-and-die
                   (mk-threads executor-data task-datas))]
    (setup-ticks! worker executor-data)

    (log-message "Finished loading executor " component-id ":" (pr-str executor-id))
    ;; TODO: add method here to get rendered stats... have worker call that when heartbeating
    (reify
      RunningExecutor
      (render-stats [this]
        (stats/render-stats! (:stats executor-data)))
      (get-executor-id [this]
        executor-id )
      Shutdownable
      (shutdown
        [this]
        (log-message "Shutting down executor " component-id ":" (pr-str executor-id))
        (disruptor/halt-with-interrupt! (:receive-queue executor-data))
;        (disruptor/halt-with-interrupt! (:batch-transfer-queue executor-data))
        (doseq [h handlers]
          (.interrupt h)
          (.join h))

;        (doseq [user-context (map :user-context (vals task-datas))]
;          (doseq [hook (.getHooks user-context)]
;            (.cleanup hook)))
        (when @(:open-or-prepare-was-called? executor-data)
          (doseq [obj (map :object (vals task-datas))]
            (close-component executor-data obj)))
        (log-message "Shut down executor " component-id ":" (pr-str executor-id)))
        )))


(defmethod mk-executor-stats :spout [_ rate]
  (stats/mk-spout-stats rate))


(defmethod mk-executor-stats :bolt [_ rate]
  (stats/mk-bolt-stats rate))
