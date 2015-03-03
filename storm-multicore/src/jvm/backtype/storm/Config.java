/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Topology configs are specified as a plain old map. This class provides a 
 * convenient way to create a topology config map by providing setter methods for 
 * all the configs that can be set. It also makes it easier to do things like add 
 * serializations.
 * 
 * <p>This class also provides constants for all the configurations possible on
 * a Storm cluster and Storm topology. Each constant is paired with a schema
 * that defines the validity criterion of the corresponding field. Default
 * values for these configs can be found in defaults.yaml.</p>
 *
 * <p>Note that you may put other configurations in any of the configs. Storm
 * will ignore anything it doesn't recognize, but your topologies are free to make
 * use of them by reading them in the prepare method of Bolts or the open method of 
 * Spouts.</p>
 */
public class Config extends HashMap<String, Object> {
    /**
     * A directory on the local filesystem used by Storm for any local
     * filesystem usage it needs. The directory must exist and the Storm daemons must
     * have permission to read/write from this location.
     */
    public static final String STORM_LOCAL_DIR = "storm.local.dir";
    public static final Object STORM_LOCAL_DIR_SCHEMA = String.class;

    /**
     * A global task scheduler used to assign topologies's tasks to supervisors' workers.
     * 
     * If this is not set, a default system scheduler will be used.
     */
    public static final String STORM_SCHEDULER = "storm.scheduler";
    public static final Object STORM_SCHEDULER_SCHEMA = String.class;

    /**
     * The id assigned to a running topology. The id is the storm name with a unique nonce appended.
     */
    public static final String STORM_ID = "storm.id";
    public static final Object STORM_ID_SCHEMA = String.class;

    /**
     * A custom class that implements ITopologyValidator that is run whenever a
     * topology is submitted. Can be used to provide business-specific logic for
     * whether topologies are allowed to run or not.
     */
    public static final String NIMBUS_TOPOLOGY_VALIDATOR = "nimbus.topology.validator";
    public static final Object NIMBUS_TOPOLOGY_VALIDATOR_SCHEMA = String.class;

    /**
     * When set to true, Storm will log every message that's emitted.
     */
    public static final String TOPOLOGY_DEBUG = "topology.debug";
    public static final Object TOPOLOGY_DEBUG_SCHEMA = Boolean.class;

    /**
     * The serializer for communication between shell components and non-JVM
     * processes
     */
    public static final String TOPOLOGY_MULTILANG_SERIALIZER = "topology.multilang.serializer";
    public static final Object TOPOLOGY_MULTILANG_SERIALIZER_SCHEMA = String.class;

    /**
     * How many instances to create for a spout/bolt. A task runs on a thread with zero or more
     * other tasks for the same spout/bolt. The number of tasks for a spout/bolt is always
     * the same throughout the lifetime of a topology, but the number of executors (threads) for 
     * a spout/bolt can change over time. This allows a topology to scale to more or less resources 
     * without redeploying the topology or violating the constraints of Storm (such as a fields grouping
     * guaranteeing that the same value goes to the same task).
     */
    public static final String TOPOLOGY_TASKS = "topology.tasks";
    public static final Object TOPOLOGY_TASKS_SCHEMA = Number.class;

    /*
     * A list of classes implementing IMetricsConsumer (See storm.yaml.example for exact config format).
     * Each listed class will be routed all the metrics data generated by the storm metrics API. 
     * Each listed class maps 1:1 to a system bolt named __metrics_ClassName#N, and it's parallelism is configurable.
     */
    public static final String TOPOLOGY_METRICS_CONSUMER_REGISTER = "topology.metrics.consumer.register";
    public static final Object TOPOLOGY_METRICS_CONSUMER_REGISTER_SCHEMA = ConfigValidation.MapsValidator;


    /**
     * The maximum parallelism allowed for a component in this topology. This configuration is
     * typically used in testing to limit the number of threads spawned in local mode.
     */
    public static final String TOPOLOGY_MAX_TASK_PARALLELISM="topology.max.task.parallelism";
    public static final Object TOPOLOGY_MAX_TASK_PARALLELISM_SCHEMA = Number.class;


    /**
     * The maximum number of tuples that can be pending on a spout task at any given time. 
     * This config applies to individual tasks, not to spouts or topologies as a whole. 
     * 
     * A pending tuple is one that has been emitted from a spout but has not been acked or failed yet.
     * Note that this config parameter has no effect for unreliable spouts that don't tag 
     * their tuples with a message id.
     */
    public static final String TOPOLOGY_MAX_SPOUT_PENDING="topology.max.spout.pending"; 
    public static final Object TOPOLOGY_MAX_SPOUT_PENDING_SCHEMA = Number.class;

    /**
     * A class that implements a strategy for what to do when a spout needs to wait. Waiting is
     * triggered in one of two conditions:
     * 
     * 1. nextTuple emits no tuples
     * 2. The spout has hit maxSpoutPending and can't emit any more tuples
     */
    public static final String TOPOLOGY_SPOUT_WAIT_STRATEGY="topology.spout.wait.strategy"; 
    public static final Object TOPOLOGY_SPOUT_WAIT_STRATEGY_SCHEMA = String.class;

    /**
     * The amount of milliseconds the SleepEmptyEmitStrategy should sleep for.
     */
    public static final String TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS="topology.sleep.spout.wait.strategy.time.ms";
    public static final Object TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS_SCHEMA = Number.class;

    /**
     * The percentage of tuples to sample to produce stats for a task.
     */
    public static final String TOPOLOGY_STATS_SAMPLE_RATE="topology.stats.sample.rate";
    public static final Object TOPOLOGY_STATS_SAMPLE_RATE_SCHEMA = Number.class;

    /**
     * The time period that builtin metrics data in bucketed into. 
     */
    public static final String TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS="topology.builtin.metrics.bucket.size.secs";
    public static final Object TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS_SCHEMA = Number.class;

    /**
     * Whether or not to use Java serialization in a topology.
     */
    public static final String TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION="topology.fall.back.on.java.serialization";
    public static final Object TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION_SCHEMA = Boolean.class;

    /**
     * A list of task hooks that are automatically added to every spout and bolt in the topology. An example
     * of when you'd do this is to add a hook that integrates with your internal 
     * monitoring system. These hooks are instantiated using the zero-arg constructor.
     */
    public static final String TOPOLOGY_AUTO_TASK_HOOKS="topology.auto.task.hooks";
    public static final Object TOPOLOGY_AUTO_TASK_HOOKS_SCHEMA = ConfigValidation.StringsValidator;


    /**
     * The size of the Disruptor receive queue for each executor. Must be a power of 2.
     */
    public static final String TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE="topology.executor.receive.buffer.size";
    public static final Object TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE_SCHEMA = ConfigValidation.PowerOf2Validator;

    /**
     * The size of the Disruptor send queue for each executor. Must be a power of 2.
     */
    public static final String TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE="topology.executor.send.buffer.size";
    public static final Object TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE_SCHEMA = ConfigValidation.PowerOf2Validator;

    /**
     * The size of the Disruptor transfer queue for each worker.
     */
    public static final String TOPOLOGY_TRANSFER_BUFFER_SIZE="topology.transfer.buffer.size";
    public static final Object TOPOLOGY_TRANSFER_BUFFER_SIZE_SCHEMA = Number.class;

   /**
    * Configure the wait strategy used for internal queuing. Can be used to tradeoff latency
    * vs. throughput
    */
    public static final String TOPOLOGY_DISRUPTOR_WAIT_STRATEGY="topology.disruptor.wait.strategy";
    public static final Object TOPOLOGY_DISRUPTOR_WAIT_STRATEGY_SCHEMA = String.class;

   /**
    * The size of the shared thread pool for worker tasks to make use of. The thread pool can be accessed 
    * via the TopologyContext.
    */
    public static final String TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE="topology.worker.shared.thread.pool.size";
    public static final Object TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE_SCHEMA = Number.class;

    /**
     * The interval in seconds to use for determining whether to throttle error reported to Zookeeper. For example,
     * an interval of 10 seconds with topology.max.error.report.per.interval set to 5 will only allow 5 errors to be
     * reported to Zookeeper per task for every 10 second interval of time.
     */
    public static final String TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS="topology.error.throttle.interval.secs";
    public static final Object TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS_SCHEMA = Number.class;

    /**
     * See doc for TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS
     */
    public static final String TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL="topology.max.error.report.per.interval";
    public static final Object TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL_SCHEMA = Number.class;

    /**
     * Name of the topology. This config is automatically set by Storm when the topology is submitted.
     */
    public static final String TOPOLOGY_NAME="topology.name";
    public static final Object TOPOLOGY_NAME_SCHEMA = String.class;

    /**
     * Max pending tuples in one ShellBolt
     */
    public static final String TOPOLOGY_SHELLBOLT_MAX_PENDING="topology.shellbolt.max.pending";
    public static final Object TOPOLOGY_SHELLBOLT_MAX_PENDING_SCHEMA = Number.class;

    /**
     * This value is passed to spawned JVMs (e.g., Nimbus, Supervisor, and Workers)
     * for the java.library.path value. java.library.path tells the JVM where 
     * to look for native libraries. It is necessary to set this config correctly since
     * Storm uses the ZeroMQ and JZMQ native libs. 
     */
    public static final String JAVA_LIBRARY_PATH = "java.library.path";
    public static final Object JAVA_LIBRARY_PATH_SCHEMA = String.class;

    /**
     * A map from topology name to the number of machines that should be dedicated for that topology. Set storm.scheduler
     * to backtype.storm.scheduler.IsolationScheduler to make use of the isolation scheduler.
     */
    public static final String ISOLATION_SCHEDULER_MACHINES = "isolation.scheduler.machines";
    public static final Object ISOLATION_SCHEDULER_MACHINES_SCHEMA = Map.class;

    public static void setDebug(Map conf, boolean isOn) {
        conf.put(Config.TOPOLOGY_DEBUG, isOn);
    } 

    public void setDebug(boolean isOn) {
        setDebug(this, isOn);
    }

    public void registerMetricsConsumer(Class klass, Object argument, long parallelismHint) {
        HashMap m = new HashMap();
        m.put("class", klass.getCanonicalName());
        m.put("parallelism.hint", parallelismHint);
        m.put("argument", argument);

        List l = (List)this.get(TOPOLOGY_METRICS_CONSUMER_REGISTER);
        if(l == null) { l = new ArrayList(); }
        l.add(m);
        this.put(TOPOLOGY_METRICS_CONSUMER_REGISTER, l);
    }

    public void registerMetricsConsumer(Class klass, long parallelismHint) {
        registerMetricsConsumer(klass, null, parallelismHint);
    }

    public void registerMetricsConsumer(Class klass) {
        registerMetricsConsumer(klass, null, 1L);
    }
    
    public static void setMaxTaskParallelism(Map conf, int max) {
        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, max);
    }

    public void setMaxTaskParallelism(int max) {
        setMaxTaskParallelism(this, max);
    }
    
    public static void setMaxSpoutPending(Map conf, int max) {
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, max);
    }

    public void setMaxSpoutPending(int max) {
        setMaxSpoutPending(this, max);
    }
    
    public static void setStatsSampleRate(Map conf, double rate) {
        conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, rate);
    }    

    public void setStatsSampleRate(double rate) {
        setStatsSampleRate(this, rate);
    }    

    public static void setFallBackOnJavaSerialization(Map conf, boolean fallback) {
        conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, fallback);
    }    

    public void setFallBackOnJavaSerialization(boolean fallback) {
        setFallBackOnJavaSerialization(this, fallback);
    }    

}
