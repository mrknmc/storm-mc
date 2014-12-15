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
package backtype.storm.topology;

import backtype.storm.Config;
import backtype.storm.generated.*;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import org.json.simple.JSONValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * TopologyBuilder exposes the Java API for specifying a topology for Storm
 * to execute. Topologies are Thrift structures in the end, but since the Thrift API
 * is so verbose, TopologyBuilder greatly eases the process of creating topologies.
 * The template for creating and submitting a topology looks something like:
 *
 * <pre>
 * TopologyBuilder builder = new TopologyBuilder();
 *
 * builder.setSpout("1", new TestWordSpout(true), 5);
 * builder.setSpout("2", new TestWordSpout(true), 3);
 * builder.setBolt("3", new TestWordCounter(), 3)
 *          .fieldsGrouping("1", new Fields("word"))
 *          .fieldsGrouping("2", new Fields("word"));
 * builder.setBolt("4", new TestGlobalCount())
 *          .globalGrouping("1");
 *
 * Map conf = new HashMap();
 * conf.put(Config.TOPOLOGY_WORKERS, 4);
 * 
 * StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
 * </pre>
 *
 * Running the exact same topology in local mode (in process), and configuring it to log all tuples
 * emitted, looks like the following. Note that it lets the topology run for 10 seconds
 * before shutting down the local cluster.
 *
 * <pre>
 * TopologyBuilder builder = new TopologyBuilder();
 *
 * builder.setSpout("1", new TestWordSpout(true), 5);
 * builder.setSpout("2", new TestWordSpout(true), 3);
 * builder.setBolt("3", new TestWordCounter(), 3)
 *          .fieldsGrouping("1", new Fields("word"))
 *          .fieldsGrouping("2", new Fields("word"));
 * builder.setBolt("4", new TestGlobalCount())
 *          .globalGrouping("1");
 *
 * Map conf = new HashMap();
 * conf.put(Config.TOPOLOGY_WORKERS, 4);
 * conf.put(Config.TOPOLOGY_DEBUG, true);
 *
 * LocalCluster cluster = new LocalCluster();
 * cluster.submitTopology("mytopology", conf, builder.createTopology());
 * Utils.sleep(10000);
 * cluster.shutdown();
 * </pre>
 *
 * <p>The pattern for TopologyBuilder is to map component ids to components using the setSpout
 * and setBolt methods. Those methods return objects that are then used to declare
 * the inputs for that component.</p>
 */
public class TopologyBuilder {
    private Map<String, IRichBolt> _bolts = new HashMap<String, IRichBolt>();
    private Map<String, IRichSpout> _spouts = new HashMap<String, IRichSpout>();
    private Map<String, ComponentCommonBuilder> _commonBuilders = new HashMap<String, ComponentCommonBuilder>();
    
    public StormTopology createTopology() {
        Map<String, Bolt> boltSpecs = new HashMap<String, Bolt>();
        Map<String, SpoutSpec> spoutSpecs = new HashMap<String, SpoutSpec>();
        for (String boltId: _bolts.keySet()) {
            IRichBolt bolt = _bolts.get(boltId);
            ComponentCommon common = getComponentCommon(boltId, bolt);
            boltSpecs.put(boltId, new Bolt(bolt, common));
        }
        for (String spoutId: _spouts.keySet()) {
            IRichSpout spout = _spouts.get(spoutId);
            ComponentCommon common = getComponentCommon(spoutId, spout);
            spoutSpecs.put(spoutId, new SpoutSpec(spout, common));
        }
        return new StormTopology(spoutSpecs, boltSpecs);
    }

    /**
     * Define a new bolt in this topology with parallelism of just one thread.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @return use the returned object to declare the inputs to this component
     */
    public BoltDeclarer setBolt(String id, IRichBolt bolt) {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology with the specified amount of parallelism.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somewhere around the cluster.
     * @return use the returned object to declare the inputs to this component
     */
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint) {
        validateUnusedId(id);
        initCommonBuilder(id, bolt, parallelism_hint);
        _bolts.put(id, bolt);
        return new BoltGetter(id);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @return use the returned object to declare the inputs to this component
     */
    public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somwehere around the cluster.
     * @return use the returned object to declare the inputs to this component
     */
    public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelism_hint) {
        return setBolt(id, new BasicBoltExecutor(bolt), parallelism_hint);
    }

    /**
     * Define a new spout in this topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param spout the spout
     */
    public SpoutDeclarer setSpout(String id, IRichSpout spout) {
        return setSpout(id, spout, null);
    }

    /**
     * Define a new spout in this topology with the specified parallelism. If the spout declares
     * itself as non-distributed, the parallelism_hint will be ignored and only one task
     * will be allocated to this component.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param parallelism_hint the number of tasks that should be assigned to execute this spout. Each task will run on a thread in a process somwehere around the cluster.
     * @param spout the spout
     */
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism_hint) {
        validateUnusedId(id);
        initCommonBuilder(id, spout, parallelism_hint);
        _spouts.put(id, spout);
        return new SpoutGetter(id);
    }

    private void validateUnusedId(String id) {
        if (_bolts.containsKey(id)) {
            throw new IllegalArgumentException("Bolt has already been declared for id " + id);
        }
        if (_spouts.containsKey(id)) {
            throw new IllegalArgumentException("Spout has already been declared for id " + id);
        }
    }

    private ComponentCommon getComponentCommon(String id, IComponent component) {
        ComponentCommonBuilder commonBuilder = _commonBuilders.get(id);
        OutputFieldsGetter getter = new OutputFieldsGetter();
        component.declareOutputFields(getter);
        commonBuilder.setStreams(getter.getFieldsDeclaration());
        return commonBuilder.build();
    }
    
    private void initCommonBuilder(String id, IComponent component, Number parallelismHint) {
        ComponentCommonBuilder commonBuilder = new ComponentCommonBuilder();
        Map<String, Object> conf = component.getComponentConfiguration();
        if (parallelismHint != null) {
            commonBuilder.setParallelismHint(parallelismHint);
        }
        if (conf != null) {
            commonBuilder.setConf(conf);
        }
        _commonBuilders.put(id, commonBuilder);
    }

    protected class ConfigGetter<T extends ComponentConfigurationDeclarer> extends BaseConfigurationDeclarer<T> {
        String _id;
        
        public ConfigGetter(String id) {
            _id = id;
        }
        
        @Override
        public T addConfigurations(Map conf) {
            if (conf != null && conf.containsKey(Config.TOPOLOGY_KRYO_REGISTER)) {
                throw new IllegalArgumentException("Cannot set serializations for a component using fluent API");
            }
            _commonBuilders.get(_id).putToConf(conf);
            return (T) this;
        }
    }
    
    protected class SpoutGetter extends ConfigGetter<SpoutDeclarer> implements SpoutDeclarer {
        public SpoutGetter(String id) {
            super(id);
        }        
    }
    
    protected class BoltGetter extends ConfigGetter<BoltDeclarer> implements BoltDeclarer {
        private String _boltId;

        public BoltGetter(String boltId) {
            super(boltId);
            _boltId = boltId;
        }

        public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
            return fieldsGrouping(componentId, Utils.DEFAULT_STREAM_ID, fields);
        }

        public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
            return grouping(componentId, streamId, Grouping.fields(fields.toList()));
        }

        public BoltDeclarer globalGrouping(String componentId) {
            return globalGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer globalGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.fields(new ArrayList<String>()));
        }

        public BoltDeclarer shuffleGrouping(String componentId) {
            return shuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.shuffle());
        }

        public BoltDeclarer localOrShuffleGrouping(String componentId) {
            return localOrShuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer localOrShuffleGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.localOrShuffle());
        }
        
        public BoltDeclarer noneGrouping(String componentId) {
            return noneGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer noneGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.none());
        }

        public BoltDeclarer allGrouping(String componentId) {
            return allGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer allGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.all());
        }

        public BoltDeclarer directGrouping(String componentId) {
            return directGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer directGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.direct());
        }

        private BoltDeclarer grouping(String componentId, String streamId, Grouping grouping) {
            _commonBuilders.get(_boltId).putToInputs(new GlobalStreamId(componentId, streamId), grouping);
            return this;
        }

        @Override
        public BoltDeclarer customGrouping(String componentId, CustomStreamGrouping grouping) {
            return customGrouping(componentId, Utils.DEFAULT_STREAM_ID, grouping);
        }

        @Override
        public BoltDeclarer customGrouping(String componentId, String streamId, CustomStreamGrouping grouping) {
            return grouping(componentId, streamId, Grouping.customObject(grouping));
        }

        @Override
        public BoltDeclarer grouping(GlobalStreamId id, Grouping grouping) {
            return grouping(id.getComponentId(), id.getStreamId(), grouping);
        }
    }
    
    private static Map parseJson(String json) {
        if (json == null) {
            return new HashMap();
        } else {
            return (Map) JSONValue.parse(json);
        }
    }

}
