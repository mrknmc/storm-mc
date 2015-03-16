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
package backtype.storm.task;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.ThriftTopologyUtils;
import backtype.storm.utils.Utils;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import org.json.simple.JSONAware;
import org.json.simple.JSONValue;

import java.util.*;

public class GeneralTopologyContext implements JSONAware {
    private StormTopology _topology;
    private Map<Integer, String> _taskToComponent;
    private Map<String, List<Integer>> _componentToTasks;
    private Map<String, Map<String, Fields>> _componentToStreamToFields;
    private String _stormId;
    protected Map _stormConf;
    
    // pass in componentToSortedTasks for the case of running tons of tasks in single executor
    public GeneralTopologyContext(StormTopology topology, Map stormConf,
                                  Map<Integer, String> taskToComponent, Map<String, List<Integer>> componentToSortedTasks,
                                  Map<String, Map<String, Fields>> componentToStreamToFields, String stormId) {
        _topology = topology;
        _stormConf = stormConf;
        _taskToComponent = taskToComponent;
        _stormId = stormId;
        _componentToTasks = componentToSortedTasks;
        _componentToStreamToFields = componentToStreamToFields;
    }

    /**
     * Gets the unique id assigned to this topology. The id is the storm name with a
     * unique nonce appended to it.
     * @return the storm id
     */
    public String getStormId() {
        return _stormId;
    }

    /**
     * Gets the Thrift object representing the topology.
     * 
     * @return the Thrift definition representing the topology
     */
    public StormTopology getRawTopology() {
        return _topology;
    }

    /**
     * Gets the component id for the specified task id. The component id maps
     * to a component id specified for a Spout or Bolt in the topology definition.
     *
     * @param taskId the task id
     * @return the component id for the input task id
     */
    public String getComponentId(int taskId) {
        if(taskId==Constants.SYSTEM_TASK_ID) {
            return Constants.SYSTEM_COMPONENT_ID;
        } else {
            return _taskToComponent.get(taskId);
        }
    }

    /**
     * Gets the set of streams declared for the specified component.
     */
    public Set<String> getComponentStreams(String componentId) {
        return getComponentCommon(componentId).getStreams().keySet();
    }

    /**
     * Gets the task ids allocated for the given component id. The task ids are
     * always returned in ascending order.
     */
    public List<Integer> getComponentTasks(String componentId) {
        List<Integer> ret = _componentToTasks.get(componentId);
        if(ret==null) return new ArrayList<Integer>();
        else return new ArrayList<Integer>(ret);
    }

    /**
     * Gets the declared output fields for the specified component/stream.
     */
    public Fields getComponentOutputFields(String componentId, String streamId) {
        // hack to make ticks work
        if (componentId.equals(Constants.SYSTEM_COMPONENT_ID) && streamId.equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            return new Fields("rate_secs");
        }
        Fields ret = _componentToStreamToFields.get(componentId).get(streamId);
        if(ret==null) {
            throw new IllegalArgumentException("No output fields defined for component:stream " + componentId + ":" + streamId);
        }
        return ret;
    }

    /**
     * Gets the declared output fields for the specified global stream id.
     */
    public Fields getComponentOutputFields(GlobalStreamId id) {
        return getComponentOutputFields(id.getComponentId(), id.getStreamId());
    }    
    
    /**
     * Gets the declared inputs to the specified component.
     *
     * @return A map from subscribed component/stream to the grouping subscribed with.
     */
    public Map<GlobalStreamId, Grouping> getSources(String componentId) {
        return getComponentCommon(componentId).getInputs();
    }

    /**
     * Gets information about who is consuming the outputs of the specified component,
     * and how.
     *
     * @return Map from stream id to component id to the Grouping used.
     */
    public Map<String, Map<String, Grouping>> getTargets(String componentId) {
        Map<String, Map<String, Grouping>> ret = new HashMap<String, Map<String, Grouping>>();
        for(String otherComponentId: getComponentIds()) {
            Map<GlobalStreamId, Grouping> inputs = getComponentCommon(otherComponentId).getInputs();
            for(GlobalStreamId id: inputs.keySet()) {
                if(id.getComponentId().equals(componentId)) {
                    Map<String, Grouping> curr = ret.get(id.getStreamId());
                    if(curr==null) curr = new HashMap<String, Grouping>();
                    curr.put(otherComponentId, inputs.get(id));
                    ret.put(id.getStreamId(), curr);
                }
            }
        }
        return ret;
    }

    @Override
    public String toJSONString() {
        Map obj = new HashMap();
        obj.put("task->component", _taskToComponent);
        // TODO: jsonify StormTopology
        // at the minimum should send source info
        return JSONValue.toJSONString(obj);
    }

    /**
     * Gets a map from task id to component id.
     */
    public Map<Integer, String> getTaskToComponent() {
        return _taskToComponent;
    }
    
    /**
     * Gets a list of all component ids in this topology
     */
    public Set<String> getComponentIds() {
        return ThriftTopologyUtils.getComponentIds(getRawTopology());
    }

    public ComponentCommon getComponentCommon(String componentId) {
        return ThriftTopologyUtils.getComponentCommon(getRawTopology(), componentId);
    }

}