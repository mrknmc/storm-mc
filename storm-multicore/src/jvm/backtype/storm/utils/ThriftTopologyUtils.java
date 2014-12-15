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
package backtype.storm.utils;


import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ThriftTopologyUtils {

    public static Set<String> getComponentIds(StormTopology topology) {
        Set<String> ret = new HashSet<String>();
        ret.addAll(topology.getBolts().keySet());
        ret.addAll(topology.getSpouts().keySet());
        return ret;
    }

    public static ComponentCommon getComponentCommon(StormTopology topology, String componentId) {
        Map<String, SpoutSpec> spoutsMap = topology.getSpouts();
        Map<String, Bolt> boltsMap = topology.getBolts();

        if (spoutsMap.containsKey(componentId)) {
            return spoutsMap.get(componentId).getCommon();
        } else if (boltsMap.containsKey(componentId)) {
            return boltsMap.get(componentId).getCommon();
        } else {
            throw new IllegalArgumentException("Could not find component common for " + componentId);
        }
    }
}
