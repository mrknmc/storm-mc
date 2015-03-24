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
package backtype.storm.scheduler;

import java.util.*;

public class Cluster {

    /**
     * key: topologyId, value: topology's current assignments.
     */
    private SchedulerAssignmentImpl assignment;

    private List<WorkerSlot> schedulingSlots;

    private final INimbus inimbus;

    public Cluster(INimbus nimbus, List<WorkerSlot> schedulingSlots) {
        this.inimbus = nimbus;
        this.schedulingSlots = schedulingSlots;
    }

    public boolean isSlotOccupied(WorkerSlot slot) {
        if (assignment == null) {
            return false;
        }

        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : assignment.getExecutorToSlot().entrySet()) {
            if (entry.getValue().equals(slot)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return all the available slots on this supervisor.
     *
     * @param cluster
     * @return
     */
    public List<WorkerSlot> getAvailableSlots() {
        return schedulingSlots;
    }

    /**
     * Assign the slot to the executors for this topology.
     * 
     * @throws RuntimeException if the specified slot is already occupied.
     */
    public void assign(WorkerSlot slot, String topologyId, Collection<ExecutorDetails> executors) {
        if (this.isSlotOccupied(slot)) {
            throw new RuntimeException("slot: [" + slot.getUUID() + "] is already occupied.");
        }

        if (assignment == null) {
            assignment = new SchedulerAssignmentImpl(topologyId, new HashMap<ExecutorDetails, WorkerSlot>());
        } else {
            for (ExecutorDetails executor : executors) {
                 if (assignment.isExecutorAssigned(executor)) {
                     throw new RuntimeException("the executor is already assigned, you should unassign it before assign it to another slot.");
                 }
            }
        }

        assignment.assign(slot, executors);
    }

    public SchedulerAssignment getAssignment() {
        return assignment;
    }

}
