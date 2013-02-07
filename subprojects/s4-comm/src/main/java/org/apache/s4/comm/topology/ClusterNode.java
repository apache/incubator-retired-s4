/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.comm.topology;

/**
 * Represents an node.
 * 
 */
public class ClusterNode {
    private final int partition;
    private final int port;
    private final String machineName;
    private final String taskId;

    public ClusterNode(int partition, int port, String machineName, String taskId) {
        this.partition = partition;
        this.port = port;
        this.machineName = machineName;
        this.taskId = taskId;
    }

    public int getPartition() {
        return partition;
    }

    public int getPort() {
        return port;
    }

    public String getMachineName() {
        return machineName;
    }

    public String getTaskId() {
        return taskId;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("{").append("partition=").append(partition).append(",port=").append(port).append(",machineName=")
                .append(machineName).append(",taskId=").append(taskId).append("}");
        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((machineName == null) ? 0 : machineName.hashCode());
        result = prime * result + partition;
        result = prime * result + port;
        result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ClusterNode other = (ClusterNode) obj;
        if (machineName == null) {
            if (other.machineName != null)
                return false;
        } else if (!machineName.equals(other.machineName))
            return false;
        if (partition != other.partition)
            return false;
        if (port != other.port)
            return false;
        if (taskId == null) {
            if (other.taskId != null)
                return false;
        } else if (!taskId.equals(other.taskId))
            return false;
        return true;
    }

}
