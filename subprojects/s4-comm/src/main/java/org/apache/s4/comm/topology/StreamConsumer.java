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

import java.util.Map;

/**
 * A subscriber to a published stream. Identified through its cluster name (for dispatching to the remote cluster) and
 * application ID (for dispatching within a node (NOTE: this parameter is ignored)).
 * 
 */
public class StreamConsumer {

    int appId;
    String clusterName;

    /**
     * The keys are PE prototype ids.
     */
    Map<String, PartitionData> pePartitionInfo;

    public StreamConsumer(int appId, String clusterName) {
        super();
        this.appId = appId;
        this.clusterName = clusterName;
    }

    public int getAppId() {
        return appId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public Map<String, PartitionData> getPePartitionInfo() {
        return pePartitionInfo;
    }

    public void setPePartitionInfo(Map<String, PartitionData> pePartitionInfo) {
        this.pePartitionInfo = pePartitionInfo;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + appId;
        result = prime * result + ((clusterName == null) ? 0 : clusterName.hashCode());
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
        StreamConsumer other = (StreamConsumer) obj;
        if (appId != other.appId)
            return false;
        if (clusterName == null) {
            if (other.clusterName != null)
                return false;
        } else if (!clusterName.equals(other.clusterName))
            return false;
        return true;
    }

    public String toString() {
        return "appId: " + appId + ", clusterName: " + clusterName + ", pePartitionInfo: " + pePartitionInfo;
    }

}
