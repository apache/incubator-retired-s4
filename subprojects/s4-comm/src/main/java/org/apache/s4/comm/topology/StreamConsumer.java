package org.apache.s4.comm.topology;

/**
 * A subscriber to a published stream. Identified through its cluster name (for dispatching to the remote cluster) and
 * application ID (for dispatching within a node).
 * 
 */
public class StreamConsumer {

    int appId;
    String clusterName;

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

}
