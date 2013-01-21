package com.officedrop.redis.failover;

/**
 * User: Maurício Linhares
 * Date: 1/3/13
 * Time: 2:47 PM
 */
public class NodeState {

    public static final NodeState OFFLINE_STATE = new NodeState(-1, true);

    private final long latency;
    private final boolean offline;

    private NodeState( long latency, boolean offline ) {
        this.latency = latency;
        this.offline = offline;
    }

    public NodeState( long latency ) {
        this(latency, false);
    }

    public long getLatency() {
        return latency;
    }

    public boolean isOffline() {
        return offline;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof NodeState)) return false;

        NodeState nodeState = (NodeState) o;

        if (latency != nodeState.latency) return false;
        if (offline != nodeState.offline) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (latency ^ (latency >>> 32));
        result = 31 * result + (offline ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "NodeState{" +
                "latency=" + latency +
                ", offline=" + offline +
                '}';
    }
}