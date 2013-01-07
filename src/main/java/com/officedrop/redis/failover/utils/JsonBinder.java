package com.officedrop.redis.failover.utils;

import com.officedrop.redis.failover.ClusterStatus;
import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.NodeState;

import java.util.Map;

/**
 * User: Maurício Linhares
 * Date: 1/3/13
 * Time: 10:05 PM
 */
public interface JsonBinder {

    public byte[] toBytes(Map<HostConfiguration, NodeState> nodeStates);

    public Map<HostConfiguration,NodeState> toNodeState(byte[] data);

    public byte[] toBytes( ClusterStatus clusterStatus);

    public ClusterStatus toClusterStatus( byte[] data  );

}
