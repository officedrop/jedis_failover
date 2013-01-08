package com.officedrop.redis.failover;

/**
 * User: Maurício Linhares
 * Date: 1/8/13
 * Time: 4:54 PM
 */
public interface ClusterChangeEventSource {

    public void addListeners(NodeManagerListener... listeners);

    public void removeListeners( NodeManagerListener ... listeners );

    public ClusterStatus getLastClusterStatus();

}
