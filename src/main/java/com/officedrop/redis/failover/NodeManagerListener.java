package com.officedrop.redis.failover;

/**
 * User: Maurício Linhares
 * Date: 1/7/13
 * Time: 7:48 AM
 */
public interface NodeManagerListener {

    public void masterChanged( NodeManager manager, ClusterStatus status );

    public void slavesChanged( NodeManager manager, ClusterStatus status );

}
