package com.officedrop.redis.failover;

/**
 * User: Maurício Linhares
 * Date: 12/24/12
 * Time: 6:29 PM
 */
public interface NodeListener {

    public void nodeIsOnline( Node node, long latency );
    public void nodeIsOffline( Node node, Exception e );

}
