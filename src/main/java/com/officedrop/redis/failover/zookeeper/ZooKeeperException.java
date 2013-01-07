package com.officedrop.redis.failover.zookeeper;

/**
 * User: Maurício Linhares
 * Date: 1/4/13
 * Time: 3:56 PM
 */
public class ZooKeeperException extends IllegalStateException {

    public ZooKeeperException( Throwable t ) {
        super(t);
    }

}
