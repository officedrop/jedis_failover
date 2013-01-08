package com.officedrop.redis.failover.jedis;

import com.officedrop.redis.failover.Client;
import com.officedrop.redis.failover.NodeManager;

/**
 * User: Maurício Linhares
 * Date: 1/8/13
 * Time: 4:24 PM
 */
public class FailoverJedisFactory implements JedisFactory {

    private final NodeManager nodeManager;

    public FailoverJedisFactory( NodeManager manager ) {
        this.nodeManager = manager;
    }

    @Override
    public JedisActions create() {
        return new Client( this.nodeManager, GenericJedisClientFactory.INSTANCE );
    }
}
