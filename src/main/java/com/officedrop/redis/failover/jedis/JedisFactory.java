package com.officedrop.redis.failover.jedis;

/**
 * User: Maurício Linhares
 * Date: 1/8/13
 * Time: 4:24 PM
 */
public interface JedisFactory {

    public JedisActions create();

}
