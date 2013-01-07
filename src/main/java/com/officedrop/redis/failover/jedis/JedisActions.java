package com.officedrop.redis.failover.jedis;

import redis.clients.jedis.JedisCommands;

/**
 * User: Maurício Linhares
 * Date: 12/20/12
 * Time: 4:26 PM
 */
public interface JedisActions extends JedisCommands {

    public Long del(final String... keys);

    public String quit();

    public String ping();

    public String slaveof(final String host, final int port);

    public String slaveofNoOne();

    public String info();

}