package com.officedrop.redis.failover.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

/**
 * User: Maurício Linhares
 * Date: 12/19/12
 * Time: 2:01 PM
 */
public class GenericJedisClient extends Jedis implements JedisClient {


    public GenericJedisClient(JedisShardInfo shardInfo) {
        super(shardInfo);
    }

    public GenericJedisClient(String host, int port) {
        super(host, port);
    }

    public GenericJedisClient(String host, int port, int timeout) {
        super(host, port, timeout);
    }

    public GenericJedisClient(String host) {
        super(host);
    }

}