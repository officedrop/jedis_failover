package com.officedrop.redis.failover.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

/**
 * User: Maurício Linhares
 * Date: 12/19/12
 * Time: 2:01 PM
 */
public class GenericJedisClient extends Jedis implements JedisClient {

    public GenericJedisClient(String host, int port, int timeout, int database) {
        super(host, port, timeout);
        if ( database != Protocol.DEFAULT_DATABASE) {
            this.select(database);
        }
    }

}