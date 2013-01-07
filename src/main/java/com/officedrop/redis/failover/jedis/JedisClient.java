package com.officedrop.redis.failover.jedis;

import redis.clients.jedis.BinaryJedisCommands;

/**
 * User: Maurício Linhares
 * Date: 12/19/12
 * Time: 1:57 PM
 */
public interface JedisClient extends BinaryJedisCommands, JedisActions {

}