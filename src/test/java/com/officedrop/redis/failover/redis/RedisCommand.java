package com.officedrop.redis.failover.redis;

/**
 * User: Maurício Linhares
 * Date: 1/2/13
 * Time: 10:33 PM
 * */

 public enum RedisCommand {

    PING,
    INFO,
    SLAVEOF,
    GET,
    SET,
    QUIT,
    SELECT,

}