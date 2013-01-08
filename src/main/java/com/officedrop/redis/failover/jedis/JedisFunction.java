package com.officedrop.redis.failover.jedis;

/**
 * User: Maurício Linhares
 * Date: 1/8/13
 * Time: 4:43 PM
 */
public interface JedisFunction {

    public void execute( JedisActions jedis ) throws Exception;

}