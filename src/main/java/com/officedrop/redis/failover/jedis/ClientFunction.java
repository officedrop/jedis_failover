package com.officedrop.redis.failover.jedis;

/**
 * User: Maurício Linhares
 * Date: 12/19/12
 * Time: 4:08 PM
 */
public interface ClientFunction <R> {

    public R apply( JedisClient client );

}
