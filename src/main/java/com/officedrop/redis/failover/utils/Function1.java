package com.officedrop.redis.failover.utils;

/**
 * User: Maurício Linhares
 * Date: 12/26/12
 * Time: 3:47 PM
 */
public interface Function1 <IN,OUT> {

    public OUT apply( IN parameter );

}
