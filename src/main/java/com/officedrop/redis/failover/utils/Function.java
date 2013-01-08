package com.officedrop.redis.failover.utils;

/**
 * User: Maurício Linhares
 * Date: 1/7/13
 * Time: 11:01 AM
 */
public interface Function <OUT> {

    public OUT apply();

}
