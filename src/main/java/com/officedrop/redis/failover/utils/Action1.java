package com.officedrop.redis.failover.utils;

/**
 * User: Maurício Linhares
 * Date: 12/24/12
 * Time: 6:36 PM
 */
public interface Action1<T> {

    public void apply(T parameter);

}
