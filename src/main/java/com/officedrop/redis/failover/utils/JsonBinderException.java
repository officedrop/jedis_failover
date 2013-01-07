package com.officedrop.redis.failover.utils;

/**
 * User: Maurício Linhares
 * Date: 1/4/13
 * Time: 2:14 PM
 */
public class JsonBinderException extends IllegalArgumentException {

    public JsonBinderException( Throwable t ) {
        super( t );
    }

}
