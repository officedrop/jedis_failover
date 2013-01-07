package com.officedrop.redis.failover.jedis;

/**
 * User: Maurício Linhares
 * Date: 12/26/12
 * Time: 4:01 PM
 */
public class ConnectionException extends RuntimeException {

    public ConnectionException(final Throwable cause) {
        super(cause);
    }

}