package com.officedrop.redis.failover.jedis;

import com.officedrop.redis.failover.HostConfiguration;

/**
 * User: Maurício Linhares
 * Date: 1/3/13
 * Time: 9:24 AM
 */
public class GenericJedisClientFactory implements JedisClientFactory {

    public static final GenericJedisClientFactory INSTANCE = new GenericJedisClientFactory();

    @Override
    public JedisClient create(final HostConfiguration configuration) {
        return new GenericJedisClient(
                configuration.getHost(),
                configuration.getPort(),
                configuration.getTimeout(),
                configuration.getDatabase()
                );
    }

}
