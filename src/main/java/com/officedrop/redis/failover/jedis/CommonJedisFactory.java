package com.officedrop.redis.failover.jedis;

import com.officedrop.redis.failover.HostConfiguration;

/**
 * User: Maurício Linhares
 * Date: 1/8/13
 * Time: 4:34 PM
 */
public class CommonJedisFactory implements JedisFactory {

    private final HostConfiguration configuration;

    public CommonJedisFactory( HostConfiguration configuration ) {
        this.configuration = configuration;
    }

    @Override
    public JedisActions create() {
        return new GenericJedisClient(
                this.configuration.getHost(),
                this.configuration.getPort(),
                this.configuration.getTimeout(),
                this.configuration.getDatabase()
                );
    }

}
