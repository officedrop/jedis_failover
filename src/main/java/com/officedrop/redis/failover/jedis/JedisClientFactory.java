package com.officedrop.redis.failover.jedis;

import com.officedrop.redis.failover.HostConfiguration;

/**
 * User: Maurício Linhares
 * Date: 12/19/12
 * Time: 2:17 PM
 */
public interface JedisClientFactory {

    public JedisClient create( HostConfiguration configuration  );

}
