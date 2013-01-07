package com.officedrop.redis.failover.utils;

import com.officedrop.redis.failover.redis.RedisServer;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * User: Maurício Linhares
 * Date: 1/2/13
 * Time: 9:42 PM
 */
public class JedisClientTest {

    @Test
    public void testClient() throws Exception {

        RedisServer server = new RedisServer("localhost", 8000);
        server.start();

        Jedis jedis = new Jedis("localhost", 8000);
        jedis.ping();
        jedis.disconnect();

    }

}
