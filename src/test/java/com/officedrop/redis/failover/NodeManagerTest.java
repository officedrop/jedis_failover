package com.officedrop.redis.failover;

import com.officedrop.redis.failover.redis.RedisServer;

/**
 * User: Maurício Linhares
 * Date: 1/7/13
 * Time: 10:49 AM
 */
public class NodeManagerTest {

    public void testStartFromBlankConfiguration() {

        RedisServer masterRedis = new RedisServer("localhost", 7000);
        RedisServer slaveRedis1 = new RedisServer("localhost", 7001);
        RedisServer slaveRedis2 = new RedisServer("localhost", 7002);



    }

}
