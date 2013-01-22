package com.officedrop.redis.failover.jedis;

import com.netflix.curator.test.TestingServer;
import com.officedrop.redis.failover.HostConfiguration;
import com.officedrop.redis.failover.redis.RedisServer;
import com.officedrop.redis.failover.utils.Function;
import com.officedrop.redis.failover.utils.SleepUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: mauricio
 * Date: 1/15/13
 * Time: 6:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class JedisPoolBuilderTest {

    private static final Logger log = LoggerFactory.getLogger(JedisPoolBuilderTest.class);

    JedisPool pool = new JedisPoolBuilder()
            .withFailoverConfiguration(
                    "localhost:2838",
                    Arrays.asList(new HostConfiguration("localhost", 7000), new HostConfiguration("localhost", 7001)))
            .build();

    @Test
    public void testRealClientFailsOverToNewMaster() throws Exception {

        TestingServer zooKeeper = new TestingServer();

        RedisServer server1 = new RedisServer("localhost", 8000);
        server1.start();

        RedisServer server2 = new RedisServer("localhost", 8001);
        server2.setMasterHost(server1.getHostConfiguration().getHost());
        server2.setMasterPort(server1.getPort());
        server2.start();

        final JedisPool pool = new JedisPoolBuilder()
                .withFailoverConfiguration(
                        zooKeeper.getConnectString(),
                        Arrays.asList( server1.getHostConfiguration(), server2.getHostConfiguration()))
                .build();

        final JedisFunction function = new JedisFunction() {
            @Override
            public void execute(JedisActions jedis) throws Exception {
                jedis.ping();
            }
        };

        pool.withJedis(function);

        server1.stop();

        pool.withJedis(new JedisFunction() {
            @Override
            public void execute(final JedisActions jedis) throws Exception {
                SleepUtils.waitUntil(10000, new Function<Boolean>() {
                    @Override
                    public Boolean apply() {
                        try {
                            jedis.ping();
                            return true;
                        } catch (Exception e) {
                            log.error("Failed to ping server", e);
                            SleepUtils.safeSleep(4, TimeUnit.SECONDS);
                            return false;
                        }
                    }
                });
            }
        });

        pool.withJedis(function);

        pool.close();

        zooKeeper.stop();
        server2.stop();

    }

    @Test
    public void testRealClientFailsOverFromSlaveToMaster() throws Exception {

        TestingServer zooKeeper = new TestingServer();

        RedisServer server1 = new RedisServer("localhost", 8000);
        server1.start();

        RedisServer server2 = new RedisServer("localhost", 8001);
        server2.setMasterHost(server1.getHostConfiguration().getHost());
        server2.setMasterPort(server1.getPort());
        server2.start();

        final JedisPool pool = new JedisPoolBuilder()
                .withFailoverConfiguration(
                        zooKeeper.getConnectString(),
                        Arrays.asList( server1.getHostConfiguration(), server2.getHostConfiguration()))
                .build();

        final JedisFunction function = new JedisFunction() {
            @Override
            public void execute(JedisActions jedis) throws Exception {
                jedis.get("some-key");
            }
        };

        pool.withJedis(function);

        server2.stop();

        pool.withJedis(new JedisFunction() {
            @Override
            public void execute(final JedisActions jedis) throws Exception {
                SleepUtils.waitUntil(10000, new Function<Boolean>() {
                    @Override
                    public Boolean apply() {
                        try {
                            jedis.get("some-key");
                            return true;
                        } catch (Exception e) {
                            log.error("Failed to ping server", e);
                            SleepUtils.safeSleep(4, TimeUnit.SECONDS);
                            return false;
                        }
                    }
                });
            }
        });

        pool.withJedis(function);

        pool.close();

        zooKeeper.stop();
        server2.stop();

    }

}
