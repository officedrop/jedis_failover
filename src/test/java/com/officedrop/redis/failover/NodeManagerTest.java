package com.officedrop.redis.failover;

import com.netflix.curator.test.TestingServer;
import com.officedrop.redis.failover.jedis.GenericJedisClientFactory;
import com.officedrop.redis.failover.redis.RedisServer;
import com.officedrop.redis.failover.strategy.LatencyFailoverSelectionStrategy;
import com.officedrop.redis.failover.strategy.SimpleMajorityStrategy;
import com.officedrop.redis.failover.utils.Function;
import com.officedrop.redis.failover.utils.SleepUtils;
import com.officedrop.redis.failover.utils.ThreadPool;
import com.officedrop.redis.failover.zookeeper.ZooKeeperNetworkClient;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * User: Maurício Linhares
 * Date: 1/7/13
 * Time: 10:49 AM
 */
public class NodeManagerTest {

    @Test
    public void testStartFromBlankConfiguration() throws Exception {

        TestingServer server = new TestingServer();

        RedisServer masterRedis = new RedisServer("localhost", 7000);
        masterRedis.start();

        RedisServer slaveRedis1 = new RedisServer("localhost", 7001);
        slaveRedis1.start();
        slaveRedis1.setMasterHost("localhost");
        slaveRedis1.setMasterPort("7000");

        RedisServer slaveRedis2 = new RedisServer("localhost", 7002);
        slaveRedis2.start();
        slaveRedis2.setMasterHost("localhost");
        slaveRedis2.setMasterPort("7000");

        final ZooKeeperNetworkClient zooKeeper = new ZooKeeperNetworkClient(server.getConnectString());

        List<HostConfiguration> hosts = Arrays.asList(
                new HostConfiguration("localhost", 7000),
                new HostConfiguration("localhost", 7001),
                new HostConfiguration("localhost", 7002) );

        Set<HostConfiguration> slaves = new HashSet<HostConfiguration>();
        slaves.add( hosts.get(1) );
        slaves.add( hosts.get(2) );

        final NodeManager manager = new NodeManager(
                zooKeeper,
                hosts,
                GenericJedisClientFactory.INSTANCE,
                ThreadPool.POOL,
                new LatencyFailoverSelectionStrategy(),
                new SimpleMajorityStrategy(),
                500,
                3);


        manager.start();

        SleepUtils.waitUntil(5000, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                ClusterStatus status = zooKeeper.getClusterData();

                return !status.isEmpty();
            }
        });

        ClusterStatus status = zooKeeper.getClusterData();

        Assert.assertEquals( hosts.get(0), status.getMaster() );
        Assert.assertEquals( slaves, status.getSlaves() );

        manager.stop();
        server.stop();

        masterRedis.stop();
        slaveRedis1.stop();
        slaveRedis2.stop();
    }

}
