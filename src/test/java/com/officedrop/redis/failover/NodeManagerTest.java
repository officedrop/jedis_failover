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
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: Maurício Linhares
 * Date: 1/7/13
 * Time: 10:49 AM
 */
public class NodeManagerTest {

    private static final AtomicInteger COUNT = new AtomicInteger(10000);

    TestingServer server;
    RedisServer masterRedis;
    RedisServer slaveRedis1;
    RedisServer slaveRedis2;
    ZooKeeperNetworkClient zooKeeper;
    List<HostConfiguration> hosts;

    @Before
    public void setup() throws Exception {
        server = new TestingServer();

        masterRedis = new RedisServer("localhost", COUNT.getAndIncrement());
        masterRedis.start();

        slaveRedis1 = new RedisServer("localhost", COUNT.getAndIncrement());
        slaveRedis1.start();
        slaveRedis1.setMasterHost("localhost");
        slaveRedis1.setMasterPort( masterRedis.getPort() );

        slaveRedis2 = new RedisServer("localhost", COUNT.getAndIncrement());
        slaveRedis2.start();
        slaveRedis2.setMasterHost("localhost");
        slaveRedis2.setMasterPort(masterRedis.getPort());

        zooKeeper = new ZooKeeperNetworkClient(server.getConnectString());

        hosts = Arrays.asList(
                masterRedis.getHostConfiguration(),
                slaveRedis1.getHostConfiguration(),
                slaveRedis2.getHostConfiguration() );
    }


    @After
    public void tearDown() {
        close(server, masterRedis, slaveRedis1, slaveRedis2);
    }

    @Test
    public void testStartFromBlankConfiguration() throws Exception {
        Assert.assertTrue( zooKeeper.getClusterData().isEmpty() );

        Set<HostConfiguration> slaves = new HashSet<HostConfiguration>();
        slaves.add( hosts.get(1) );
        slaves.add( hosts.get(2) );

        final NodeManager manager = create();

        SleepUtils.waitUntil(5000, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                ClusterStatus status = zooKeeper.getClusterData();

                return !status.isEmpty();
            }
        });

        ClusterStatus status = zooKeeper.getClusterData();

        Assert.assertEquals( slaves, status.getSlaves() );
        Assert.assertEquals( hosts.get(0), status.getMaster() );

        manager.stop();
    }

    @Test
    public void testWithOneFailedClient() throws Exception {
        Assert.assertTrue( zooKeeper.getClusterData().isEmpty() );

        Set<HostConfiguration> slaves = new HashSet<HostConfiguration>();
        slaves.add( hosts.get(1) );

        final NodeManager manager = create();

        SleepUtils.waitUntil(5000, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                ClusterStatus status = zooKeeper.getClusterData();
                return !status.isEmpty();
            }
        });

        slaveRedis2.stop();

        SleepUtils.waitUntil(10000, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                ClusterStatus status = zooKeeper.getClusterData();
                return !status.getUnavailables().isEmpty();
            }
        });

        ClusterStatus status = zooKeeper.getClusterData();

        Assert.assertEquals( hosts.get(0), status.getMaster() );
        Assert.assertEquals( slaves, status.getSlaves() );

        manager.stop();
    }

    @Test
    public void testWithTwoNodeManagersAtTheSameTime() {

        Assert.assertTrue( zooKeeper.getClusterData().isEmpty() );

        Set<HostConfiguration> slaves = new HashSet<HostConfiguration>();
        slaves.add( hosts.get(1) );

        List<NodeManager> managers = Arrays.asList( create(), create());

        SleepUtils.waitUntil(5000, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                ClusterStatus status = zooKeeper.getClusterData();
                return !status.isEmpty();
            }
        });

        slaveRedis2.stop();

        SleepUtils.waitUntil(5000, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                ClusterStatus status = zooKeeper.getClusterData();
                return !status.getUnavailables().isEmpty();
            }
        });

        ClusterStatus status = zooKeeper.getClusterData();

        Assert.assertEquals( hosts.get(0), status.getMaster() );
        Assert.assertEquals( slaves, status.getSlaves() );

        for ( NodeManager manager : managers ) {
            manager.stop();
        }

    }

    private void close( Closeable ... closeables ) {
        for ( Closeable c : closeables ) {
            IOUtils.closeQuietly(c);
        }
    }

    private NodeManager create() {
        NodeManager manager =  new NodeManager(
                zooKeeper,
                hosts,
                GenericJedisClientFactory.INSTANCE,
                ThreadPool.POOL,
                new LatencyFailoverSelectionStrategy(),
                new SimpleMajorityStrategy(),
                1000,
                3);

        manager.start();

        return manager;
    }

}
