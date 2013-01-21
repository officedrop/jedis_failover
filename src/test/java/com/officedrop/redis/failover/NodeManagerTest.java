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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: Maurício Linhares
 * Date: 1/7/13
 * Time: 10:49 AM
 */
public class NodeManagerTest {

    private static final Logger log = LoggerFactory.getLogger(NodeManager.class);

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
        slaveRedis1.setMasterPort(masterRedis.getPort());

        slaveRedis2 = new RedisServer("localhost", COUNT.getAndIncrement());
        slaveRedis2.start();
        slaveRedis2.setMasterHost("localhost");
        slaveRedis2.setMasterPort(masterRedis.getPort());

        zooKeeper = new ZooKeeperNetworkClient(server.getConnectString());

        hosts = Arrays.asList(
                masterRedis.getHostConfiguration(),
                slaveRedis1.getHostConfiguration(),
                slaveRedis2.getHostConfiguration());
    }


    @After
    public void tearDown() {
        close(server, masterRedis, slaveRedis1, slaveRedis2);
    }

    @Test
    public void testStartFromBlankConfiguration() throws Exception {
        Assert.assertTrue(zooKeeper.getClusterData().isEmpty());

        Set<HostConfiguration> slaves = new HashSet<HostConfiguration>();
        slaves.add(hosts.get(1));
        slaves.add(hosts.get(2));

        final NodeManager manager = create();

        SleepUtils.waitUntil(5000, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                ClusterStatus status = zooKeeper.getClusterData();

                return !status.isEmpty();
            }
        });

        ClusterStatus status = zooKeeper.getClusterData();

        Assert.assertEquals(slaves, status.getSlaves());
        Assert.assertEquals(hosts.get(0), status.getMaster());


        manager.stop();
    }

    @Test
    public void testWithOneFailedClient() throws Exception {
        Assert.assertTrue(zooKeeper.getClusterData().isEmpty());

        Set<HostConfiguration> slaves = new HashSet<HostConfiguration>();
        slaves.add(hosts.get(1));

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

        Assert.assertEquals(hosts.get(0), status.getMaster());
        Assert.assertEquals(slaves, status.getSlaves());

        manager.stop();
    }

    @Test
    public void testWithTwoNodeManagersAtTheSameTime() {

        Assert.assertTrue(zooKeeper.getClusterData().isEmpty());

        Set<HostConfiguration> slaves = new HashSet<HostConfiguration>();
        slaves.add(hosts.get(1));

        ZooKeeperClient client = new ZooKeeperNetworkClient(server.getConnectString());

        List<NodeManager> managers = Arrays.asList(create(), create(client));

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

        Assert.assertEquals(hosts.get(0), status.getMaster());
        Assert.assertEquals(slaves, status.getSlaves());

        for (NodeManager manager : managers) {
            manager.stop();
        }

    }

    @Test
    public void testWithMasterFailing() {

        Assert.assertTrue(zooKeeper.getClusterData().isEmpty());

        Set<HostConfiguration> slaves = new HashSet<HostConfiguration>();
        slaves.add(hosts.get(1));

        List<NodeManager> managers = Arrays.asList(create());

        SleepUtils.waitUntil(5000, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                ClusterStatus status = zooKeeper.getClusterData();
                return !status.isEmpty();
            }
        });

        masterRedis.close();

        SleepUtils.waitUntil(10000, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                ClusterStatus status = zooKeeper.getClusterData();
                return !status.getUnavailables().isEmpty();
            }
        });

        ClusterStatus status = zooKeeper.getClusterData();

        Assert.assertTrue(status.hasMaster());
        Assert.assertEquals(1, slaves.size());
        Assert.assertEquals(masterRedis.getHostConfiguration(), status.getUnavailables().iterator().next());

        if (status.getMaster().equals(slaveRedis1.getHostConfiguration())) {
            Assert.assertEquals(slaveRedis1.getHostConfiguration(), slaveRedis2.getMasterConfiguration());
        } else {
            Assert.assertEquals(slaveRedis2.getHostConfiguration(), slaveRedis1.getMasterConfiguration());
        }

        for (NodeManager manager : managers) {
            manager.stop();
        }
    }

    @Test
    public void testWithDataAlreadyAvailable() {
        Assert.assertTrue(zooKeeper.getClusterData().isEmpty());

        ClusterStatus status = new ClusterStatus(
                masterRedis.getHostConfiguration(),
                Arrays.asList(slaveRedis1.getHostConfiguration(), slaveRedis2.getHostConfiguration()),
                Collections.EMPTY_LIST);

        zooKeeper.setClusterData(status);

        masterRedis.close();

        NodeManager manager = create();
        manager.start();

        SleepUtils.waitUntil(5000, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                ClusterStatus status = zooKeeper.getClusterData();
                return !status.getUnavailables().isEmpty();
            }
        });

        ClusterStatus currentStatus = zooKeeper.getClusterData();

        Assert.assertEquals(masterRedis.getHostConfiguration(), currentStatus.getUnavailables().iterator().next());
        Assert.assertEquals(1, currentStatus.getUnavailables().size());

        if (currentStatus.getMaster().equals(slaveRedis1.getHostConfiguration())) {
            Assert.assertEquals(slaveRedis2.getHostConfiguration(), currentStatus.getSlaves().iterator().next());
        } else {
            Assert.assertEquals(slaveRedis2.getHostConfiguration(), currentStatus.getMaster());
            Assert.assertEquals(slaveRedis1.getHostConfiguration(), currentStatus.getSlaves().iterator().next());
        }

        manager.stop();
    }

    @Test
    public void testSetMachineThatCameBackAsSlave() {
        Assert.assertTrue(zooKeeper.getClusterData().isEmpty());

        ClusterStatus status = new ClusterStatus(
                masterRedis.getHostConfiguration(),
                Arrays.asList(slaveRedis1.getHostConfiguration(), slaveRedis2.getHostConfiguration()),
                Collections.EMPTY_LIST);

        zooKeeper.setClusterData(status);

        masterRedis.close();

        NodeManager manager = create();
        manager.start();

        SleepUtils.waitUntil(5000, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                ClusterStatus status = zooKeeper.getClusterData();
                return !status.getUnavailables().isEmpty();
            }
        });

        masterRedis = new RedisServer(masterRedis.getHostConfiguration().getHost(), masterRedis.getPort());
        masterRedis.start();

        SleepUtils.waitUntil(10000, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                ClusterStatus status = zooKeeper.getClusterData();
                return status.getUnavailables().isEmpty();
            }
        });

        ClusterStatus currentStatus = zooKeeper.getClusterData();

        Assert.assertTrue(currentStatus.getSlaves().contains(masterRedis.getHostConfiguration()));
        Assert.assertEquals(currentStatus.getMaster(), masterRedis.getMasterConfiguration());

        manager.stop();
    }

    @Test
    public void testNodeManagerWithManualFailover() throws Exception {

        Assert.assertTrue(zooKeeper.getClusterData().isEmpty());

        ClusterStatus status = new ClusterStatus(
                masterRedis.getHostConfiguration(),
                Arrays.asList(slaveRedis1.getHostConfiguration(), slaveRedis2.getHostConfiguration()),
                Collections.EMPTY_LIST);

        zooKeeper.setClusterData(status);

        final NodeManager manager = create();
        manager.start();
        manager.waitUntilMasterIsAvailable(5000);

        zooKeeper.getCurator().create().forPath(
                ZooKeeperNetworkClient.MANUAL_FAILOVER_PATH,
                slaveRedis1.getHostConfiguration().asHost().getBytes("UTF-8") );

        Assert.assertEquals(slaveRedis1.getHostConfiguration(), zooKeeper.getManualFailoverConfiguration());

        log.info("Manual failover was set");

        SleepUtils.waitUntil(10000, new Function<Boolean>() {
            @Override
            public Boolean apply() {
                return manager.getLastClusterStatus().getMaster().equals( slaveRedis1.getHostConfiguration() );
            }
        });

        Assert.assertNull(zooKeeper.getCurator().checkExists().forPath(ZooKeeperNetworkClient.MANUAL_FAILOVER_PATH));

        manager.stop();
    }

    private void close(Closeable... closeables) {
        for (Closeable c : closeables) {
            IOUtils.closeQuietly(c);
        }
    }

    private NodeManager create(ZooKeeperClient client) {
        NodeManager manager = new NodeManager(
                client,
                hosts,
                GenericJedisClientFactory.INSTANCE,
                ThreadPool.POOL,
                new LatencyFailoverSelectionStrategy(),
                new SimpleMajorityStrategy(),
                1000,
                3,
                true);

        manager.start();

        return manager;
    }

    private NodeManager create() {
        return create(zooKeeper);
    }

}
